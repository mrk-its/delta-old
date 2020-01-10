/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.storage

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.util.FileNames
import com.google.common.cache.CacheBuilder
import com.google.common.io.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.http.client.utils.URLEncodedUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import java.sql.{DriverManager, Connection, ResultSet, SQLException}


/*
  Following spark properties needs to be configured:
   - spark.delta.PostgreSQLLogStore.db_url - db url in form:
     postgresql://host:port/db_name?user=...&password=...
   - spark.delta.PostgreSQLLogStore.db_table - name of delta_log db table (defaults to 'delta_log')

  for now manual creation of log table is required:
  create table delta_log(path text primary key collate "C", length bigint, timestamp_ms bigint);
*/

abstract class BaseExternalLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  protected def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  protected def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  protected def stripUserInfo(path: Path): Path = {
    val uri = path.toUri
    val newUri = new URI(
      uri.getScheme,
      null,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
    new Path(newUri)
  }

  /**
   * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
   * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
   * `iterWithPrecedence` and discard that from `iter`.
   */
  protected def mergeFileIterators(
      iter: Iterator[FileStatus],
      iterWithPrecedence: Iterator[FileStatus]): Iterator[FileStatus] = {
    val result = (
      iter.map(f => (f.getPath, f)).toMap
      ++ iterWithPrecedence.map(f => (f.getPath, f)))
      .values
      .toSeq
      .sortBy(_.getPath.getName)
    // logDebug(s"mergeFileIterators: ${result.toList}")
    result.iterator
  }

  protected def writeActions(fs: FileSystem, path: Path, actions: Iterator[String]): Long = {
    logDebug(color_text(s"writeActions to: $path", BLUE))
    val stream = fs.create(path, true)
    val countingStream = new CountingOutputStream(stream)
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(countingStream.write)
    stream.close()
    countingStream.getCount()
  }

  protected def delete_file(fs: FileSystem, path: Path) = {
    logDebug(color_text(s"delete file: $path", BLUE))
    fs.delete(path, false)
  }


  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */

  override def listFrom(path: Path): Iterator[FileStatus] = {
    logDebug(color_text(s"listFrom path: ${path}", MAGENTA))
    val (fs, resolvedPath) = resolved(path)
    listFrom(fs, resolvedPath)
  }

  /**
   * List fitles starting from `resolvedPath` (inclusive) in the same directory, which merges
   * the file system list and the db list
   */

  def listFrom(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }

    val listedFromFs =
      fs.listStatus(parentPath).filter( path => (path.getPath.getName >= resolvedPath.getName) )
    listedFromFs.iterator.map(entry => s"fs item: ${entry.getPath}").foreach(x => logDebug(x))

    val listedFromDB = listFromInternal(fs, resolvedPath).toList
    listedFromDB.iterator.map(entry => s"db item: ${entry.getPath}").foreach(x => logDebug(x))

    mergeFileIterators(listedFromFs.iterator, listedFromDB.iterator)
  }

  def listFromInternal(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    fixTransactionLog(fs, parentPath, resolvedPath)
    try {
      return listLogEntriesFrom(fs, parentPath, resolvedPath)
        .filter(_.isComplete)
        .map(item => item.asFileStatus(fs))
    } catch {
      case e: Throwable => throw new java.nio.file.FileSystemException(e.toString())
    }
  }

  private def fixTransactionLog(
      fs: FileSystem,
      parentPath: Path,
      resolvedPath: Path
  ) = {
    listLogEntriesFrom(fs, parentPath, resolvedPath)
    .filter(!_.isComplete)
    .foreach(item => {
        assert(item.tempPath.isDefined, "tempPath must be present for incomplete entries")
        logDebug(green_text(s"fixing $item"))
        // TODO simply copy streams instead of copying line by line
        // and put it outside transaction
        val tempPath = item.tempPath.get
        val length = writeActions(fs, item.path, read(tempPath).iterator)
        putLogEntry(LogEntry(item.path, None, length, System.currentTimeMillis(), true), true)
        logDebug(color_text(s"delete ${item.tempPath}", BLUE))
        delete_file(fs, tempPath)
    })
  }

  protected def putLogEntry(
    logEntry: LogEntry, overwrite: Boolean): Unit

  protected def listLogEntriesFrom(
    fs: FileSystem,
    parentPath: Path,
    fromPath: Path
  ): Iterator[LogEntry]

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)
    val parentPath = resolvedPath.getParent
    val lockedPath = getPathKey(resolvedPath)
    var stream: FSDataOutputStream = null
    logDebug(color_text(s"write path: ${path}, ${overwrite}", MAGENTA))

    if (overwrite) {
        // TODO - store in db too
        writeActions(fs, resolvedPath, actions)
        return
    }

    val uuid = java.util.UUID.randomUUID().toString
    val temp_path = new Path(s"$parentPath/temp/$uuid")

    val actions_seq = actions.toSeq

    val length = writeActions(fs, temp_path, actions_seq.iterator)

    try {
      putLogEntry(
        LogEntry(resolvedPath, Some(temp_path), length, System.currentTimeMillis(), false), false
      )
    } catch {
        case e: Throwable => { // scalastyle:ignore
            logError(red_text(s"${e.getClass.getName}: $e"));
            delete_file(fs, temp_path)
            throw e
        }
    }

    injectFail("1")
    try {
        injectFail("2")
        val length = writeActions(fs, resolvedPath, actions_seq.iterator)
        putLogEntry(LogEntry(resolvedPath, None, length, System.currentTimeMillis(), true), true)
        delete_file(fs, temp_path)
    } catch {
        case e: Throwable => logWarning(e.toString())
    }
  }

  /**
   * Check if the path is an initial version of a Delta log.
   */
  protected def isInitialVersion(path: Path): Boolean = {
    FileNames.isDeltaFile(path) && FileNames.deltaVersion(path) == 0L
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
  }

  // debug utils

  val failConfKey: String = "spark.delta.LogStore.fails"
  val failPropability = sparkConf.get(failConfKey, ",").split(',').map(
    part => {
      val parts = part.split('='); (parts(0) -> parts(1).toFloat)
    }
  ).toMap

  protected def injectFail(name: String) = {
    assert(
      scala.util.Random.nextFloat >= failPropability.getOrElse(name, 0.0f),
      red_text(s"FAIL $name INJECTED")
    )
  }

  val RED = "31"
  val GREEN = "32"
  val YELLOW = "33"
  val BLUE = "34"
  val MAGENTA = "35"
  val CYAN = "36"

  protected def color_text(text: String, color: String) = s"\u001b[0;${color}m${text}\u001b[m "
  protected def red_text(text: String) = color_text(text, RED)
  protected def green_text(text: String) = color_text(text, GREEN)
  logWarning(color_text(s"will inject fails with propabilities: $failPropability", RED))
}

/**
 * The file metadata to be stored in the external store
 */

case class LogEntry(
  path: Path,
  tempPath: Option[Path],
  length: Long,
  modificationTime: Long,
  isComplete: Boolean
) {
    def asFileStatus(fs: FileSystem): FileStatus = {
        new FileStatus(
            length,
            false,
            1,
            fs.getDefaultBlockSize(path),
            modificationTime,
            path
        )
    }
}


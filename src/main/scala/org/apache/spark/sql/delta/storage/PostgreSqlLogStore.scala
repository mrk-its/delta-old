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


class PostgreSqlLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  val UNIQUE_VIOLATION = "23505"

  val dbUrlConfKey: String = "spark.delta.PostgreSqlLogStore.db_url"
  val dbTableConfKey: String = "spark.delta.PostgreSqlLogStore.db_table"

  val db_table: String = sparkConf.get(dbTableConfKey, "delta_log")
  val db_url: String = sparkConf.get(dbUrlConfKey)

  def getConnection(): Connection = {
    val params = URLEncodedUtils.parse(new java.net.URI(db_url), UTF_8)
    val scalaParams: Seq[(String, String)] = collection.immutable.Seq(params.asScala: _*).map(
      pair => pair.getName -> pair.getValue
    )
    val paramsMap: Map[String, String] = scalaParams.toMap
    DriverManager.getConnection(s"jdbc:$db_url", paramsMap("user"), paramsMap("password"))
  }

  private def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  private def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  private def stripUserInfo(path: Path): Path = {
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
  private def mergeFileIterators(
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

  private def listFromDB(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      logDebug(s"No such file or directory: $parentPath")
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }
    val sql_connection = getConnection()
    try {
      val stmt = sql_connection.prepareStatement(s"""
        SELECT path, length, timestamp_ms
        FROM $db_table
        WHERE path LIKE (? || '/%') AND path >= ?
      """)
      stmt.setString(1, parentPath.toString())
      stmt.setString(2, resolvedPath.toString())
      logDebug(stmt.toString())
      return new RsIterator(stmt.executeQuery()).map(row => {
        logDebug(s"item: ${row.getString("path")}")
        new FileStatus(
          row.getLong("length"),
          false,
          1,
          fs.getDefaultBlockSize(new Path(row.getString("path"))),
          row.getLong("timestamp_ms"),
          new Path(row.getString("path"))
        )
      })
    } catch {
      case e: Throwable => throw new java.nio.file.FileSystemException(e.toString())
    } finally {
      sql_connection.close()
    }
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
   * the file system list and the db list
   */
  private def listFromInternal(fs: FileSystem, resolvedPath: Path) = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }

    val listedFromFs =
      fs.listStatus(parentPath).filter( path =>
        (path.getPath.getName >= resolvedPath.getName) && path.getPath.getName.endsWith(".parquet")
      )
    // logDebug(s"listedfromFs: ${listedFromFs.toList}")
    val listedFromDB = listFromDB(fs, resolvedPath)

    mergeFileIterators(listedFromFs.iterator, listedFromDB)
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */
  override def listFrom(path: Path): Iterator[FileStatus] = {
    logDebug(s"listFrom path: ${path}")
    val (fs, resolvedPath) = resolved(path)
    listFromInternal(fs, resolvedPath)
  }

  /**
   * Check if the path is an initial version of a Delta log.
   */
  private def isInitialVersion(path: Path): Boolean = {
    FileNames.isDeltaFile(path) && FileNames.deltaVersion(path) == 0L
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)
    val lockedPath = getPathKey(resolvedPath)
    var stream: FSDataOutputStream = null
    logDebug(s"write path: ${path}, ${overwrite}")
    val sql_connection = getConnection()
    try {
      sql_connection.setAutoCommit(false)
      val insert_stmt = sql_connection.prepareStatement(
        if (!overwrite) {
          s"INSERT INTO $db_table(path) VALUES(?)"
        } else {
          s"INSERT INTO $db_table(path) VALUES(?) ON CONFLICT(path) DO NOTHING"
      }
      )
      insert_stmt.setString(1, resolvedPath.toString())
      logDebug(insert_stmt.toString())
      insert_stmt.executeUpdate()

      stream = fs.create(resolvedPath, true)
      val countingStream = new CountingOutputStream(stream)
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(countingStream.write)
      stream.close()

      // When a Delta log starts afresh, all cached files in that Delta log become obsolete,
      // so we remove them from the cache.
      if (isInitialVersion(resolvedPath)) {
      }

      val update_stmt = sql_connection.prepareStatement(s"""
        UPDATE $db_table
        SET length=?, timestamp_ms=?
        WHERE path=?
      """)
      update_stmt.setLong(1, countingStream.getCount())
      update_stmt.setLong(2, System.currentTimeMillis())
      update_stmt.setString(3, resolvedPath.toString())
      logDebug(update_stmt.toString())
      update_stmt.executeUpdate()
      sql_connection.commit()
    } catch {
      case e: Throwable => { // scalastyle:ignore
        logError(s"${e.getClass.getName}: $e");
        if (
          e.isInstanceOf[SQLException]
          && e.asInstanceOf[SQLException].getSQLState() == UNIQUE_VIOLATION
        ) {
          throw new java.nio.file.FileAlreadyExistsException(resolvedPath.toString())
        }
        if (stream != null) {
          fs.delete(resolvedPath, false)
          logDebug(s"removed invalid file: $resolvedPath")
        }
        throw new java.nio.file.FileSystemException(resolvedPath.toString())
      }
    } finally {
      sql_connection.close()
    }
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
  }
}

object PostgreSqlLogStore {
}

class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
  def hasNext: Boolean = rs.next()
  def next(): ResultSet = rs
}

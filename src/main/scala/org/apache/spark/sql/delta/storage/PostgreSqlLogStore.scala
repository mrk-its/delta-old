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
   - spark.delta.PostgreSqlLogStore.db_url - db url in form:
     postgresql://host:port/db_name?user=...&password=...
   - spark.delta.PostgreSqlLogStore.db_table - name of delta_log db table (defaults to 'delta_log')

  for now manual creation of log table is required:
  create table delta_log(path text primary key collate "C", length bigint, timestamp_ms bigint);
*/

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

  private def toPath(path: String) = {
    if (path != null) {
        new Path(path)
    } else {
        null
    }
  }

  private def getDBLog(
      sql_connection: Connection,
      parentPath: Path,
      resolvedPath: Path,
      isComplete: Boolean
  ): Iterator[DBFileStatus] = {
    val stmt = sql_connection.prepareStatement(s"""
    SELECT path, temp_path, length, timestamp_ms, is_complete
    FROM $db_table
    WHERE path LIKE (? || '/%') AND path >= ? AND is_complete = ?
    """)
    stmt.setString(1, parentPath.toString())
    stmt.setString(2, resolvedPath.toString())
    stmt.setBoolean(3, isComplete)
    logDebug(stmt.toString())
    return new RsIterator(stmt.executeQuery()).map(row => {
        DBFileStatus(
            toPath(row.getString("path")),
            toPath(row.getString("temp_path")),
            row.getLong("length"),
            row.getLong("timestamp_ms"),
            row.getBoolean("is_complete")
        )
    })
  }

  private def fixTransactionLog(
      sql_connection: Connection,
      fs: FileSystem,
      parentPath: Path,
      resolvedPath: Path
  ) = {
    sql_connection.setAutoCommit(false)
    val update_stmt = sql_connection.prepareStatement(s"""
        UPDATE $db_table
        SET length=?, timestamp_ms=?, is_complete=true, temp_path=null
        WHERE path=?
    """)
    getDBLog(sql_connection, parentPath, resolvedPath, false)
    .foreach(item => {
        logDebug(green_text(s"fixing $item"))
        // TODO simply copy streams instead copying line by line
        // and put it outside transaction
        val length = writeActions(fs, item.path, read(item.tempPath).iterator)
        update_stmt.setLong(1, length)
        update_stmt.setLong(2, System.currentTimeMillis())
        update_stmt.setString(3, item.path.toString())
        logDebug(update_stmt.toString())
        update_stmt.executeUpdate()
        sql_connection.commit()
        logDebug(s"delete ${item.tempPath}")
        fs.delete(item.tempPath)
    })
  }

  private def listFromDB(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      logDebug(s"No such file or directory: $parentPath")
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }
    val sql_connection = getConnection()
    fixTransactionLog(sql_connection, fs, parentPath, resolvedPath)
    try {
      return getDBLog(sql_connection, parentPath, resolvedPath, true)
        .map(item => item.asFileStatus(fs))
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
      fs.listStatus(parentPath).filter( path => (path.getPath.getName >= resolvedPath.getName) )
    listedFromFs.iterator.map(entry => s"fs item: ${entry.getPath}").foreach(x => logDebug(x))

    val listedFromDB = listFromDB(fs, resolvedPath).toList
    listedFromDB.iterator.map(entry => s"db item: ${entry.getPath}").foreach(x => logDebug(x))

    mergeFileIterators(listedFromFs.iterator, listedFromDB.iterator)
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

  private def writeActions(fs: FileSystem, path: Path, actions: Iterator[String]): Long = {
    val stream = fs.create(path, true)
    val countingStream = new CountingOutputStream(stream)
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(countingStream.write)
    stream.close()
    countingStream.getCount()
  }

  val RED = "31"
  val GREEN = "32"
  private def color_text(text: String, color: String) = s"\u001b[0;${color}m${text}\u001b[m "
  private def red_text(text: String) = color_text(text, RED)
  private def green_text(text: String) = color_text(text, GREEN)

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)
    val parentPath = resolvedPath.getParent
    val lockedPath = getPathKey(resolvedPath)
    var stream: FSDataOutputStream = null
    logDebug(s"write path: ${path}, ${overwrite}")
    val sql_connection = getConnection()

    if (overwrite) {
        // TODO - store in db too
        writeActions(fs, resolvedPath, actions)
        return
    }

    val uuid = java.util.UUID.randomUUID().toString
    val temp_path = new Path(s"$parentPath/temp/$uuid")

    val actions_list = actions.toList

    writeActions(fs, temp_path, actions_list.iterator)

    try {
      sql_connection.setAutoCommit(false)
      val insert_stmt = sql_connection.prepareStatement(
          s"INSERT INTO $db_table(path, temp_path, is_complete) VALUES(?, ?, false)"
      )
      insert_stmt.setString(1, resolvedPath.toString())
      insert_stmt.setString(2, temp_path.toString())
      logDebug(insert_stmt.toString())
      insert_stmt.executeUpdate()
      sql_connection.commit()
    } catch {
        case e: Throwable => { // scalastyle:ignore
            logError(s"${e.getClass.getName}: $e");
            if (
                e.isInstanceOf[SQLException]
                && e.asInstanceOf[SQLException].getSQLState() == UNIQUE_VIOLATION
            ) {
                sql_connection.rollback()
                fs.delete(temp_path, false)
                throw new java.nio.file.FileAlreadyExistsException(resolvedPath.toString())
            }
            sql_connection.rollback()
            fs.delete(temp_path, false)
            throw new java.nio.file.FileSystemException(resolvedPath.toString())
        }
    }
    // assert(scala.util.Random.nextFloat > 0.25, "\u001b[0;31mLLURE 1 INJECTED\u001b[m ")

    try {
        // >assert(scala.util.Random.nextFloat > 0.5, "\u001b[0;31mFAILURE 2 INJECTED\u001b[m ")

        val length = writeActions(fs, resolvedPath, actions_list.iterator)
        val update_stmt = sql_connection.prepareStatement(s"""
            UPDATE $db_table
            SET length=?, timestamp_ms=?, is_complete=true, temp_path=null
            WHERE path=?
        """)
        update_stmt.setLong(1, length)
        update_stmt.setLong(2, System.currentTimeMillis())
        update_stmt.setString(3, resolvedPath.toString())
        logDebug(update_stmt.toString())
        update_stmt.executeUpdate()
        sql_connection.commit()
        fs.delete(temp_path, false)
    } catch {
        case e: Throwable => logWarning(e.toString())
    } finally {
        sql_connection.close()
    }
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
  }
}

class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next()
    def next(): ResultSet = rs
  }

/**
 * The file metadata to be stored in the cache.
 */
case class DBFileStatus(
  path: Path,
  tempPath: Path,
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

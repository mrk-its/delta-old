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
  create table delta_log(path text primary key collate "C", length bigint, mtime bigint);
*/

class PostgreSqlLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf)
{

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
    val connection =
      DriverManager.getConnection(s"jdbc:$db_url", paramsMap("user"), paramsMap("password"))
    connection.setAutoCommit(false)
    connection
  }

  def listLogEntriesFrom(
      fs: FileSystem,
      parentPath: Path,
      fromPath: Path
  ): Iterator[LogEntry] = {
    val sql_connection = getConnection()
    val stmt = sql_connection.prepareStatement(s"""
    SELECT path, temp_path, length, mtime, is_complete
    FROM $db_table
    WHERE path LIKE (? || '/%') AND path >= ?
    """)
    stmt.setString(1, parentPath.toString())
    stmt.setString(2, fromPath.toString())
    logDebug(stmt.toString())
    return new RsIterator(stmt.executeQuery()).map(row => {
        val path = row.getString("path")
        logDebug(s"db item: $path")
        LogEntry(
            new Path(path),
            Option(row.getString("temp_path")).map(new Path(_)),
            row.getLong("length"),
            row.getLong("mtime"),
            row.getBoolean("is_complete")
        )
    })
  }

  protected def putLogEntry(
    logEntry: LogEntry, overwrite: Boolean
  ): Unit = {
    val sql_connection = getConnection()
    val java_temp_path = if (logEntry.tempPath.isDefined) logEntry.tempPath.get.toString() else null
    val mtime = System.currentTimeMillis()
    try {
      if (!overwrite) {
        val insert_stmt = sql_connection.prepareStatement(
          s"""
          INSERT INTO $db_table(path, temp_path, length, is_complete)
          VALUES(?, ?, ?, false)
          """
        )
        insert_stmt.setString(1, logEntry.path.toString())
        insert_stmt.setString(2, java_temp_path)
        insert_stmt.setLong(3, logEntry.length)
        logDebug(color_text(insert_stmt.toString(), YELLOW))
        insert_stmt.executeUpdate()
        sql_connection.commit()
      } else {
        val insert_stmt = sql_connection.prepareStatement(
          s"""
          INSERT INTO $db_table(path, temp_path, length, mtime, is_complete)
          VALUES(?, ?, ?, ?, ?)
          ON CONFLICT(path)
          DO UPDATE SET temp_path=?, length=?, mtime=?, is_complete=?
          """
        )
        insert_stmt.setString(1, logEntry.path.toString())
        insert_stmt.setString(2, java_temp_path)
        insert_stmt.setLong(3, logEntry.length)
        insert_stmt.setLong(4, mtime)
        insert_stmt.setBoolean(5, logEntry.isComplete)

        insert_stmt.setString(6, java_temp_path)
        insert_stmt.setLong(7, logEntry.length)
        insert_stmt.setLong(8, mtime)
        insert_stmt.setBoolean(9, logEntry.isComplete)

        logDebug(color_text(insert_stmt.toString(), YELLOW))
        insert_stmt.executeUpdate()
        sql_connection.commit()
      }
    } catch {
      case e: Throwable => { // scalastyle:ignore
        if (
            e.isInstanceOf[SQLException]
            && e.asInstanceOf[SQLException].getSQLState() == UNIQUE_VIOLATION
        ) {
            sql_connection.rollback()
            throw new java.nio.file.FileAlreadyExistsException(logEntry.path.toString())
        }
        sql_connection.rollback()
        throw new java.nio.file.FileSystemException(logEntry.path.toString())
      }
    }
  }
}

class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next()
    def next(): ResultSet = rs
  }

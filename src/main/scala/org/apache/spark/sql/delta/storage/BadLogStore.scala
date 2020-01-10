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

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import java.sql.{DriverManager, Connection, ResultSet}

/**
 * Single Spark-driver/JVM LogStore implementation for S3.
 *
 * We assume the following from S3's [[FileSystem]] implementations:
 * - File writing on S3 is all-or-nothing, whether overwrite or not.
 * - List-after-write can be inconsistent.
 *
 * Regarding file creation, this implementation:
 * - Opens a stream to write to S3 (regardless of the overwrite option).
 * - Failures during stream write may leak resources, but may never result in partial writes.
 *
 * Regarding directory listing, this implementation:
 * - returns a list of files from db
 */
class BadLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  private def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
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

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)
    logInfo(s"write path: ${path}, ${overwrite}")

    val stream = fs.create(resolvedPath, true)
    val countingStream = new CountingOutputStream(stream)
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(countingStream.write)
    stream.close()
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
  }
}

object NaiveLogStore {
}

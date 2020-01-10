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

import scala.collection.JavaConverters._
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  PutItemRequest,
  QueryRequest,
  ConditionalCheckFailedException}
import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf


class DynamoDBLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf)
{
  import DynamoDBLogStore._

  private def logEntryToPutItemRequest(entry: LogEntry, overwrite: Boolean) = {
  }

  override def putLogEntry(
    logEntry: LogEntry,
    overwrite: Boolean): Unit =
  {
    val parentPath = logEntry.path.getParent()
    try {
      logInfo(color_text(s"putItem $logEntry, overwrite: $overwrite", YELLOW))
      client.putItem(logEntry.asPutItemRequest(overwrite))
    } catch {
      case e: ConditionalCheckFailedException => { // scalastyle:ignore
        logError(e.toString)
        throw new java.nio.file.FileAlreadyExistsException(logEntry.path.toString())
      }
      case e: Throwable => { // scalastyle:ignore
        logError(e.toString)
        throw new java.nio.file.FileSystemException(logEntry.path.toString())
      }
    }
  }

  override def listLogEntriesFrom(
    fs: FileSystem, parentPath: Path, from: Path): Iterator[LogEntry] =
  {
    val filename = from.getName()
    logInfo(color_text(s"query parentPath = $parentPath AND filename >= $filename", YELLOW))
    val result = client.query(
        new QueryRequest("delta_log")
        .withConsistentRead(true)
        .withKeyConditionExpression("parentPath = :path AND filename >= :filename")
        .withExpressionAttributeValues(
            Map(
                ":path" -> new AttributeValue(parentPath.toString()),
                ":filename" -> new AttributeValue(filename)
            ).asJava
        )
    ).getItems().asScala
    result.iterator.map( item => {
      logInfo(s"query result item: ${item.toString()}")
      val parentPath = item.get("parentPath").getS()
      val filename = item.get("filename").getS()
      val tempPath = Option(item.get("tempPath").getS()).map(new Path(_))
      val length = item.get("length").getN().toLong
      val modificationTime = item.get("modificationTime").getN().toLong
      val isComplete = item.get("isComplete").getBOOL()
      LogEntry(
          path = new Path(s"$parentPath/$filename"),
          tempPath = tempPath,
          length = length,
          modificationTime = modificationTime,
          isComplete = isComplete
      )
    })
  }

  val client = {
    val conf =
        new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")
    AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(conf).build()
  }
}

object DynamoDBLogStore {
  implicit def logEntryToWrapper(entry: LogEntry): LogEntryWrapper = LogEntryWrapper(entry)
}

case class LogEntryWrapper(entry: LogEntry) {
  def asPutItemRequest(overwrite: Boolean): PutItemRequest = {
    val pr = new PutItemRequest(
      "delta_log",
      Map(
          "parentPath" -> new AttributeValue(entry.path.getParent().toString()),
          "filename" -> new AttributeValue(entry.path.getName()),
          "tempPath" -> (entry.tempPath
            .map(path => new AttributeValue(path.toString))
            .getOrElse(new AttributeValue().withNULL(true))),
          "length" -> new AttributeValue().withN(entry.length.toString),
          "modificationTime" -> new AttributeValue().withN(System.currentTimeMillis().toString()),
          "isComplete" ->  new AttributeValue().withBOOL(entry.isComplete)
      ).asJava
    )
    if (!overwrite) pr.withConditionExpression("attribute_not_exists(filename)") else pr
  }
}
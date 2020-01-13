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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.auth.profile.ProfileCredentialsProvider

import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  PutItemRequest,
  QueryRequest,
  Condition,
  ComparisonOperator,
  ConditionalCheckFailedException}
import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
// import com.amazonaws.auth.AWSStaticCredentialsProvider

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
        .withKeyConditions(
          Map(
            "filename" -> new Condition()
              .withComparisonOperator(ComparisonOperator.GE)
              .withAttributeValueList(new AttributeValue(filename)),
            "parentPath" -> new Condition()
              .withComparisonOperator(ComparisonOperator.EQ)
              .withAttributeValueList(new AttributeValue(parentPath.toString()))
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
      val isComplete = Option(item.get("isComplete").getS()).map(_.toBoolean).getOrElse(false)
      LogEntry(
          path = new Path(s"$parentPath/$filename"),
          tempPath = tempPath,
          length = length,
          modificationTime = modificationTime,
          isComplete = isComplete
      )
    })
  }

  val accessKeyConfKey = "spark.delta.DynamoDBLogStore.aws_access_key"
  val secretKeyConfKey = "spark.delta.DynamoDBLogStore.aws_secret_key"
  val serviceEndpointConfKey = "spark.delta.DynamoDBLogStore.service_endpoint"
  val signingRegionConfKey = "spark.delta.DynamoDBLogStore.signing_region"

  val client = {
    val credentials = if (sparkConf.contains(accessKeyConfKey)) {
      new BasicAWSCredentials(sparkConf.get(accessKeyConfKey), sparkConf.get(secretKeyConfKey))
    } else {
      new ProfileCredentialsProvider().getCredentials()
    }
    val client = new AmazonDynamoDBClient(credentials)
    client.setRegion(Region.getRegion(Regions.US_WEST_2))
    client
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
          "tempPath" -> (
            entry.tempPath
            .map(path => new AttributeValue(path.toString))
            .getOrElse(new AttributeValue().withN("0"))
          ),
          "length" -> new AttributeValue().withN(entry.length.toString),
          "modificationTime" -> new AttributeValue().withN(System.currentTimeMillis().toString()),
          "isComplete" ->  new AttributeValue().withS(entry.isComplete.toString)
      ).asJava
    )
    if (!overwrite) {
      pr.withExpected(Map("filename" -> new ExpectedAttributeValue(false)).asJava)
    } else pr
  }
}
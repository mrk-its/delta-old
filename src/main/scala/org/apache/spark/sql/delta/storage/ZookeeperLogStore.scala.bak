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
import com.loopfor.zookeeper
import com.loopfor.zookeeper.{SynchronousZookeeper, Persistent, ACL, Status}
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.arrow.flatbuf.Bool

class ZookeeperLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf)
{

  override def putLogEntry(logEntry: LogEntry, overwrite: Boolean): Unit =
  {
    val znode_path = pathToZNode(logEntry.path)
    makeParentZnodes(client, znode_path)
    val data = ZNodeData(logEntry.tempPath, logEntry.length, logEntry.isComplete).encode()
    val data_str = new String(data, UTF_8)
    try {
      if (!overwrite) {
        logInfo(color_text(s"zookeeper create($znode_path, $data_str)", YELLOW))
        client.create(znode_path, data, ACL.AnyoneAll, Persistent)
      } else {
        logInfo(color_text(s"zookeeper set($znode_path, $data_str)", YELLOW))
        client.set(znode_path, data, None)
      }
    } catch {
      case e: NodeExistsException =>
        throw new java.nio.file.FileAlreadyExistsException(logEntry.path.toString())
      case e: Throwable => throw new java.nio.file.FileSystemException(logEntry.path.toString())
    }
  }

  override def listLogEntriesFrom(
    fs: FileSystem, parentPath: Path, from: Path): Iterator[LogEntry] =
  {
    val znodeParentPath = pathToZNode(parentPath)
    val fromName = from.getName()
    var children: Seq[String] = Seq()
    try {
      children = client.children(znodeParentPath)
    } catch {
      case e: NoNodeException => Iterator.empty  // TODO
        // throw new java.io.FileNotFoundException(parentPath.toString())
    }
    children.filter(_ >= fromName).map( name => {
      val (data, status) = client.get(s"$znodeParentPath/$name")
        val nodeData = ZNodeData.parse(data)
        logDebug(s"znode ${znodeParentPath}: $nodeData")
        LogEntry(
          new Path(s"$parentPath/$name"),
          nodeData.tempPath,
          nodeData.length, // length
          status.mtime,
          nodeData.isComplete
        )
    }).iterator
  }

  private def pathToZNode(path: Path) = {
    val uri = path.toUri()
    val scheme = uri.getScheme()
    val host = uri.getHost()
    val uri_path = uri.getPath()
    if (host != null) {
      s"/${scheme}/${host}${uri_path}"
    } else {
      s"/${scheme}${uri_path}"
    }
  }

  private def makeParentZnodes(client: SynchronousZookeeper, path: String): Unit = {
    logDebug(s"makeParentZnodes $path")
    val (head, _) = path splitAt(path lastIndexOf '/')  // rsplit('/', 1)
    if (head != "") {
      if (!client.exists(head).isDefined) {
        makeParentZnodes(client, head)
        try {
          client.create(head, Array(), ACL.AnyoneAll, Persistent)
          logDebug(s"created node $head")
        } catch {
          case e: NodeExistsException => return
        }
      }
    }
  }

  val serversConfKey: String = "spark.delta.ZookeeperLogStore.servers"
  val servers = sparkConf.get(serversConfKey)
    .split(',')
    .toSeq
    .map(host_port => {
      val parts = host_port.split(':');
      new java.net.InetSocketAddress(parts(0), parts(1).toInt)
    })

  private def getClient(): SynchronousZookeeper = {
    SynchronousZookeeper(zookeeper.Configuration(servers))
  }
  val client = getClient()
}

object ZookeeperLogStore {
}

case class ZNodeData(tempPath: Option[Path], length: Long, isComplete: Boolean) {
  def encode(): Array[Byte] = {
    val path = tempPath.map(_.toString()).getOrElse("")
    s"${path}|${length}|${isComplete}".getBytes(UTF_8)
  }
}

object ZNodeData {
  def parse(data: Array[Byte]): ZNodeData = {
    val row = new String(data, UTF_8).split('|')
    val tempPath = if (row(0).length() > 0) {
      Some(new Path(row(0)))
    } else {
      None
    }
    ZNodeData(tempPath, row(1).toLong, row(2).toBoolean)
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.profiler.ui

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.profiler.{FlamegraphNode, JfrConvertor, SparkProfilerTaskThreadEvent}
import org.apache.spark.util.Utils

/** Local recording snapshot owned by the profiler servlet lifecycle. */
private[profiler] class ProfilerStore(
    conf: SparkConf,
    directory: String,
    appId: String,
    attemptId: Option[String]) extends AutoCloseable {

  private val appPath = new Path(directory, Utils.nameForAppAndAttempt(appId, attemptId))
  private val executorFile = "profile-exec-(.+)\\.jfr".r
  private var localDirectory: Option[File] = None
  private var recordings: Seq[(String, File)] = Seq.empty

  private def download(): Unit = synchronized {
    require(localDirectory.isEmpty, "Profiler recordings have already been downloaded")
    val tempDir = Utils.createTempDir(namePrefix = "spark-profiler")
    localDirectory = Some(tempDir)
    Utils.tryWithSafeFinallyAndFailureCallbacks {
      val fs = appPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
      val files = if (fs.exists(appPath)) {
        fs.listStatus(appPath).toSeq.filter(_.isFile).flatMap { status =>
          status.getPath.getName match {
            case "profile-driver.jfr" => Some(SparkContext.DRIVER_IDENTIFIER -> status.getPath)
            case executorFile(id) => Some(id -> status.getPath)
            case _ => None
          }
        }.sortBy(_._1)
      } else {
        Seq.empty
      }
      recordings = files.zipWithIndex.map { case ((executorId, path), index) =>
        val local = new File(tempDir, s"profile-$index.jfr")
        fs.copyToLocalFile(false, path, new Path(local.toURI), true)
        executorId -> local
      }
    }(catchBlock = close())
  }

  // Keep cleanup from deleting recordings while a request is reading them.
  def flamegraph(
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent]): FlamegraphNode = synchronized {
    if (localDirectory.isEmpty) download()
    val root = FlamegraphNode("root")
    val tasksByExecutor = tasks.groupBy(_.executorId)
    recordings.foreach { case (executorId, local) =>
      val selected = selection match {
        case filter: ExecutorProfilerFilter => filter.executor.forall(_ == executorId)
        case _: TaskProfilerFilter => tasksByExecutor.contains(executorId)
      }
      if (selected) {
        JfrConvertor.convert(local, executorId, selection,
          tasksByExecutor.getOrElse(executorId, Nil), root)
      }
    }
    root
  }

  override def close(): Unit = synchronized {
    localDirectory.foreach(Utils.deleteRecursively)
    recordings = Seq.empty
    localDirectory = None
  }
}

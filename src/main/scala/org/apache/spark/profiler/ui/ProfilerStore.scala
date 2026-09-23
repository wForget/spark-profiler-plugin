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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.profiler.{FlamegraphNode, JfrConvertor}
import org.apache.spark.util.Utils

private[profiler] case class ProfilerResult(index: ProfilerIndex, graph: Option[FlamegraphNode])

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

  def download(): Unit = synchronized {
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
  def load(
      selection: ProfilerFilter,
      jobs: Seq[ProfilerJob],
      renderGraph: Boolean): ProfilerResult = synchronized {
    require(localDirectory.isDefined, "Profiler recordings have not been downloaded")
    val threads = new ArrayBuffer[ProfilerThread]
    val spans = new ArrayBuffer[TaskSpan]
    val graph = if (renderGraph) Some(FlamegraphNode("root")) else None
    recordings.foreach { case (executorId, local) =>
      if (selection.mode == ProfilerAnalyzeMode.Executor ||
          executorId != SparkContext.DRIVER_IDENTIFIER) {
        val (fileThreads, fileSpans) = JfrConvertor.scan(local, executorId)
        threads ++= fileThreads
        spans ++= fileSpans
        graph.foreach { root =>
          val index = ProfilerIndex(fileThreads, fileSpans, jobs)
          val filter = JfrConvertor.eventFilter(executorId, index, selection)
          JfrConvertor.convert(local, selection.sampleType, filter, root)
        }
      }
    }
    val index = ProfilerIndex(threads.toSeq, spans.toSeq, jobs)
    index.validate(selection)
    ProfilerResult(index, graph)
  }

  override def close(): Unit = synchronized {
    localDirectory.foreach(Utils.deleteRecursively)
    recordings = Seq.empty
    localDirectory = None
  }
}

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

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.profiler.{FlamegraphNode, JfrConvertor, SparkProfilerTaskThreadEvent}
import org.apache.spark.util.Utils

/** Reads selected recordings directly from the configured file system. */
private[profiler] class ProfilerStore(
    conf: SparkConf,
    directory: String,
    appId: String,
    attemptId: Option[String]) {

  private val appPath = new Path(directory, Utils.nameForAppAndAttempt(appId, attemptId))
  private val executorFile = "profile-exec-(.+)\\.jfr".r

  // Serialize requests to bound concurrent parsing and aggregation memory use.
  def flamegraph(
      selection: ProfilerFilter,
      tasks: Seq[SparkProfilerTaskThreadEvent]): FlamegraphNode = synchronized {
    val fs = appPath.getFileSystem(SparkHadoopUtil.get.newConfiguration(conf))
    val recordings = if (fs.exists(appPath)) {
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
    val root = FlamegraphNode("root")
    val tasksByExecutor = tasks.groupBy(_.executorId)
    recordings.foreach { case (executorId, path) =>
      val selected = selection match {
        case filter: ExecutorProfilerFilter => filter.executor.forall(_ == executorId)
        case _: TaskProfilerFilter => tasksByExecutor.contains(executorId)
      }
      if (selected) {
        JfrConvertor.convert(fs, path, executorId, selection,
          tasksByExecutor.getOrElse(executorId, Nil), root)
      }
    }
    root
  }
}

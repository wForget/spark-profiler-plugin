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

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.profiler.PROFILER_DFS_DIR
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore, KVUtils}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.ui.SparkUI

class ProfilerHistoryServerPlugin extends AppHistoryServerPlugin {
  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new ProfilerListener(store))
  }

  override def setupUI(ui: SparkUI): Unit = {
    if (ui.sc.isEmpty) {
      val directory = ui.conf.get(PROFILER_DFS_DIR).orElse {
        ui.store.asOption(ui.store.environmentInfo()).flatMap { environment =>
          environment.sparkProperties.toMap.get(PROFILER_DFS_DIR.key)
        }
      }
      directory.foreach { dir =>
        val app = ui.store.applicationInfo()
        val store = new ProfilerStore(ui.conf, dir,
          app.id, app.attempts.headOption.flatMap(_.attemptId))
        val servlet = new ProfilerServlet(store,
          () => KVUtils.viewToSeq(ui.store.store.view(classOf[ProfilerJob])))
        ui.attachHandler(ui.basePath + "/profiler/api", servlet, "/*")
        new ProfilerTab(ui)
      }
    }
  }
}

// Keep the SQL relationship here so the profiler does not depend on the SQL module.
private[profiler] class ProfilerJob(
    @KVIndexParam val jobId: Int,
    val sqlExecutionId: Option[Long],
    val stageIds: Seq[Int])

private[profiler] class ProfilerListener(store: ElementTrackingStore) extends SparkListener {
  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val executionId = Option(event.properties)
      .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
      .flatMap(id => Try(id.toLong).toOption)
    store.write(new ProfilerJob(event.jobId, executionId, event.stageIds))
  }
}

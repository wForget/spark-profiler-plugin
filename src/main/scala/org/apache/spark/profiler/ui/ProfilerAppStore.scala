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

import org.apache.spark.profiler.SparkProfilerTaskThreadEvent
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.{AppStatusStore, KVUtils}
import org.apache.spark.status.api.v1.{JobData, StageData, StageStatus, TaskData}

/** Task hierarchy metadata and task thread mappings from Spark's status store. */
case class ProfilerAppStore(store: AppStatusStore) {
  private val sqlStatusStore = new SQLAppStatusStore(store.store)

  def listExecutions(): Seq[Long] = {
    sqlStatusStore.executionsList().filter(_.stages.nonEmpty).map(_.executionId).sorted
  }

  def listJobs(executionId: Option[Long]): Seq[JobData] = {
    val jobs = executionId match {
      case Some(id) =>
        val execution = sqlStatusStore.execution(id)
          .getOrElse(throw new IllegalArgumentException("Unknown SQL execution"))
        execution.jobs.keys.toSeq.flatMap(id => store.asOption(store.job(id)))
      case None => store.jobsList(null)
    }
    jobs.sortBy(_.jobId)
  }

  private[profiler] def listStages(filter: TaskProfilerFilter): Seq[StageData] = {
    val jobs = listJobs(filter.sqlExecution)
    require(filter.job.forall(id => jobs.exists(_.jobId == id)),
      "Unknown job or job outside the selected SQL execution")
    val stages = if (filter.job.isDefined || filter.sqlExecution.isDefined) {
      jobs.filter(j => filter.job.forall(_ == j.jobId)).flatMap(_.stageIds).distinct
        .flatMap(id => store.stageData(id))
    } else {
      store.stageList(null)
    }
    stages.filterNot(_.status == StageStatus.SKIPPED).sortBy(s => (s.stageId, s.attemptId))
  }

  private[profiler] def selectedStages(filter: TaskProfilerFilter): Seq[StageData] = {
    filter.stage match {
      case Some(id) =>
        val parts = id.split(":", -1)
        require(parts.length == 2, "Invalid stage attempt")
        val stage = store.asOption(store.stageAttempt(parts(0).toInt, parts(1).toInt)._1)
          .getOrElse(throw new IllegalArgumentException("Unknown stage attempt"))
        Seq(stage)
      case None => listStages(filter)
    }
  }

  def listTasks(stageId: Int, attemptId: Int): Seq[TaskData] = {
    store.taskList(stageId, attemptId, Int.MaxValue)
  }

  def taskThreadEvents(executorId: Option[String] = None): Seq[SparkProfilerTaskThreadEvent] = {
    val view = store.store.view(classOf[SparkProfilerTaskThreadEvent])
    val selected = executorId.fold(view)(id => view.index("executorId").first(id).last(id))
    KVUtils.viewToSeq(selected)
  }

  def hasTaskThreadEvents: Boolean = store.store.count(classOf[SparkProfilerTaskThreadEvent]) > 0

  private[profiler] def selectedTaskEvents(
      filter: TaskProfilerFilter): Seq[SparkProfilerTaskThreadEvent] = {
    if (filter.tasks.nonEmpty) {
      filter.tasks.toSeq.sorted.flatMap { taskId =>
        store.asOption(store.store.read(classOf[SparkProfilerTaskThreadEvent], taskId))
      }
    } else if (filter.sqlExecution.isEmpty && filter.job.isEmpty && filter.stage.isEmpty) {
      taskThreadEvents()
    } else {
      val tasks = selectedStages(filter).flatMap(s => listTasks(s.stageId, s.attemptId))
      tasks.flatMap { task =>
        store.asOption(store.store.read(classOf[SparkProfilerTaskThreadEvent], task.taskId))
          .filter(_.executorId == task.executorId)
      }
    }
  }
}

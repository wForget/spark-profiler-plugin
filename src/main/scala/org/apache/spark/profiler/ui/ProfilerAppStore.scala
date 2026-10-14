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

import org.apache.spark.SparkContext
import org.apache.spark.profiler.SparkProfilerTaskThreadEvent
import org.apache.spark.sql.execution.ui.SQLAppStatusStore
import org.apache.spark.status.{AppStatusStore, KVUtils}
import org.apache.spark.status.api.v1.{JobData, StageData, TaskData}

/** Executor and task hierarchy metadata from Spark's status store. */
case class ProfilerAppStore(store: AppStatusStore) {
  private val sqlStatusStore = new SQLAppStatusStore(store.store)

  def listExecutors(): Seq[String] = {
    (SparkContext.DRIVER_IDENTIFIER +: store.executorList(activeOnly = false).map(_.id))
      .distinct.sorted
  }

  def listExecutions(): Seq[Long] = sqlStatusStore.executionsList().map(_.executionId).sorted

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
    stages.sortBy(s => (s.stageId, s.attemptId))
  }

  private[profiler] def selectedStages(filter: TaskProfilerFilter): Seq[StageData] = {
    val stages = listStages(filter)
    require(filter.stage.forall(id => stages.exists(s => s"${s.stageId}:${s.attemptId}" == id)),
      "Unknown stage attempt or stage outside the selected job")
    stages.filter(s => filter.stage.forall(_ == s"${s.stageId}:${s.attemptId}"))
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

  private[profiler] def validateExecutor(filter: ExecutorProfilerFilter): Unit = {
    require(filter.executor.forall(listExecutors().contains), "Unknown executor")
  }

  private[profiler] def selectedTaskEvents(
      filter: TaskProfilerFilter): Seq[SparkProfilerTaskThreadEvent] = {
    require(filter.tasks.isEmpty || filter.stage.isDefined,
      "Select a stage attempt before selecting tasks")
    if (filter.sqlExecution.isEmpty && filter.job.isEmpty && filter.stage.isEmpty) {
      taskThreadEvents()
    } else {
      val tasks = selectedStages(filter).flatMap(s => listTasks(s.stageId, s.attemptId))
      require(filter.tasks.subsetOf(tasks.map(_.taskId).toSet),
        "Unknown task attempt or task outside the selected stage")
      tasks.filter(t => filter.tasks.isEmpty || filter.tasks.contains(t.taskId)).flatMap { task =>
        store.asOption(store.store.read(classOf[SparkProfilerTaskThreadEvent], task.taskId))
          .filter(_.executorId == task.executorId)
      }
    }
  }
}

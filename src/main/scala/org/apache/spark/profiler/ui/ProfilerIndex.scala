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

import org.apache.spark.SparkContext

private[profiler] case class ProfilerThread(
    executorId: String,
    threadId: Int,
    name: String,
    startTime: Long,
    endTime: Long) {
  def id: String = s"$executorId:$threadId"
  def label: String = s"Executor $executorId / $name (tid=$threadId)"
}

private[profiler] case class TaskSpan(
    executorId: String,
    threadId: Int,
    startNanos: Long,
    endNanos: Long,
    stageId: Int,
    stageAttempt: Int,
    partitionId: Int,
    taskId: Long,
    taskAttempt: Int) {
  def stageKey: String = s"$stageId:$stageAttempt"
  def taskKey: String = s"$stageKey:$taskId:$taskAttempt"
  def label: String = s"Stage $stageId (attempt $stageAttempt) / " +
    s"partition $partitionId / task $taskId (attempt $taskAttempt)"
}

private[profiler] object TaskSpan {
  def parse(
      tag: String,
      executorId: String,
      threadId: Int,
      startNanos: Long,
      endNanos: Long): Option[TaskSpan] = {
    Option(tag).filter(_.startsWith("SparkTask:")).flatMap { value =>
      Try {
        val fields = value.stripPrefix("SparkTask:").split(",").map { field =>
          val pair = field.split("=", 2)
          require(pair.length == 2, "Invalid task span field")
          pair(0) -> pair(1)
        }.toMap
        val span = TaskSpan(executorId, threadId, startNanos, endNanos,
          fields("stageId").toInt, fields("stageAttemptNumber").toInt,
          fields("partitionId").toInt, fields("taskAttemptId").toLong,
          fields("attemptNumber").toInt)
        require(span.stageId >= 0 && span.stageAttempt >= 0 && span.partitionId >= 0 &&
          span.taskId >= 0 && span.taskAttempt >= 0 && endNanos > startNanos,
          "Invalid task span")
        span
      }.toOption
    }
  }
}

private[profiler] case class ProfilerIndex(
    threads: Seq[ProfilerThread],
    spans: Seq[TaskSpan],
    jobs: Seq[ProfilerJob]) {

  def threadOptions(filter: ExecutorProfilerFilter): Seq[ProfilerThread] = {
    threads.filter(t => filter.executor.forall(_ == t.executorId))
      .distinct.sortBy(t => (t.executorId, t.threadId))
  }

  def timeRange(filter: ExecutorProfilerFilter): Option[(Long, Long)] = {
    val selected = threadOptions(filter)
    Option.when(selected.nonEmpty) {
      (selected.map(_.startTime).min, selected.map(_.endTime).max)
    }
  }

  private lazy val sampledSpans: Seq[TaskSpan] = {
    val sampledThreads = threads.map(_.id).toSet
    spans.filter(s => sampledThreads.contains(s"${s.executorId}:${s.threadId}"))
  }

  def executionOptions(filter: TaskProfilerFilter): Seq[Long] = {
    jobOptions(filter.copy(sqlExecution = None)).flatMap(_.sqlExecutionId).distinct.sorted
  }

  def jobOptions(filter: TaskProfilerFilter): Seq[ProfilerJob] = {
    val stageIds = sampledSpans.map(_.stageId).toSet
    jobs.filter(j => j.stageIds.exists(stageIds.contains) &&
      filter.sqlExecution.forall(id => j.sqlExecutionId.contains(id)))
      .sortBy(_.jobId)
  }

  def stageSpans(filter: TaskProfilerFilter): Seq[TaskSpan] = {
    val matchingJobs = jobOptions(filter).filter(j => filter.job.forall(_ == j.jobId))
    val stageIds = matchingJobs.flatMap(_.stageIds).toSet
    val restrictJobs = filter.sqlExecution.isDefined || filter.job.isDefined
    sampledSpans.filter(s => !restrictJobs || stageIds.contains(s.stageId))
  }

  def taskOptions(filter: TaskProfilerFilter): Seq[TaskSpan] = {
    stageSpans(filter).filter(s => filter.stage.forall(_ == s.stageKey))
      .groupBy(_.taskKey).values.map(_.head).toSeq
      .sortBy(s => (s.stageId, s.stageAttempt, s.taskId, s.taskAttempt))
  }

  def selectedSpans(filter: TaskProfilerFilter): Seq[TaskSpan] = {
    stageSpans(filter).filter(s => filter.stage.forall(_ == s.stageKey) &&
      (filter.tasks.isEmpty || filter.tasks.contains(s.taskKey)))
  }

  def validate(selection: ProfilerFilter): Unit = selection match {
    case filter: ExecutorProfilerFilter =>
      // Keep the default driver selectable even when it has no samples.
      require(filter.executor.forall(id => id == SparkContext.DRIVER_IDENTIFIER ||
        threads.exists(_.executorId == id)), "Unknown executor")
      require(filter.threads.subsetOf(threadOptions(filter).map(_.id).toSet),
        "Unknown thread or thread outside the selected executor")
    case filter: TaskProfilerFilter =>
      require(filter.sqlExecution.forall(id => executionOptions(filter).contains(id)),
        "Unknown SQL execution")
      require(filter.job.forall(id => jobOptions(filter).exists(_.jobId == id)),
        "Unknown job or job outside the selected SQL execution")
      require(filter.stage.forall(id => stageSpans(filter).exists(_.stageKey == id)),
        "Unknown stage attempt or stage outside the selected job")
      require(filter.tasks.subsetOf(taskOptions(filter).map(_.taskKey).toSet),
        "Unknown task attempt or task outside the selected stage")
  }
}

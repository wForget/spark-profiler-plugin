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

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext

private[profiler] case class ProfilerThread(
    executorId: String,
    threadId: Int,
    name: String,
    sampleType: String) {
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

private[profiler] case class ProfilerFilter(
    mode: String = "executor",
    sampleType: String = "wall",
    executor: Option[String] = None,
    threads: Set[String] = Set.empty,
    sqlExecution: Option[Long] = None,
    job: Option[Int] = None,
    stage: Option[String] = None,
    tasks: Set[String] = Set.empty) {
  require(Set("executor", "task").contains(mode), "Unknown profiler mode")
  require(Set("wall", "cpu").contains(sampleType), "Unknown sample type")
  require(if (mode == "executor") {
    sqlExecution.isEmpty && job.isEmpty && stage.isEmpty && tasks.isEmpty
  } else {
    executor.isEmpty && threads.isEmpty
  }, "Cannot combine executor and task filters")
}

private[profiler] object ProfilerFilter {
  def fromRequest(request: HttpServletRequest, jobs: Seq[ProfilerJob]): ProfilerFilter = {
    def values(name: String): Seq[String] = {
      Option(request.getParameterValues(name)).toSeq.flatMap(_.toSeq).filter(_.nonEmpty)
    }
    def single(name: String): Option[String] = {
      val selected = values(name)
      require(selected.size <= 1, s"Only one $name may be selected")
      selected.headOption
    }
    val mode = single("mode").getOrElse("executor")
    val executor = single("executor").orElse {
      Option.when(mode == "executor")(SparkContext.DRIVER_IDENTIFIER)
    }
    val sqlExecution = single("sqlExecution").map(_.toLong)
    val job = single("job").map(_.toInt)
    val defaultJob = if (mode == "task") {
      jobs.filter(j => job.forall(_ == j.jobId) &&
        sqlExecution.forall(id => j.sqlExecutionId.contains(id))).minByOption(_.jobId)
    } else {
      None
    }
    // An explicit empty parameter means All, so only default omitted parameters.
    val executionSelection = if (request.getParameterValues("sqlExecution") == null) {
      defaultJob.flatMap(_.sqlExecutionId)
    } else {
      sqlExecution
    }
    val jobSelection = if (request.getParameterValues("job") == null) {
      defaultJob.map(_.jobId)
    } else {
      job
    }
    ProfilerFilter(
      mode = mode,
      sampleType = single("sampleType").getOrElse("wall"),
      executor = executor,
      threads = values("thread").toSet,
      sqlExecution = executionSelection,
      job = jobSelection,
      stage = single("stage"),
      tasks = values("task").toSet)
  }
}

private[profiler] case class ProfilerIndex(
    threads: Seq[ProfilerThread],
    spans: Seq[TaskSpan],
    jobs: Seq[ProfilerJob]) {

  def threadOptions(filter: ProfilerFilter): Seq[ProfilerThread] = {
    threads.filter(t => t.sampleType == filter.sampleType &&
      filter.executor.forall(_ == t.executorId)).distinct.sortBy(t => (t.executorId, t.threadId))
  }

  private def sampledSpans(filter: ProfilerFilter): Seq[TaskSpan] = {
    val sampledThreads = threads.filter(_.sampleType == filter.sampleType).map(_.id).toSet
    spans.filter(s => sampledThreads.contains(s"${s.executorId}:${s.threadId}"))
  }

  def executionOptions(filter: ProfilerFilter): Seq[Long] = {
    jobOptions(filter.copy(sqlExecution = None)).flatMap(_.sqlExecutionId).distinct.sorted
  }

  def jobOptions(filter: ProfilerFilter): Seq[ProfilerJob] = {
    val stageIds = sampledSpans(filter).map(_.stageId).toSet
    jobs.filter(j => j.stageIds.exists(stageIds.contains) &&
      filter.sqlExecution.forall(id => j.sqlExecutionId.contains(id)))
      .sortBy(_.jobId)
  }

  def stageSpans(filter: ProfilerFilter): Seq[TaskSpan] = {
    val matchingJobs = jobOptions(filter).filter(j => filter.job.forall(_ == j.jobId))
    val stageIds = matchingJobs.flatMap(_.stageIds).toSet
    val restrictJobs = filter.sqlExecution.isDefined || filter.job.isDefined
    sampledSpans(filter).filter(s => !restrictJobs || stageIds.contains(s.stageId))
  }

  def taskOptions(filter: ProfilerFilter): Seq[TaskSpan] = {
    stageSpans(filter).filter(s => filter.stage.forall(_ == s.stageKey))
      .groupBy(_.taskKey).values.map(_.head).toSeq
      .sortBy(s => (s.stageId, s.stageAttempt, s.taskId, s.taskAttempt))
  }

  def selectedSpans(filter: ProfilerFilter): Seq[TaskSpan] = {
    stageSpans(filter).filter(s => filter.stage.forall(_ == s.stageKey) &&
      (filter.tasks.isEmpty || filter.tasks.contains(s.taskKey)))
  }

  def validate(filter: ProfilerFilter): Unit = {
    if (filter.mode == "executor") {
      // Keep the default driver selectable even when it has no samples of the requested type.
      require(filter.executor.forall(id => id == SparkContext.DRIVER_IDENTIFIER ||
        threads.exists(t => t.executorId == id && t.sampleType == filter.sampleType)),
        "Unknown executor for this sample type")
      require(filter.threads.subsetOf(threadOptions(filter).map(_.id).toSet),
        "Unknown thread or thread outside the selected executor")
    } else {
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
}

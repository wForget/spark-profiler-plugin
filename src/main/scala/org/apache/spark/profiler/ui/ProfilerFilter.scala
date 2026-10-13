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

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext

private[profiler] object ProfilerAnalyzeMode extends Enumeration {
  type ProfilerAnalyzeMode = Value
  val Executor: Value = Value("executor")
  val Task: Value = Value("task")

  def fromString(value: String): Value = {
    values.find(_.toString == value).getOrElse {
      throw new IllegalArgumentException("Unknown profiler mode")
    }
  }
}

private[profiler] object ProfilerSampleType extends Enumeration {
  type ProfilerSampleType = Value
  val Wall: Value = Value("wall")
  val Cpu: Value = Value("cpu")
  val NativeMemory: Value = Value("nativemem")

  def fromString(value: String): Value = {
    values.find(_.toString == value).getOrElse {
      throw new IllegalArgumentException("Unknown sample type")
    }
  }
}

private[profiler] sealed trait ProfilerFilter {
  def mode: ProfilerAnalyzeMode.Value
  def sampleType: ProfilerSampleType.Value
}

private[profiler] case class ExecutorProfilerFilter(
    override val sampleType: ProfilerSampleType.Value = ProfilerSampleType.Wall,
    executor: Option[String] = None,
    threads: Set[String] = Set.empty,
    startTime: Option[Long] = None,
    endTime: Option[Long] = None) extends ProfilerFilter {
  override val mode: ProfilerAnalyzeMode.Value = ProfilerAnalyzeMode.Executor

  // Request timestamps are epoch milliseconds; the JFR filter uses epoch nanoseconds.
  require((startTime.toSeq ++ endTime.toSeq).forall(t =>
    t >= 0 && t <= Long.MaxValue / 1000000L), "Time must be a valid epoch millisecond value")
  require(startTime.forall(start => endTime.forall(_ > start)),
    "End time must be after start time")
}

private[profiler] case class TaskProfilerFilter(
    override val sampleType: ProfilerSampleType.Value = ProfilerSampleType.Wall,
    sqlExecution: Option[Long] = None,
    job: Option[Int] = None,
    stage: Option[String] = None,
    tasks: Set[Long] = Set.empty) extends ProfilerFilter {
  override val mode: ProfilerAnalyzeMode.Value = ProfilerAnalyzeMode.Task
}

private[profiler] object ProfilerFilter {
  def fromRequest(request: HttpServletRequest): ProfilerFilter = {
    ProfilerAnalyzeMode.fromString(single(request, "mode").getOrElse("executor")) match {
      case ProfilerAnalyzeMode.Executor => executorFromRequest(request)
      case ProfilerAnalyzeMode.Task => taskFromRequest(request)
    }
  }

  private def executorFromRequest(request: HttpServletRequest): ExecutorProfilerFilter = {
    require(Seq("sqlExecution", "job", "stage", "task").forall(values(request, _).isEmpty),
      "Cannot combine executor and task filters")
    ExecutorProfilerFilter(
      sampleType = single(request, "sampleType").map(ProfilerSampleType.fromString)
        .getOrElse(ProfilerSampleType.Wall),
      executor = single(request, "executor").orElse(Some(SparkContext.DRIVER_IDENTIFIER)),
      threads = values(request, "thread").toSet,
      startTime = single(request, "startTime").map(_.toLong),
      endTime = single(request, "endTime").map(_.toLong))
  }

  private def taskFromRequest(request: HttpServletRequest): TaskProfilerFilter = {
    require(Seq("executor", "thread", "startTime", "endTime")
      .forall(values(request, _).isEmpty), "Cannot combine executor and task filters")
    TaskProfilerFilter(
      sampleType = single(request, "sampleType").map(ProfilerSampleType.fromString)
        .getOrElse(ProfilerSampleType.Wall),
      sqlExecution = single(request, "sqlExecution").map(_.toLong),
      job = single(request, "job").map(_.toInt),
      stage = single(request, "stage"),
      tasks = values(request, "task").map(_.toLong).toSet)
  }

  private def values(request: HttpServletRequest, name: String): Seq[String] = {
    Option(request.getParameterValues(name)).toSeq.flatMap(_.toSeq).filter(_.nonEmpty)
  }

  private def single(request: HttpServletRequest, name: String): Option[String] = {
    val selected = values(request, name)
    require(selected.size <= 1, s"Only one $name may be selected")
    selected.headOption
  }
}

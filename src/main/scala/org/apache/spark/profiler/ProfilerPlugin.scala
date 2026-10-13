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
package org.apache.spark.profiler

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext, TaskContext, TaskFailedReason}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.EXECUTOR_ID

/**
 * Spark plugin to do profiling
 */
class ProfilerPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ProfilerDriverPlugin

  override def executorPlugin(): ExecutorPlugin = new ProfilerExecutorPlugin
}

class ProfilerDriverPlugin extends DriverPlugin with Logging {

  private var sparkConf: SparkConf = _
  private var pluginCtx: PluginContext = _
  private var profiler: SparkAsyncProfiler = _
  private var driverProfilingEnabled: Boolean = _
  private var sparkContext: SparkContext = _
  private var taskThreadEventEnabled: Boolean = _

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    pluginCtx = ctx
    sparkConf = ctx.conf()
    sparkContext = sc
    taskThreadEventEnabled = sparkConf.get(PROFILER_EXECUTOR_TASK_THREAD_EVENT_ENABLED) &&
      sparkConf.getBoolean("spark.eventLog.enabled", false)
    driverProfilingEnabled = sparkConf.get(PROFILER_DRIVER_ENABLED)
    if (driverProfilingEnabled) {
      logInfo("Driver starting profiling")
      profiler = new SparkAsyncProfiler(sparkConf, pluginCtx.executorID())
      profiler.start()
    }

    Map(PROFILER_EXECUTOR_TASK_THREAD_EVENT_ENABLED.key -> taskThreadEventEnabled.toString).asJava
  }

  override def receive(message: AnyRef): AnyRef = {
    message match {
      case event: SparkProfilerTaskThreadEvent if taskThreadEventEnabled =>
        sparkContext.listenerBus.post(event)
      case _: SparkProfilerTaskThreadEvent =>
      case _ => throw new IllegalArgumentException("Unexpected profiler plugin message")
    }
    null
  }

  override def shutdown(): Unit = {
    logInfo("Driver profiler shutting down")
    if (profiler != null) {
      profiler.stop()
    }
  }
}

class ProfilerExecutorPlugin extends ExecutorPlugin with Logging {

  private var sparkConf: SparkConf = _
  private var pluginCtx: PluginContext = _
  private var profiler: SparkAsyncProfiler = _
  private var executorProfilerEnabled: Boolean = _
  private var executorProfilerFraction: Double = _
  private var taskThreadEventEnabled: Boolean = _
  private val rand: Random = new Random(System.currentTimeMillis())
  // TaskContext is cleared before the task success and failure callbacks run.
  private val taskThreadEvent = new ThreadLocal[SparkProfilerTaskThreadEvent]

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    pluginCtx = ctx
    sparkConf = ctx.conf()
    executorProfilerEnabled = sparkConf.get(PROFILER_EXECUTOR_ENABLED)
    taskThreadEventEnabled = java.lang.Boolean.parseBoolean(
      extraConf.get(PROFILER_EXECUTOR_TASK_THREAD_EVENT_ENABLED.key))
    if (executorProfilerEnabled) {
      executorProfilerFraction = sparkConf.get(PROFILER_EXECUTOR_FRACTION)
      if (rand.nextInt(100) * 0.01 < executorProfilerFraction) {
        logInfo(log"Executor id ${MDC(EXECUTOR_ID, pluginCtx.executorID())} " +
          log"selected for profiling")
        profiler = new SparkAsyncProfiler(sparkConf, pluginCtx.executorID())
        profiler.start()
      }
    }
    Map.empty[String, String].asJava
  }

  override def onTaskStart(): Unit = {
    if (taskThreadEventEnabled) {
      taskThreadEvent.remove()
      val context = TaskContext.get()
      taskThreadEvent.set(SparkProfilerTaskThreadEvent(
        context.taskAttemptId(), pluginCtx.executorID(), Thread.currentThread().getId,
        System.currentTimeMillis(), 0L))
    }
  }

  override def onTaskSucceeded(): Unit = {
    sendTaskEvent()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    sendTaskEvent()
  }

  private def sendTaskEvent(): Unit = {
    if (taskThreadEventEnabled) {
      val event = taskThreadEvent.get()
      taskThreadEvent.remove()
      if (event != null) {
        try {
          pluginCtx.send(event.copy(endTime = System.currentTimeMillis()))
        } catch {
          case NonFatal(e) => logWarning("Failed to report task thread event", e)
        }
      }
    }
  }

  override def shutdown(): Unit = {
    logInfo("Executor profiler shutting down")
    if (profiler != null) {
      profiler.stop()
    }
  }
}

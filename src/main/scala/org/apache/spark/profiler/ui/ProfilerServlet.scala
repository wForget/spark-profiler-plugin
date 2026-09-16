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

import scala.util.control.NonFatal

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.json4s.JsonDSL._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[profiler] class ProfilerServlet(
    store: ProfilerStore,
    jobs: () => Seq[ProfilerJob]) extends HttpServlet with Logging {

  override def init(): Unit = {
    Utils.tryWithSafeFinallyAndFailureCallbacks {
      super.init()
      store.download()
    }(catchBlock = store.close())
  }

  override def destroy(): Unit = {
    Utils.tryWithSafeFinally {
      super.destroy()
    } {
      store.close()
    }
  }

  override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setContentType("application/json;charset=utf-8")
    response.setHeader("Cache-Control", "no-store")
    val result = try {
      request.getPathInfo match {
        case "/filters" => filters(ProfilerFilter.fromRequest(request))
        case "/flamegraph" => flamegraph(ProfilerFilter.fromRequest(request))
        case _ =>
          response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          compact(render(("error" -> "Unknown profiler endpoint"): JValue))
      }
    } catch {
      case e: IllegalArgumentException =>
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        compact(render(("error" -> e.getMessage): JValue))
      case NonFatal(e) =>
        logWarning("Unable to read profiler recordings", e)
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        compact(render(("error" -> ("Unable to read profiler recordings. " +
          "Check the History Server logs and the downloaded JFR files.")): JValue))
    }
    response.getWriter.print(result)
  }

  override def doTrace(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
  }

  private def filters(filter: ProfilerFilter): String = {
    val index = store.load(filter, jobs(), renderGraph = false).index
    val executors = index.threads.filter(_.sampleType == filter.sampleType)
      .map(_.executorId).filterNot(_ == SparkContext.DRIVER_IDENTIFIER).distinct.sorted
    val stages = index.stageSpans(filter).map(s => (s.stageId, s.stageAttempt)).distinct.sorted
    val options: JValue =
      ("executors" -> (SparkContext.DRIVER_IDENTIFIER +: executors)) ~
      ("threads" -> index.threadOptions(filter).map { t =>
        ("id" -> t.id) ~ ("name" -> t.name) ~
          ("executorId" -> t.executorId) ~ ("threadId" -> t.threadId)
      }) ~
      ("sqlExecutions" -> index.executionOptions(filter).map(_.toString)) ~
      ("jobs" -> index.jobOptions(filter).map(_.jobId.toString)) ~
      ("stages" -> stages.map { case (id, attempt) =>
        ("id" -> s"$id:$attempt") ~ ("stageId" -> id) ~ ("attempt" -> attempt)
      }) ~
      ("tasks" -> index.taskOptions(filter).map { t =>
        ("id" -> t.taskKey) ~ ("taskId" -> t.taskId.toString) ~
          ("attempt" -> t.taskAttempt) ~ ("stageId" -> t.stageId) ~
          ("stageAttempt" -> t.stageAttempt) ~ ("partitionId" -> t.partitionId)
      }) ~
      ("hasSamples" -> index.threads.nonEmpty) ~ ("hasTaskSpans" -> index.spans.nonEmpty)
    compact(render(options))
  }

  private def flamegraph(filter: ProfilerFilter): String = {
    store.load(filter, jobs(), renderGraph = true).graph.get.toJsonString
  }
}

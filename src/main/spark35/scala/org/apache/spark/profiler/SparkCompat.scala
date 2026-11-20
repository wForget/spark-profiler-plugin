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

import org.apache.spark.deploy.history.EventLogFileWriter
import org.apache.spark.internal.Logging

private[profiler] object SparkCompat {
  type HttpServlet = javax.servlet.http.HttpServlet
  type HttpServletRequest = javax.servlet.http.HttpServletRequest
  type HttpServletResponse = javax.servlet.http.HttpServletResponse

  val chartAssets = "/static/profiler"

  def nameForAppAndAttempt(appId: String, attemptId: Option[String]): String =
    EventLogFileWriter.nameForAppAndAttempt(appId, attemptId)
}

private[profiler] trait ProfilerLogging extends Logging {
  protected def logExecutorSelected(executorId: String): Unit =
    logInfo(s"Executor id $executorId selected for profiling")

  protected def logCopyTo(path: String): Unit =
    logInfo(s"Copying profiling file to $path")
}

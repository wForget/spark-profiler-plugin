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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.UIUtils.prependBaseUri

private[profiler] class ProfilerPage(parent: ProfilerTab) extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val pagePath = prependBaseUri(request, parent.basePath + "/profiler/")
    val content =
      <div id="profiler" data-api-base={pagePath + "api/"}>
        <nav class="nav nav-underline border-bottom gap-4 mb-4" aria-label="Profiling mode">
          <a class="nav-link fw-medium" data-mode="executor"
              href={pagePath + "?mode=executor"}>Executor / Thread</a>
          <a class="nav-link fw-medium" data-mode="task"
              href={pagePath + "?mode=task"}>SQL / Task</a>
        </nav>
        <div id="profiler-error" class="alert alert-warning d-none" role="alert"></div>
        <div id="profiler-loading" role="status" class="text-body-secondary">
          Loading filters...
        </div>
        <form id="profiler-filters" action={pagePath}>
          <input type="hidden" name="mode" value="executor"/>
          <div id="profiler-sample-type"
              class="d-flex align-items-center flex-wrap gap-3 border-bottom pb-3 mb-3"></div>
          <div id="profiler-hierarchy" class="text-body-secondary mb-3"></div>
          <div id="profiler-options" class="row">
            <div class="col-md-3 mb-3 d-none">
              <label for="profiler-start-time" class="form-label">Start time (inclusive)</label>
              <input id="profiler-start-time" name="startTime" type="datetime-local"
                  step="0.001" class="form-control" disabled="disabled"/>
            </div>
            <div class="col-md-3 mb-3 d-none">
              <label for="profiler-end-time" class="form-label">End time (exclusive)</label>
              <input id="profiler-end-time" name="endTime" type="datetime-local"
                  step="0.001" class="form-control" disabled="disabled"/>
            </div>
          </div>
          <p id="profiler-time-help" class="text-body-secondary d-none"></p>
          <p id="profiler-help" class="text-body-secondary border-top pt-3"></p>
          <button class="btn btn-primary" type="submit" disabled="disabled">
            Show flame graph
          </button>
          <a id="profiler-reset" class="btn btn-secondary ms-2" href={pagePath}>Reset</a>
        </form>
        <p id="profiler-empty" class="alert alert-info mt-3 d-none"></p>
        <div id="profiler-graph" class="mt-3 d-none">
          <p>Frame widths represent sample counts. Click a frame to zoom.</p>
          <button id="profiler-reset-zoom" type="button" class="btn btn-secondary">
            Reset zoom
          </button>
          <div id="profiler-chart" class="mt-3"></div>
          <div id="profiler-details"></div>
        </div>
        <noscript>Enable JavaScript to load profiler filters and flame graphs.</noscript>
        <link rel="stylesheet" type="text/css"
            href={prependBaseUri(request, "/static/d3-flamegraph.css")}/>
        <link rel="stylesheet" type="text/css"
            href={prependBaseUri(request, "/static/profiler/profiler.css")}/>
        <script src={prependBaseUri(request, "/static/d3.min.js")}></script>
        <script src={prependBaseUri(request, "/static/d3-flamegraph.min.js")}></script>
        <script type="module"
            src={prependBaseUri(request, "/static/profiler/profiler.js")}></script>
      </div>
    UIUtils.headerSparkPage(request, "Profiler", content, parent)
  }
}

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

/* global d3, flamegraph */

export function initializeSelect(container) {
  const select = container.querySelector("select");
  const find = name => container.querySelector(`.profiler-select-${name}`);
  const toggle = container.querySelector("button[aria-expanded]");
  const dropdown = bootstrap.Dropdown.getOrCreateInstance(toggle);
  const panel = find("panel");
  const search = find("search");
  const onlySelected = find("only");
  const list = find("options");
  const chips = find("chips");
  const selectResults = find("select");
  const pages = find("pages");
  const previous = pages.querySelector('[data-direction="previous"]');
  const next = pages.querySelector('[data-direction="next"]');
  const plural = container.dataset.plural;
  const pageSize = 50;
  let page = 0;
  let matches = [];
  const entries = Array.from(select.options, (option, index) => ({
    option,
    index,
    label: option.dataset.label,
    detail: option.dataset.detail,
    summary: option.dataset.summary,
    searchText: `${option.textContent} ${option.value}`.toLowerCase()
  }));

  function renderOptions() {
    const query = search.value.trim().toLowerCase();
    matches = entries.filter(entry => entry.searchText.includes(query) &&
      (!onlySelected?.checked || entry.option.selected));
    page = Math.min(page, Math.max(0, Math.ceil(matches.length / pageSize) - 1));
    const start = page * pageSize;
    const visible = matches.slice(start, start + pageSize);
    const focusedIndex = document.activeElement.dataset.optionIndex;
    const scrollTop = list.scrollTop;
    list.replaceChildren();
    for (const entry of visible) {
      if (!select.multiple) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "dropdown-item text-wrap rounded";
        button.textContent = entry.label;
        button.dataset.optionIndex = entry.index;
        button.disabled = entry.option.disabled;
        button.classList.toggle("active", entry.option.selected);
        if (entry.option.selected) button.setAttribute("aria-current", "true");
        button.addEventListener("click", () => {
          const changed = select.value !== entry.option.value;
          select.value = entry.option.value;
          dropdown.hide();
          toggle.focus();
          if (changed) select.dispatchEvent(new Event("change", {bubbles: true}));
        });
        list.append(button);
        continue;
      }
      const row = document.createElement("label");
      row.className = "profiler-select-option d-flex gap-2 p-2 rounded";
      row.classList.toggle("bg-primary-subtle", entry.option.selected);
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.className = "form-check-input flex-shrink-0";
      checkbox.checked = entry.option.selected;
      checkbox.dataset.optionIndex = entry.index;
      checkbox.addEventListener("change", () => {
        entry.option.selected = checkbox.checked;
        select.dispatchEvent(new Event("change", {bubbles: true}));
      });
      const text = document.createElement("span");
      const label = document.createElement("span");
      label.className = "d-block";
      label.textContent = entry.label;
      const detail = document.createElement("span");
      detail.className = "d-block text-body-secondary small";
      detail.textContent = entry.detail;
      text.append(label, detail);
      row.append(checkbox, text);
      list.append(row);
    }
    if (!visible.length) {
      const empty = document.createElement("div");
      empty.className = "p-2 text-body-secondary";
      empty.textContent = entries.length ? "No matching options." : `No ${plural} available.`;
      list.append(empty);
    }
    if (focusedIndex !== undefined) {
      const option = list.querySelector(`[data-option-index="${focusedIndex}"]`);
      (option || onlySelected || search).focus();
    }
    list.scrollTop = scrollTop;
    pages.classList.toggle("d-none", matches.length <= pageSize);
    previous.disabled = page === 0;
    next.disabled = start + pageSize >= matches.length;
    if (selectResults) selectResults.disabled = matches.length === 0;
    find("results").textContent = matches.length > pageSize ?
      `${start + 1}-${start + visible.length} of ${matches.length} results` :
      `${matches.length} results`;
    dropdown.update();
  }

  function updateSelection() {
    const chosen = entries.filter(entry => entry.option.selected);
    if (!select.multiple) {
      find("summary").textContent = chosen[0]?.label || "Select an option";
      if (panel.classList.contains("show")) renderOptions();
      return;
    }
    find("summary").textContent = chosen.length ?
      `${chosen.length} ${plural} selected` : `All matching ${plural}`;
    find("status").textContent = !entries.length ? `No ${plural} available.` :
      chosen.length ? `${chosen.length} of ${entries.length} selected` :
        `No restriction: all ${entries.length} ${plural} included`;
    chips.replaceChildren();
    for (const entry of chosen.slice(0, 2)) {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "profiler-select-chip btn btn-outline-secondary btn-sm";
      chip.textContent = `${entry.summary} \u00d7`;
      chip.setAttribute("aria-label", `Remove ${entry.label} / ${entry.detail}`);
      chip.addEventListener("click", () => {
        entry.option.selected = false;
        select.dispatchEvent(new Event("change", {bubbles: true}));
        toggle.focus();
      });
      chips.append(chip);
    }
    if (chosen.length > 2) {
      const more = document.createElement("button");
      more.type = "button";
      more.className = "btn btn-outline-secondary btn-sm";
      more.textContent = `+${chosen.length - 2} more`;
      more.setAttribute("aria-label", `Show all ${chosen.length} selected ${plural}`);
      more.addEventListener("click", event => {
        // This button is outside the menu; do not immediately auto-close the dropdown.
        event.stopPropagation();
        search.value = "";
        onlySelected.checked = true;
        page = 0;
        renderOptions();
        dropdown.show();
        search.focus();
      });
      chips.append(more);
    }
    if (panel.classList.contains("show")) {
      renderOptions();
    }
  }

  toggle.addEventListener("show.bs.dropdown", renderOptions);
  toggle.addEventListener("shown.bs.dropdown", () => search.focus());
  search.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
    }
  });
  const filterOptions = () => {
    page = 0;
    list.scrollTop = 0;
    renderOptions();
  };
  search.addEventListener("input", filterOptions);
  onlySelected?.addEventListener("change", filterOptions);
  previous.addEventListener("click", () => {
    page--;
    list.scrollTop = 0;
    renderOptions();
    if (previous.disabled) next.focus();
  });
  next.addEventListener("click", () => {
    page++;
    list.scrollTop = 0;
    renderOptions();
    if (next.disabled) previous.focus();
  });
  selectResults?.addEventListener("click", () => {
    matches.forEach(entry => { entry.option.selected = true; });
    select.dispatchEvent(new Event("change", {bubbles: true}));
  });
  find("all")?.addEventListener("click", () => {
    entries.forEach(entry => { entry.option.selected = false; });
    onlySelected.checked = false;
    select.dispatchEvent(new Event("change", {bubbles: true}));
  });
  select.addEventListener("change", updateSelection);
  updateSelection();
  select.hidden = true;
  container.querySelector(`label[for="${select.id}"]`).htmlFor = toggle.id;
  find("controls").classList.remove("d-none");
}

// The backend returns data only; all controls and labels are rendered here.
function createSelect(name, label, plural, options, selected, multiple = false) {
  const container = document.createElement("div");
  container.className = "col-md-3 mb-3 profiler-select";
  container.dataset.plural = plural;
  // This template is static. API values are inserted using textContent and option properties.
  container.innerHTML = `
    <div class="d-flex justify-content-between gap-2 mb-2">
      <label class="form-label mb-0"></label>
      <span class="profiler-select-count text-body-secondary"></span>
    </div>
    <select class="form-select"></select>
    <div class="profiler-select-controls d-none">
      <div class="dropdown">
        <button type="button" class="form-select text-start" data-bs-toggle="dropdown"
            data-bs-auto-close="outside" aria-expanded="false">
          <span class="profiler-select-summary"></span>
        </button>
        <div class="profiler-select-panel dropdown-menu w-100 shadow p-3" role="group">
          <input type="search" class="form-control profiler-select-search mb-2">
          <div class="profiler-multiple d-flex justify-content-between flex-wrap gap-2 mb-2">
            <button type="button" class="btn btn-link p-0 profiler-select-select">
              Select results
            </button>
            <button type="button" class="btn btn-link p-0 profiler-select-all"></button>
          </div>
          <div class="profiler-select-options"></div>
          <div class="profiler-select-pages d-flex justify-content-between mt-2">
            <button type="button" class="btn btn-outline-secondary btn-sm"
                data-direction="previous">Previous</button>
            <button type="button" class="btn btn-outline-secondary btn-sm"
                data-direction="next">Next</button>
          </div>
          <div class="d-flex justify-content-between flex-wrap gap-2 border-top pt-2 mt-2">
            <span class="profiler-select-results text-body-secondary" aria-live="polite"></span>
            <label class="profiler-multiple form-check mb-0">
              <input type="checkbox" class="form-check-input profiler-select-only">
              <span class="form-check-label">Selected only</span>
            </label>
          </div>
        </div>
      </div>
      <div class="profiler-multiple profiler-select-chips d-flex flex-wrap gap-2 mt-2"></div>
    </div>
    <div class="profiler-multiple profiler-select-status text-body-secondary mt-2"
        aria-live="polite"></div>`;
  const id = `profiler-${name}`;
  const labelNode = container.querySelector("label");
  labelNode.id = `${id}-label`;
  labelNode.htmlFor = id;
  labelNode.textContent = label;
  const select = container.querySelector("select");
  select.id = id;
  select.name = name;
  select.multiple = multiple;
  if (multiple) select.size = 4;
  for (const entry of options) {
    const option = new Option(entry.detail ? `${entry.label} / ${entry.detail}` : entry.label,
      entry.value, false, selected.includes(entry.value));
    option.dataset.label = entry.label;
    option.dataset.detail = entry.detail || "";
    option.dataset.summary = entry.summary || "";
    select.append(option);
  }
  const toggle = container.querySelector("button[aria-expanded]");
  toggle.id = `${id}-toggle`;
  toggle.setAttribute("aria-controls", `${id}-panel`);
  toggle.setAttribute("aria-labelledby", `${id}-label ${id}-summary`);
  const panel = container.querySelector(".profiler-select-panel");
  panel.id = `${id}-panel`;
  panel.setAttribute("aria-labelledby", `${id}-label`);
  container.querySelector(".profiler-select-summary").id = `${id}-summary`;
  const search = container.querySelector("input[type=search]");
  search.setAttribute("aria-label", `Search ${plural}`);
  search.placeholder = `Search ${plural} by name or ID`;
  container.querySelector(".profiler-select-count").textContent =
    multiple ? `${options.length} available` : "";
  container.querySelector(".profiler-select-all").textContent = `Use all ${plural}`;
  if (!multiple) container.querySelectorAll(".profiler-multiple").forEach(node => node.remove());
  return container;
}

const children = {
  executor: ["thread", "startTime", "endTime"],
  sqlExecution: ["job", "stage", "task"],
  job: ["stage", "task"],
  stage: ["task"]
};

export function initializeProfiler(root) {
  const find = id => root.querySelector(`#profiler-${id}`);
  const form = find("filters");
  const submit = form.querySelector("button[type=submit]");
  const error = find("error");
  const loading = find("loading");
  let controller;
  let generation = 0;
  let chart;
  let filtersReady = false;
  const pendingFields = new Set();
  const timeInputs = [find("start-time"), find("end-time")];
  const sampleInputs = find("sample-type").querySelectorAll("input[name=sampleType]");

  function formParams() {
    const params = new URLSearchParams(new FormData(form));
    for (const input of timeInputs) {
      if (!input.disabled && input.value) {
        params.set(input.name, new Date(input.value).getTime().toString());
      }
    }
    return params;
  }

  function validateTimeRange() {
    const [start, end] = timeInputs;
    const reversed = start.value && end.value &&
      new Date(start.value).getTime() >= new Date(end.value).getTime();
    end.setCustomValidity(reversed ? "End time must be after start time." : "");
  }

  function clearGraph() {
    chart = undefined;
    find("graph").classList.add("d-none");
    find("chart").replaceChildren();
    find("details").textContent = "";
  }

  function cancelRequest() {
    controller?.abort();
    generation++;
    loading.textContent = "";
    submit.disabled = !filtersReady;
  }

  async function request(endpoint, params, message, apply) {
    cancelRequest();
    const current = generation;
    controller = new AbortController();
    error.classList.add("d-none");
    loading.textContent = message;
    submit.disabled = true;
    clearGraph();
    try {
      const response = await fetch(`${root.dataset.apiBase}${endpoint}?${params}`, {
        signal: controller.signal,
        headers: {Accept: "application/json"}
      });
      const json = response.headers.get("content-type")?.includes("application/json");
      const data = json ? await response.json() : null;
      if (!response.ok) {
        throw new Error(data?.error || `Unable to load profiler data (HTTP ${response.status}).`);
      }
      if (!json) throw new Error("The profiler API returned an unexpected response.");
      if (current !== generation) return false;
      apply(data);
      return true;
    } catch (failure) {
      if (current === generation && failure.name !== "AbortError") {
        error.textContent = failure.message;
        error.classList.remove("d-none");
      }
      return false;
    } finally {
      if (current === generation) {
        loading.textContent = "";
        submit.disabled = !filtersReady;
      }
    }
  }

  function disposeSelect(container) {
    const toggle = container.querySelector("button[aria-expanded]");
    if (toggle) bootstrap.Dropdown.getInstance(toggle)?.dispose();
  }

  function renderFilters(data, params, fields) {
    if (!fields) {
      form.querySelectorAll(".profiler-select").forEach(disposeSelect);
    }
    const mode = params.get("mode") || "executor";
    const sampleType = params.get("sampleType") || "wall";
    if (!fields || fields.has("startTime") || fields.has("endTime")) {
      find("time-help").classList.toggle("d-none", mode !== "executor");
      for (const input of timeInputs) {
        input.parentElement.classList.toggle("d-none", mode !== "executor");
        input.disabled = mode !== "executor";
        const value = params.get(input.name);
        const date = value !== undefined && value !== null && value !== "" ?
          new Date(Number(value)) : null;
        input.value = date && Number.isFinite(date.getTime()) ?
          new Date(date.getTime() - date.getTimezoneOffset() * 60000)
            .toISOString().slice(0, 23) : "";
      }
      validateTimeRange();
      const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
      find("time-help").textContent = `Times use your browser's local timezone (${timezone}). ` +
        "Leave either endpoint empty for no limit.";
    }
    form.elements.namedItem("mode").value = mode;
    root.querySelectorAll("a[data-mode]").forEach(link => {
      const active = link.dataset.mode === mode;
      link.classList.toggle("active", active);
      link.classList.toggle("text-primary", active);
      link.classList.toggle("text-body-secondary", !active);
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    const option = (value, label = value) => ({value, label});
    if (!fields) {
      sampleInputs.forEach(input => {
        input.checked = input.value === sampleType;
        input.disabled = false;
      });
    }
    const options = find("options");
    if (!fields) options.querySelectorAll(".profiler-select").forEach(node => node.remove());
    function add(name, label, entries, multiple = false, plural = label.toLowerCase()) {
      if (fields && !fields.has(name)) return;
      const selected = params.getAll(name);
      if (name === "executor" && !selected.some(Boolean)) selected.push("driver");
      const container = createSelect(name, label, plural, entries, selected, multiple);
      const previous = form.elements.namedItem(name)?.closest(".profiler-select");
      if (previous) {
        disposeSelect(previous);
        previous.replaceWith(container);
      } else {
        options.insertBefore(container, timeInputs[0].parentElement);
      }
      initializeSelect(container);
    }
    const all = option("", "All");
    if (mode === "executor") {
      add("executor", "Executor", data.executors.map(id => option(id)));
      add("thread", "Thread", data.threads.map(t => ({
        value: t.id, label: `Java thread ${t.javaThreadId}`, detail: `Executor ${t.executorId}`,
        summary: `${t.executorId}: Java thread ${t.javaThreadId}`
      })), true, "threads");
      find("hierarchy").textContent = "Executor (including driver) \u2192 Thread";
      find("help").textContent = "Thread options come from completed task mappings on the selected executor. " +
        "Leave threads unselected to include all matching threads. ";
    } else {
      add("sqlExecution", "SQL execution", [all, ...data.sqlExecutions.map(id => option(id, `SQL ${id}`))]);
      add("job", "Job", [all, ...data.jobs.map(id => option(id, `Job ${id}`))]);
      add("stage", "Stage attempt", [all, ...data.stages.map(s =>
        option(s.id, `Stage ${s.stageId} (attempt ${s.attempt})`))]);
      add("task", "Task attempt", data.tasks.map(t => ({
        value: t.id, label: `Task ${t.taskId} (attempt ${t.attempt})`,
        detail: `Partition ${t.partitionId}`,
        summary: `Task ${t.taskId} (${t.attempt})`
      })), true, "task attempts");
      find("hierarchy").textContent =
        "SQL execution \u2192 Job \u2192 Stage attempt \u2192 Task attempt";
      find("help").textContent = "Select a stage attempt to load task attempts. " +
        "A selected parent limits the options below it. " +
        "Leave a level unselected to include all matching values. ";
    }
    find("help").append("Selected threads or tasks contribute samples to one flame graph.");
    const empty = find("empty");
    empty.textContent = mode === "task" && !data.hasTaskMappings ?
      "No task thread mappings were recorded. Enable spark.eventLog.enabled and " +
        "spark.profiler.executor.taskThreadEvent.enabled when running the application." : "";
    empty.classList.toggle("d-none", !empty.textContent);
    filtersReady = true;
  }

  function renderGraph(data) {
    const container = find("chart");
    find("graph").classList.remove("d-none");
    find("reset-zoom").disabled = data.value === 0;
    if (data.value === 0) {
      container.textContent = "No samples match these filters.";
      return;
    }
    chart = flamegraph()
      .width(container.clientWidth)
      .cellHeight(18)
      .inverted(true)
      .sort(true)
      .setLabelHandler(node => {
        const label = document.createElement("span");
        label.textContent = `${node.data.name} (${node.data.value} samples)`;
        return label.innerHTML;
      })
      .setDetailsHandler(text => { find("details").textContent = text || ""; });
    d3.select(container).datum(data).call(chart);
  }

  function loadGraph(params) {
    return request("flamegraph", params, "Loading flame graph...", renderGraph);
  }

  async function loadFilters(params, descendants) {
    if (descendants) {
      descendants.forEach(name => pendingFields.add(name));
      for (const name of pendingFields) {
        const control = form.elements.namedItem(name);
        if (timeInputs.includes(control)) {
          control.disabled = true;
          continue;
        }
        const container = control?.closest(".profiler-select");
        if (!container) continue;
        bootstrap.Dropdown.getInstance(container.querySelector("button[aria-expanded]"))?.hide();
        container.querySelectorAll("select, button, input").forEach(control => {
          control.disabled = true;
        });
      }
    } else {
      pendingFields.clear();
      sampleInputs.forEach(input => { input.disabled = true; });
    }
    const fields = descendants ? new Set(pendingFields) : null;
    filtersReady = false;
    const loaded = await request("filters", params, "Loading filters...", data => {
      renderFilters(data, params, fields);
      pendingFields.clear();
    });
    if (loaded && params.get("view") === "flamegraph") {
      await loadGraph(formParams());
    }
  }

  find("sample-type").addEventListener("change", () => {
    if (filtersReady) cancelRequest();
    clearGraph();
  });
  form.addEventListener("change", event => {
    if (event.target.tagName !== "SELECT" || event.target.disabled) return;
    if (!filtersReady && !children[event.target.name]) return;
    const params = formParams();
    const descendants = children[event.target.name];
    if (descendants) {
      // Time bounds only affect graph samples, not the available filter options.
      for (const input of timeInputs) params.delete(input.name);
      for (const name of descendants) params.delete(name);
      loadFilters(params, descendants);
    } else {
      cancelRequest();
      clearGraph();
    }
  });
  for (const input of timeInputs) {
    input.addEventListener("input", () => {
      validateTimeRange();
      if (filtersReady) cancelRequest();
      clearGraph();
    });
  }
  form.addEventListener("submit", event => {
    event.preventDefault();
    if (!filtersReady || submit.disabled) return;
    if (!form.reportValidity()) return;
    const params = formParams();
    loadGraph(params);
  });
  root.querySelectorAll("a[data-mode], #profiler-reset").forEach(link => {
    link.addEventListener("click", event => {
      if (event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      const params = new URLSearchParams();
      if (link.dataset.mode) {
        params.set("mode", link.dataset.mode);
        params.set("sampleType", form.elements.namedItem("sampleType")?.value || "wall");
      }
      loadFilters(params);
    });
  });
  find("reset-zoom").addEventListener("click", () => chart?.resetZoom());
  window.addEventListener("resize", () => {
    if (chart) chart.width(find("chart").clientWidth).update();
  });
  return loadFilters(new URLSearchParams(window.location.search));
}

const profiler = document.getElementById("profiler");
if (profiler) initializeProfiler(profiler);

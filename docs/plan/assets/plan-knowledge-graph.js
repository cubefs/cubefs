(() => {
  const summary = window.__UA_GRAPH_SUMMARY__;
  if (!summary) {
    return;
  }

  const palette = [
    "#0f766e",
    "#c97b2d",
    "#2b6cb0",
    "#8b5cf6",
    "#b45309",
    "#047857",
    "#334155",
    "#7c3aed",
    "#be123c",
  ];

  const layerColorMap = new Map(
    summary.layers.map((layer, index) => [layer.id, palette[index % palette.length]]),
  );
  const moduleMap = new Map(summary.modules.map((module) => [module.module, module]));
  const neighborMap = new Map();

  for (const module of summary.modules) {
    neighborMap.set(module.module, new Set());
  }
  for (const edge of summary.moduleGraph.edges) {
    neighborMap.get(edge.source)?.add(edge.target);
    neighborMap.get(edge.target)?.add(edge.source);
  }

  const state = {
    query: "",
    activeModule: summary.modules[0]?.module || null,
  };

  const formatNumber = (value) => new Intl.NumberFormat("zh-CN").format(value);
  const formatDate = (value) => {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN", { hour12: false });
  };
  const escapeHtml = (value) =>
    String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");

  const matchesModule = (module, query) => {
    if (!query) return true;
    const haystack = [
      module.module,
      module.label,
      module.summary,
      module.primaryLayerName,
      ...module.topDependencies.map((item) => item.label),
      ...module.topDependents.map((item) => item.label),
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(query);
  };

  const setText = (selector, value) => {
    const node = document.querySelector(selector);
    if (node) node.textContent = value;
  };

  const renderHero = () => {
    setText('[data-stat="nodes"]', formatNumber(summary.totals.nodes));
    setText('[data-stat="edges"]', formatNumber(summary.totals.edges));
    setText('[data-stat="modules"]', `${formatNumber(summary.totals.modules)} / ${formatNumber(summary.totals.files)} 文件`);
    setText('[data-stat="layers"]', `${summary.totals.layers} / ${summary.totals.tours}`);
    setText("[data-git-hash]", summary.meta.gitCommitHash.slice(0, 12));
    setText("[data-generated-at]", formatDate(summary.meta.lastAnalyzedAt));
    setText("[data-project-description]", summary.project.description);

    const projectMeta = document.querySelector("[data-project-meta]");
    if (projectMeta) {
      projectMeta.innerHTML = [
        ...summary.project.languages.map((language) => `<span class="metric-pill">${escapeHtml(language)}</span>`),
        ...summary.project.frameworks.map((framework) => `<span class="metric-pill warm">${escapeHtml(framework)}</span>`),
      ].join("");
    }

    const nodeTypes = document.querySelector("[data-node-types]");
    if (nodeTypes) {
      nodeTypes.innerHTML = summary.nodeTypes
        .slice(0, 8)
        .map(
          (item) =>
            `<span class="metric-pill">${escapeHtml(item.label)} · ${formatNumber(item.count)}</span>`,
        )
        .join("");
    }
  };

  const renderLegend = () => {
    const legend = document.querySelector("[data-layer-legend]");
    if (!legend) return;
    legend.innerHTML = summary.layers
      .map(
        (layer) => `
          <span class="legend-chip">
            <span class="legend-color" style="background:${layerColorMap.get(layer.id)}"></span>
            ${escapeHtml(layer.name)}
          </span>
        `,
      )
      .join("");
  };

  const ensureActiveModuleVisible = () => {
    const visibleModules = summary.modules.filter((module) => matchesModule(module, state.query));
    if (visibleModules.length === 0) {
      state.activeModule = null;
      return;
    }
    if (!state.activeModule || !visibleModules.some((module) => module.module === state.activeModule)) {
      state.activeModule = visibleModules[0].module;
    }
  };

  const setActiveModule = (moduleName) => {
    state.activeModule = moduleName;
    renderInspector();
    renderModuleCards();
    renderGraph();
  };

  const renderInspectorList = (selector, items, formatter) => {
    const list = document.querySelector(selector);
    if (!list) return;
    if (!items.length) {
      list.innerHTML = '<li class="muted-item">暂无明显热点。</li>';
      return;
    }
    list.innerHTML = items.map(formatter).join("");
  };

  const renderInspector = () => {
    const module = state.activeModule ? moduleMap.get(state.activeModule) : null;
    if (!module) {
      setText("[data-active-module-title]", "没有匹配的模块");
      setText("[data-active-module-summary]", "请调整搜索条件。");
      const metrics = document.querySelector("[data-active-module-metrics]");
      if (metrics) metrics.innerHTML = "";
      renderInspectorList("[data-active-module-dependencies]", [], () => "");
      renderInspectorList("[data-active-module-dependents]", [], () => "");
      renderInspectorList("[data-active-module-files]", [], () => "");
      return;
    }

    setText("[data-active-module-title]", `${module.label} · ${module.primaryLayerName}`);
    setText("[data-active-module-summary]", module.summary);

    const metrics = document.querySelector("[data-active-module-metrics]");
    if (metrics) {
      metrics.innerHTML = [
        ["节点", module.nodeCount],
        ["文件", module.fileCount],
        ["函数", module.functionCount],
        ["类型", module.classCount],
        ["入向 imports", module.fileImportInbound],
        ["出向 imports", module.fileImportOutbound],
      ]
        .map(
          ([label, value]) =>
            `<span class="metric-pill"><strong>${formatNumber(value)}</strong>${escapeHtml(label)}</span>`,
        )
        .join("");
    }

    renderInspectorList(
      "[data-active-module-dependencies]",
      module.topDependencies,
      (item) => `<li><span>${escapeHtml(item.label)}</span><strong>${formatNumber(item.count)}</strong></li>`,
    );
    renderInspectorList(
      "[data-active-module-dependents]",
      module.topDependents,
      (item) => `<li><span>${escapeHtml(item.label)}</span><strong>${formatNumber(item.count)}</strong></li>`,
    );
    renderInspectorList(
      "[data-active-module-files]",
      module.sampleFiles,
      (item) => `
        <li class="file-item">
          <div>
            <strong>${escapeHtml(item.name)}</strong>
            <span>${escapeHtml(item.path)}</span>
          </div>
          <em>${formatNumber(item.importsIn)} / ${formatNumber(item.importsOut)}</em>
        </li>
      `,
    );
  };

  const renderModuleCards = () => {
    const container = document.querySelector("[data-module-list]");
    const empty = document.querySelector("[data-module-empty]");
    if (!container || !empty) return;

    const modules = summary.modules.filter((module) => matchesModule(module, state.query));
    empty.classList.toggle("is-hidden", modules.length > 0);
    container.innerHTML = modules
      .map((module) => {
        const activeClass = module.module === state.activeModule ? " is-active" : "";
        return `
          <button class="module-card${activeClass}" type="button" data-module-card="${escapeHtml(module.module)}">
            <div class="module-card-top">
              <span class="kind-pill">${escapeHtml(module.primaryLayerName)}</span>
              <span class="path-pill">${escapeHtml(module.module)}</span>
            </div>
            <h3>${escapeHtml(module.label)}</h3>
            <p>${escapeHtml(module.summary)}</p>
            <div class="module-card-metrics">
              <span>节点 ${formatNumber(module.nodeCount)}</span>
              <span>文件 ${formatNumber(module.fileCount)}</span>
              <span>函数 ${formatNumber(module.functionCount)}</span>
              <span>类型 ${formatNumber(module.classCount)}</span>
            </div>
          </button>
        `;
      })
      .join("");

    container.querySelectorAll("[data-module-card]").forEach((button) => {
      button.addEventListener("click", () => {
        const moduleName = button.getAttribute("data-module-card");
        if (moduleName) setActiveModule(moduleName);
      });
    });
  };

  const renderLayers = () => {
    const container = document.querySelector("[data-layer-list]");
    if (!container) return;
    container.innerHTML = summary.layers
      .map(
        (layer) => `
          <article class="layer-card">
            <div class="layer-card-head">
              <span class="legend-color" style="background:${layerColorMap.get(layer.id)}"></span>
              <h3>${escapeHtml(layer.name)}</h3>
            </div>
            <p>${escapeHtml(layer.description)}</p>
            <div class="module-card-metrics">
              <span>节点 ${formatNumber(layer.nodeCount)}</span>
              <span>模块 ${formatNumber(layer.moduleCount)}</span>
              <span>主归属 ${formatNumber(layer.primaryModuleCount)}</span>
            </div>
            <div class="inline-list">
              ${layer.topModules
                .map((item) => `<span class="metric-pill">${escapeHtml(item.label)} · ${formatNumber(item.count)}</span>`)
                .join("")}
            </div>
          </article>
        `,
      )
      .join("");
  };

  const renderTours = () => {
    const container = document.querySelector("[data-tour-list]");
    if (!container) return;
    container.innerHTML = summary.tours
      .map(
        (tour) => `
          <article class="tour-card">
            <div class="tour-order">${tour.order}</div>
            <div>
              <h3>${escapeHtml(tour.title)}</h3>
              <p>${escapeHtml(tour.description)}</p>
              <p class="tour-note">${escapeHtml(tour.languageLesson || "无补充说明。")}</p>
            </div>
          </article>
        `,
      )
      .join("");
  };

  const renderTable = (selector, headers, rows) => {
    const table = document.querySelector(selector);
    if (!table) return;
    const head = `<thead><tr>${headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("")}</tr></thead>`;
    const body = `<tbody>${rows.join("")}</tbody>`;
    table.innerHTML = head + body;
  };

  const renderTables = () => {
    renderTable(
      '[data-table="imported"]',
      ["文件", "模块", "被 imports 次数"],
      summary.topImportedFiles.map(
        (item) => `
          <tr>
            <td><span class="table-primary">${escapeHtml(item.name)}</span><span class="table-secondary">${escapeHtml(item.path)}</span></td>
            <td>${escapeHtml(item.moduleLabel)}</td>
            <td>${formatNumber(item.count)}</td>
          </tr>
        `,
      ),
    );

    renderTable(
      '[data-table="importing"]',
      ["文件", "模块", "出向 imports 次数"],
      summary.topImportingFiles.map(
        (item) => `
          <tr>
            <td><span class="table-primary">${escapeHtml(item.name)}</span><span class="table-secondary">${escapeHtml(item.path)}</span></td>
            <td>${escapeHtml(item.moduleLabel)}</td>
            <td>${formatNumber(item.count)}</td>
          </tr>
        `,
      ),
    );

    renderTable(
      '[data-table="module-edges"]',
      ["源模块", "目标模块", "关系数"],
      summary.crossModuleEdges.map(
        (item) => `
          <tr>
            <td>${escapeHtml(item.sourceLabel)}</td>
            <td>${escapeHtml(item.targetLabel)}</td>
            <td>${formatNumber(item.count)}</td>
          </tr>
        `,
      ),
    );
  };

  const computeGraphLayout = () => {
    const width = 1280;
    const height = 760;
    const layers = summary.layers;
    const cols = Math.max(2, Math.ceil(Math.sqrt(layers.length)));
    const rows = Math.max(1, Math.ceil(layers.length / cols));
    const centers = new Map();

    layers.forEach((layer, index) => {
      const col = index % cols;
      const row = Math.floor(index / cols);
      centers.set(layer.id, {
        x: 200 + (col * (width - 400)) / Math.max(1, cols - 1),
        y: 180 + (row * (height - 280)) / Math.max(1, rows - 1),
      });
    });

    const nodes = summary.moduleGraph.nodes.map((node, index) => {
      const center = centers.get(node.primaryLayerId) || { x: width / 2, y: height / 2 };
      const angle = (index / Math.max(1, summary.moduleGraph.nodes.length)) * Math.PI * 2;
      return {
        ...node,
        x: center.x + Math.cos(angle) * 30,
        y: center.y + Math.sin(angle) * 30,
        vx: 0,
        vy: 0,
      };
    });
    const nodeMap = new Map(nodes.map((node) => [node.module, node]));

    for (let step = 0; step < 240; step += 1) {
      for (let i = 0; i < nodes.length; i += 1) {
        const a = nodes[i];
        for (let j = i + 1; j < nodes.length; j += 1) {
          const b = nodes[j];
          const dx = a.x - b.x;
          const dy = a.y - b.y;
          const distanceSq = Math.max(dx * dx + dy * dy, 36);
          const force = 18000 / distanceSq;
          const distance = Math.sqrt(distanceSq);
          const fx = (dx / distance) * force;
          const fy = (dy / distance) * force;
          a.vx += fx;
          a.vy += fy;
          b.vx -= fx;
          b.vy -= fy;
        }
      }

      for (const edge of summary.moduleGraph.edges) {
        const source = nodeMap.get(edge.source);
        const target = nodeMap.get(edge.target);
        if (!source || !target) continue;
        const dx = target.x - source.x;
        const dy = target.y - source.y;
        const distance = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
        const desired = 110 + Math.sqrt(edge.count) * 2.4;
        const force = (distance - desired) * 0.0016 * (0.7 + edge.weight);
        source.vx += dx * force;
        source.vy += dy * force;
        target.vx -= dx * force;
        target.vy -= dy * force;
      }

      for (const node of nodes) {
        const center = centers.get(node.primaryLayerId) || { x: width / 2, y: height / 2 };
        node.vx += (center.x - node.x) * 0.004;
        node.vy += (center.y - node.y) * 0.004;
        node.vx *= 0.84;
        node.vy *= 0.84;
        node.x = Math.min(width - 70, Math.max(70, node.x + node.vx));
        node.y = Math.min(height - 60, Math.max(60, node.y + node.vy));
      }
    }

    return { width, height, nodes, nodeMap, centers };
  };

  const graphLayout = computeGraphLayout();

  const renderGraph = () => {
    const svg = document.querySelector("[data-module-graph]");
    const empty = document.querySelector("[data-graph-empty]");
    if (!svg || !empty) return;

    const visibleModules = new Set(
      summary.modules.filter((module) => matchesModule(module, state.query)).map((module) => module.module),
    );
    empty.classList.toggle("is-hidden", visibleModules.size > 0);

    const isConnectedToActive = (moduleName) => {
      if (!state.activeModule) return false;
      return neighborMap.get(state.activeModule)?.has(moduleName) || false;
    };

    const edgeMarkup = summary.moduleGraph.edges
      .map((edge) => {
        const source = graphLayout.nodeMap.get(edge.source);
        const target = graphLayout.nodeMap.get(edge.target);
        if (!source || !target) return "";
        const hidden = visibleModules.size > 0 && (!visibleModules.has(edge.source) && !visibleModules.has(edge.target));
        const active =
          state.activeModule &&
          (edge.source === state.activeModule || edge.target === state.activeModule);
        const dimmed =
          !active &&
          visibleModules.size > 0 &&
          !(visibleModules.has(edge.source) && visibleModules.has(edge.target));
        return `
          <line
            class="graph-edge${active ? " is-active" : ""}${dimmed || hidden ? " is-dimmed" : ""}"
            x1="${source.x.toFixed(2)}"
            y1="${source.y.toFixed(2)}"
            x2="${target.x.toFixed(2)}"
            y2="${target.y.toFixed(2)}"
            stroke-width="${(1.2 + edge.weight * 3).toFixed(2)}"
          />
        `;
      })
      .join("");

    const nodeMarkup = graphLayout.nodes
      .map((node) => {
        const radius = 14 + Math.sqrt(node.fileCount);
        const matches = matchesModule(moduleMap.get(node.module), state.query);
        const active = node.module === state.activeModule;
        const connected = isConnectedToActive(node.module);
        const dimmed = state.query ? !matches : state.activeModule ? !active && !connected : false;
        return `
          <g
            class="graph-node${active ? " is-active" : ""}${dimmed ? " is-dimmed" : ""}"
            data-graph-node="${escapeHtml(node.module)}"
            transform="translate(${node.x.toFixed(2)} ${node.y.toFixed(2)})"
          >
            <circle r="${radius.toFixed(2)}" fill="${layerColorMap.get(node.primaryLayerId) || "#0f766e"}"></circle>
            <text y="${(radius + 22).toFixed(2)}">${escapeHtml(node.label)}</text>
          </g>
        `;
      })
      .join("");

    svg.innerHTML = `
      <g class="graph-layer-guides">
        ${summary.layers
          .map((layer) => {
            const center = graphLayout.centers.get(layer.id);
            if (!center) return "";
            return `
              <text
                class="graph-layer-label"
                x="${center.x.toFixed(2)}"
                y="${(center.y - 84).toFixed(2)}"
              >${escapeHtml(layer.name)}</text>
            `;
          })
          .join("")}
      </g>
      <g class="graph-edges">${edgeMarkup}</g>
      <g class="graph-nodes">${nodeMarkup}</g>
    `;

    svg.querySelectorAll("[data-graph-node]").forEach((node) => {
      node.addEventListener("click", () => {
        const moduleName = node.getAttribute("data-graph-node");
        if (moduleName) setActiveModule(moduleName);
      });
    });
  };

  const searchInput = document.querySelector("[data-module-search]");
  if (searchInput instanceof HTMLInputElement) {
    searchInput.addEventListener("input", () => {
      state.query = searchInput.value.trim().toLowerCase();
      ensureActiveModuleVisible();
      renderInspector();
      renderModuleCards();
      renderGraph();
    });
  }

  renderHero();
  renderLegend();
  renderLayers();
  renderTours();
  renderTables();
  ensureActiveModuleVisible();
  renderInspector();
  renderModuleCards();
  renderGraph();
})();

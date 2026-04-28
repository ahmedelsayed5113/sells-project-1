/* ════════════════════════════════════════════════════════════════════
   Plotly chart helpers — Ain Real Estate flat design system.
   Periwinkle primary, Teal accent, Coral support.
   Solid backgrounds, neutral grids, straight lines by default.
   ════════════════════════════════════════════════════════════════════ */

const CHART_COLORS = {
  brand:       '#474dc5',  // periwinkle primary
  brand2:      '#6067df',  // periwinkle lighter
  brand3:      '#bfc2ff',  // periwinkle dim
  accent:      '#006762',  // teal tertiary
  accent2:     '#00837c',
  accent3:     '#40dcd1',  // mint highlight
  secondary:   '#884f41',  // coral / brown
  secondary2:  '#ffb4a2',  // soft coral
  info:        '#5a7cff',
  warning:     '#c47200',
  warning2:    '#ffb84d',
  danger:      '#ba1a1a',
  danger2:     '#ff6b8a',
  muted:       '#767685',
  text:        '#181b24',
  textMuted:   '#464653',
  bg:          '#ffffff',
  surface2:    '#f5f6fc',
  border:      '#e5e7ef',
  grid:        '#eef0f6',
  gridStrong:  '#d8dbe8',
};

// Honored across all chart fns. Read once at module load.
const _PREFERS_REDUCED_MOTION = !!(window.matchMedia &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches);

function _isRTL() { return document.documentElement.dir === 'rtl'; }
function _chartLocale() {
  return (typeof getLang === 'function' && getLang() === 'ar') ? 'ar' : 'en';
}
function _chartFontFamily() {
  return _isRTL()
    ? 'IBM Plex Sans Arabic, system-ui, sans-serif'
    : 'Inter, system-ui, sans-serif';
}

// Wait for Plotly to load (up to 5 seconds), then call the drawer function.
function _waitForPlotly(fn, attempt = 0) {
  if (typeof Plotly !== 'undefined') { fn(); return; }
  if (attempt >= 50) { console.warn('Plotly CDN failed to load'); return; }
  setTimeout(() => _waitForPlotly(fn, attempt + 1), 100);
}

// Sequential palette: periwinkle → teal → coral → yellow → blue → mint
const PALETTE = [
  CHART_COLORS.brand,
  CHART_COLORS.accent2,
  CHART_COLORS.secondary2,
  CHART_COLORS.warning2,
  CHART_COLORS.info,
  CHART_COLORS.accent3,
  CHART_COLORS.brand3,
  CHART_COLORS.danger2,
];

function chartLayout(opts = {}) {
  const fontFamily = _chartFontFamily();
  return {
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {
      family: fontFamily,
      color: CHART_COLORS.textMuted,
      size: 12,
    },
    margin: { t: 16, r: 24, b: 44, l: 56, ...opts.margin },
    xaxis: {
      gridcolor: CHART_COLORS.grid,
      linecolor: CHART_COLORS.grid,
      zerolinecolor: CHART_COLORS.gridStrong,
      zeroline: false,
      color: CHART_COLORS.textMuted,
      tickfont: { size: 11 },
      automargin: true,
      ...opts.xaxis,
    },
    yaxis: {
      gridcolor: CHART_COLORS.grid,
      linecolor: CHART_COLORS.grid,
      zerolinecolor: CHART_COLORS.gridStrong,
      zeroline: false,
      color: CHART_COLORS.textMuted,
      tickfont: { size: 11 },
      automargin: true,
      ...opts.yaxis,
    },
    showlegend: opts.showlegend !== false,
    legend: {
      font: { color: CHART_COLORS.textMuted, size: 11 },
      bgcolor: CHART_COLORS.bg,
      bordercolor: CHART_COLORS.border,
      borderwidth: 0,
      orientation: opts.legendOrientation || 'h',
      y: opts.legendY != null ? opts.legendY : -0.18,
      x: 0.5,
      xanchor: 'center',
      ...opts.legend,
    },
    hoverlabel: {
      bgcolor: CHART_COLORS.bg,
      bordercolor: CHART_COLORS.gridStrong,
      font: { color: CHART_COLORS.text, size: 12, family: fontFamily },
    },
    transition: { duration: _PREFERS_REDUCED_MOTION ? 0 : 300 },
    ...opts.layout,
  };
}

// Default chart config — show modebar on hover (export to PNG, autoscale).
// drawGauge overrides this since modebar makes no sense on a single indicator.
function _chartConfig(overrides = {}) {
  return {
    displayModeBar: 'hover',
    displaylogo: false,
    modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d'],
    responsive: true,
    locale: _chartLocale(),
    ...overrides,
  };
}

// Backwards-compat global — anything that imports chartConfig still gets the defaults.
const chartConfig = _chartConfig();

function drawBarChart(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'bar',
    x: data.x,
    y: data.y,
    marker: {
      color: data.colors || PALETTE[0],
      line: { width: 0 },
    },
    text: data.labels || data.y.map(v => typeof v === 'number' ? v.toFixed(1) : v),
    textposition: 'outside',
    textfont: { color: CHART_COLORS.text, size: 11, family: _chartFontFamily() },
    cliponaxis: false,
    hovertemplate: (options.hovertemplate || '<b>%{x}</b><br>%{y}<extra></extra>'),
  };

  const layout = chartLayout({
    xaxis: { tickangle: options.xangle || 0 },
    showlegend: false,
    bargap: 0.35,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

function drawHorizontalBar(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'bar',
    orientation: 'h',
    x: data.x,
    y: data.y,
    marker: {
      color: data.colors || PALETTE[0],
      line: { width: 0 },
    },
    text: data.labels || data.x.map(v => typeof v === 'number' ? v.toFixed(1) + '%' : v),
    textposition: 'outside',
    textfont: { color: CHART_COLORS.text, size: 11, family: _chartFontFamily() },
    cliponaxis: false,
    hovertemplate: '<b>%{y}</b><br>%{x}<extra></extra>',
  };

  const layout = chartLayout({
    showlegend: false,
    margin: { l: 130, r: 80, t: 24, b: 40 },
    bargap: 0.35,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

function drawDonut(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'pie',
    labels: data.labels,
    values: data.values,
    hole: 0.62,
    marker: {
      colors: data.colors || PALETTE,
      line: { color: '#ffffff', width: 3 },
    },
    textinfo: options.textinfo || 'label+percent',
    textfont: { size: 12, color: CHART_COLORS.text, family: _chartFontFamily() },
    insidetextorientation: 'horizontal',
    hovertemplate: '<b>%{label}</b><br>%{value} (%{percent})<extra></extra>',
    sort: false,
  };

  const layout = chartLayout({
    showlegend: options.showlegend !== false,
    legend: { orientation: 'h', y: -0.05, x: 0.5, xanchor: 'center' },
    margin: { t: 20, r: 20, b: 70, l: 20 },
    annotations: options.centerText ? [{
      text: options.centerText,
      showarrow: false,
      font: { size: 22, color: CHART_COLORS.text, family: _chartFontFamily(), weight: 700 },
    }] : [],
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

function drawLineChart(containerId, series, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  // Real-data trends should default to linear lines — splines invent values
  // between data points, which contradicts "trust through legibility".
  // Pass options.shape: 'spline' explicitly when smoothing is intentional.
  const lineShape = options.shape || 'linear';

  const traces = series.map((s, i) => {
    const color = s.color || PALETTE[i % PALETTE.length];
    const fillColor = hexToRgba(color, 0.14);
    return {
      type: 'scatter',
      mode: 'lines+markers',
      name: s.name,
      x: s.x,
      y: s.y,
      line: { color, width: 2.5, shape: lineShape, smoothing: lineShape === 'spline' ? 1.0 : undefined },
      marker: {
        size: 7,
        color: CHART_COLORS.bg,
        line: { color, width: 2 },
      },
      fill: s.fill ? 'tozeroy' : 'none',
      fillcolor: s.fill ? fillColor : undefined,
      hovertemplate: '<b>%{x}</b><br>' + s.name + ': %{y}<extra></extra>',
    };
  });

  const layout = chartLayout({
    legendOrientation: 'h',
    legendY: -0.15,
    ...options,
  });
  _waitForPlotly(() => Plotly.newPlot(el, traces, layout, _chartConfig()));
}

function drawAreaChart(containerId, series, options = {}) {
  // Same as line but always fills.
  return drawLineChart(containerId,
    series.map(s => ({ ...s, fill: true })),
    options);
}

function drawGauge(containerId, value, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const color = value >= 75 ? CHART_COLORS.accent2
              : value >= 55 ? CHART_COLORS.brand
              : value >= 40 ? CHART_COLORS.warning2
              : CHART_COLORS.danger;

  const trace = {
    type: 'indicator',
    mode: 'gauge+number',
    value: value,
    number: { suffix: '%', font: { color: CHART_COLORS.text, size: 36, family: _chartFontFamily() } },
    gauge: {
      axis: { range: [0, 100], tickcolor: CHART_COLORS.muted, tickfont: { size: 10 } },
      bar: { color: color, thickness: 0.78 },
      bgcolor: CHART_COLORS.surface2,
      borderwidth: 0,
      steps: [
        { range: [0, 25],   color: 'rgba(186, 26, 26, 0.10)' },
        { range: [25, 55],  color: 'rgba(255, 184, 77, 0.14)' },
        { range: [55, 75],  color: 'rgba(71, 77, 197, 0.10)' },
        { range: [75, 100], color: 'rgba(0, 131, 124, 0.14)' },
      ],
      threshold: {
        line: { color: CHART_COLORS.text, width: 3 },
        thickness: 0.85,
        value: options.target || 75,
      },
    },
  };

  const layout = chartLayout({
    margin: { t: 20, r: 20, b: 20, l: 20 },
    showlegend: false,
    ...options,
  });

  // Single-indicator gauges don't need the export modebar.
  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig({ displayModeBar: false })));
}

function drawRadarChart(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'scatterpolar',
    r: data.values,
    theta: data.labels,
    fill: 'toself',
    fillcolor: 'rgba(71, 77, 197, 0.18)',
    line: { color: CHART_COLORS.brand, width: 2.5, shape: 'spline', smoothing: 0.6 },
    marker: { size: 7, color: CHART_COLORS.bg, line: { color: CHART_COLORS.brand, width: 2 } },
    hovertemplate: '<b>%{theta}</b><br>%{r}%<extra></extra>',
  };

  const layout = {
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {
      family: _chartFontFamily(),
      color: CHART_COLORS.textMuted,
      size: 11,
    },
    margin: { t: 30, r: 60, b: 30, l: 60 },
    showlegend: false,
    polar: {
      bgcolor: 'transparent',
      radialaxis: {
        visible: true,
        range: [0, 100],
        gridcolor: CHART_COLORS.grid,
        linecolor: CHART_COLORS.grid,
        tickfont: { size: 10, color: CHART_COLORS.muted },
        showline: false,
      },
      angularaxis: {
        gridcolor: CHART_COLORS.grid,
        linecolor: CHART_COLORS.grid,
        tickfont: { size: 11, color: CHART_COLORS.text },
      },
    },
    transition: { duration: _PREFERS_REDUCED_MOTION ? 0 : 300 },
    ...options,
  };

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

function drawStackedBar(containerId, series, xLabels, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const traces = series.map((s, i) => ({
    type: 'bar',
    name: s.name,
    x: xLabels,
    y: s.values,
    marker: {
      color: s.color || PALETTE[i % PALETTE.length],
      line: { width: 0 },
    },
    hovertemplate: '<b>%{x}</b><br>' + s.name + ': %{y}<extra></extra>',
  }));

  const layout = chartLayout({
    barmode: options.barmode || 'stack',
    bargap: 0.32,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, traces, layout, _chartConfig()));
}

function drawGroupedBar(containerId, series, xLabels, options = {}) {
  return drawStackedBar(containerId, series, xLabels, { ...options, barmode: 'group' });
}

/**
 * Heatmap — for matrices like "user × KPI achievement %".
 */
function drawHeatmap(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'heatmap',
    z: data.z,
    x: data.x,
    y: data.y,
    // Sequential scale with stronger lightness drop at the low end so cold cells
    // visibly stand out at a glance — pastel-pink-vs-pastel-periwinkle wasn't enough.
    colorscale: [
      [0,    '#fbe4e6'],  // very light coral (worst)
      [0.30, '#fdebcb'],  // soft yellow
      [0.55, '#dfe3ff'],  // soft periwinkle
      [0.80, '#9aa1ee'],  // mid periwinkle
      [1,    '#474dc5'],  // primary periwinkle (best)
    ],
    colorbar: {
      thickness: 10,
      len: 0.8,
      tickfont: { size: 10, color: CHART_COLORS.muted },
      outlinewidth: 0,
    },
    hovertemplate: '<b>%{y}</b><br>%{x}: %{z}<extra></extra>',
    showscale: options.showscale !== false,
  };

  const layout = chartLayout({
    margin: { t: 20, r: 60, b: 80, l: 130 },
    xaxis: { tickangle: -30, gridcolor: 'transparent', showgrid: false },
    yaxis: { gridcolor: 'transparent', showgrid: false },
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

/**
 * Treemap — useful for breaking down revenue / lead allocation.
 */
function drawTreemap(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'treemap',
    labels: data.labels,
    parents: data.parents || data.labels.map(() => ''),
    values: data.values,
    branchvalues: 'total',
    marker: {
      colors: data.colors || PALETTE,
      line: { color: CHART_COLORS.bg, width: 2 },
    },
    textfont: { color: CHART_COLORS.bg, size: 13, family: _chartFontFamily() },
    textinfo: 'label+value+percent parent',
    hovertemplate: '<b>%{label}</b><br>%{value}<extra></extra>',
  };

  const layout = chartLayout({
    margin: { t: 10, r: 10, b: 10, l: 10 },
    showlegend: false,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

/**
 * Funnel — useful for sales pipeline (leads → meetings → reservations → deals).
 */
function drawFunnel(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'funnel',
    y: data.y,
    x: data.x,
    text: data.labels || data.x.map(v => typeof v === 'number' ? v.toLocaleString() : v),
    textposition: 'inside',
    textfont: { color: CHART_COLORS.bg, size: 13, family: _chartFontFamily() },
    marker: {
      color: data.colors || [
        CHART_COLORS.brand3,
        CHART_COLORS.brand,
        CHART_COLORS.accent2,
        CHART_COLORS.accent3,
        CHART_COLORS.warning2,
      ],
      line: { width: 0 },
    },
    connector: { line: { color: 'rgba(124,131,253,0.30)', width: 1 } },
    hovertemplate: '<b>%{y}</b><br>%{x}<extra></extra>',
  };

  const layout = chartLayout({
    margin: { l: 130, r: 30, t: 20, b: 20 },
    showlegend: false,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

/**
 * Scatter / bubble — e.g., user calls vs deals with bubble = revenue.
 */
function drawScatter(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const trace = {
    type: 'scatter',
    mode: 'markers',
    x: data.x,
    y: data.y,
    text: data.text,
    marker: {
      size: data.sizes || 14,
      sizemode: 'diameter',
      sizeref: data.sizeref || 1,
      color: data.colors || CHART_COLORS.brand,
      line: { color: CHART_COLORS.bg, width: 2 },
      opacity: 0.85,
    },
    hovertemplate: '<b>%{text}</b><br>%{xaxis.title.text}: %{x}<br>%{yaxis.title.text}: %{y}<extra></extra>',
  };

  const layout = chartLayout({
    showlegend: false,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, [trace], layout, _chartConfig()));
}

/**
 * Combo chart — bar + line on dual axes.
 */
function drawComboBarLine(containerId, barSeries, lineSeries, xLabels, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const traces = [
    {
      type: 'bar',
      name: barSeries.name,
      x: xLabels,
      y: barSeries.values,
      marker: { color: barSeries.color || CHART_COLORS.brand3, line: { width: 0 } },
      yaxis: 'y',
      hovertemplate: '<b>%{x}</b><br>' + barSeries.name + ': %{y}<extra></extra>',
    },
    {
      type: 'scatter',
      mode: 'lines+markers',
      name: lineSeries.name,
      x: xLabels,
      y: lineSeries.values,
      line: { color: lineSeries.color || CHART_COLORS.brand, width: 3, shape: 'spline' },
      marker: { size: 8, color: CHART_COLORS.bg, line: { color: lineSeries.color || CHART_COLORS.brand, width: 2 } },
      yaxis: 'y2',
      hovertemplate: '<b>%{x}</b><br>' + lineSeries.name + ': %{y}<extra></extra>',
    },
  ];

  const layout = chartLayout({
    yaxis: { title: barSeries.name, side: 'left', gridcolor: CHART_COLORS.grid },
    yaxis2: {
      title: lineSeries.name,
      side: 'right',
      overlaying: 'y',
      showgrid: false,
      tickfont: { size: 11, color: CHART_COLORS.textMuted },
    },
    bargap: 0.4,
    legendOrientation: 'h',
    legendY: -0.18,
    ...options,
  });

  _waitForPlotly(() => Plotly.newPlot(el, traces, layout, _chartConfig()));
}

function scoreColorHex(pct) {
  return pct >= 75 ? CHART_COLORS.accent2
       : pct >= 55 ? CHART_COLORS.brand
       : pct >= 40 ? CHART_COLORS.warning2
       : CHART_COLORS.danger;
}

function hexToRgba(hex, alpha = 1) {
  const m = /^#([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(hex);
  if (!m) return hex;
  const r = parseInt(m[1], 16);
  const g = parseInt(m[2], 16);
  const b = parseInt(m[3], 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

window.Charts = {
  drawBarChart,
  drawHorizontalBar,
  drawDonut,
  drawLineChart,
  drawAreaChart,
  drawGauge,
  drawRadarChart,
  drawStackedBar,
  drawGroupedBar,
  drawHeatmap,
  drawTreemap,
  drawFunnel,
  drawScatter,
  drawComboBarLine,
  scoreColorHex,
  hexToRgba,
  COLORS: CHART_COLORS,
  PALETTE,
};

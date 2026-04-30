/* ════════════════════════════════════════════════════════════════════
   Plotly chart helpers — Ain Real Estate flat design system.
   Periwinkle primary, Teal accent, Coral support.
   Solid backgrounds, neutral grids, straight lines by default.
   ════════════════════════════════════════════════════════════════════ */

// Light/dark palettes. `CHART_COLORS` is mutated in place on theme change
// (rather than reassigned) so existing references in this module still
// point at the live object — and so callers can keep using property
// access (CHART_COLORS.brand) without needing to re-read every render.
const _CHART_PALETTES = {
  // Light-mode chart palette — softened on purpose. The CSS design tokens
  // (--brand etc.) stay at full saturation for buttons, badges, and tiles
  // — those sit on small surfaces and benefit from contrast. Charts cover
  // large areas of saturated colour against white, which gets harsh fast,
  // so the chart-only versions are nudged to medium saturation / medium
  // lightness for comfortable long viewing.
  light: {
    brand:       '#5e64db',  // periwinkle, slightly softer than the CSS --brand
    brand2:      '#7d83e8',
    brand3:      '#bfc2ff',
    accent:      '#2eb5a8',  // medium teal — lighter than the deep #006762
    accent2:     '#4ec5b9',
    accent3:     '#7fdcd0',
    secondary:   '#c98c7d',  // warm coral, easier than the dark brown
    secondary2:  '#f1b4a4',
    info:        '#6b8fed',
    warning:     '#d68c2a',  // amber, less burnt than #c47200
    warning2:    '#f0b463',
    danger:      '#d65962',  // muted red, still clearly a warning
    danger2:     '#ec828a',
    muted:       '#8a8e9c',
    text:        '#2d3142',  // softened from pure black for axis labels
    textMuted:   '#5b6072',
    bg:          '#ffffff',
    surface2:    '#f7f8fc',
    border:      '#e5e7ef',
    grid:        '#eef0f6',
    gridStrong:  '#d8dbe8',
  },
  dark: {
    // Brightened brand hues so they remain legible against dark surfaces.
    brand:       '#7c83fd',
    brand2:      '#969cff',
    brand3:      '#5559c8',
    accent:      '#4ed6ce',
    accent2:     '#6ee0d8',
    accent3:     '#00837c',
    secondary:   '#d49887',
    secondary2:  '#f5b8a8',
    info:        '#8ba4ff',
    warning:     '#ffb84d',
    warning2:    '#ffc97a',
    danger:      '#ff6b8a',
    danger2:     '#ff8b9e',
    muted:       '#8388a0',
    text:        '#e8eaf2',
    textMuted:   '#b3b6c4',
    bg:          '#1c1f2b',          // matches --surface-solid
    surface2:    '#20232f',
    border:      '#2a2d3a',
    grid:        '#262934',
    gridStrong:  '#353948',
  },
};

const CHART_COLORS = Object.assign({}, _CHART_PALETTES.light);

function _currentTheme() {
  return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
}

function _applyChartPalette(theme) {
  const src = _CHART_PALETTES[theme] || _CHART_PALETTES.light;
  Object.keys(src).forEach(k => { CHART_COLORS[k] = src[k]; });
}
_applyChartPalette(_currentTheme());

// Per-container redraw closures. Every public draw function (drawBarChart
// / drawDonut / etc.) registers its argument list keyed by container id;
// on theme change we re-invoke each closure so the chart re-renders with
// the active palette. Means pages don't have to wire a per-page handler
// — the chart helpers handle their own theming end-to-end.
const _redrawCallbacks = new Map();
function _registerRedraw(id, fn) {
  if (id) _redrawCallbacks.set(id, fn);
}

window.addEventListener('themechange', () => {
  _applyChartPalette(_currentTheme());
  _rebuildPalette();

  // Two-layer redraw strategy:
  //
  // 1) Pages whose chart args are computed at draw-time (most charts —
  //    they read CHART_COLORS / PALETTE inside the drawer body, so the
  //    closure replay below catches the new palette).
  //
  // 2) Pages that pre-resolve PALETTE on the page side and pass an
  //    array of HEX strings via data.colors. Those strings are frozen
  //    in the captured closure args. For those, calling the page's
  //    onLangChange() re-runs its render(), which re-evaluates
  //    Charts.PALETTE[i] with the new live values.
  _redrawCallbacks.forEach((fn, id) => {
    const el = document.getElementById(id);
    if (!el || !el.isConnected) {
      _redrawCallbacks.delete(id);
      return;
    }
    try { fn(); } catch (e) { console.warn('chart redraw failed:', id, e); }
  });

  if (typeof window.onLangChange === 'function') {
    try { window.onLangChange(typeof getLang === 'function' ? getLang() : 'ar'); } catch (_) {}
  }
});

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

// Per-container draw tokens — guards against the race where a pending Plotly
// draw fires AFTER an empty-state innerHTML write (or after a newer draw call
// has been queued). Each draw call captures its token; if the current token
// has advanced by the time the deferred callback fires, the draw is skipped.
const _drawTokens = new Map();
function _bumpToken(id) {
  const next = (_drawTokens.get(id) || 0) + 1;
  _drawTokens.set(id, next);
  return next;
}

// Per-container ResizeObserver registry. Plotly's `responsive: true` only
// reacts to window resizes — when a chart's PARENT changes size (e.g. opening
// a <details> deep-dive panel, sidebar collapse, mobile drawer), Plotly stays
// at its old layout. A debounced ResizeObserver per container fixes that.
const _resizeObservers = new Map();   // id → ResizeObserver
const _resizeDebounce  = new Map();   // id → setTimeout handle

function _attachResizeObserver(el) {
  if (typeof ResizeObserver === 'undefined') return;
  if (!el || !el.id) return;
  // Already observing? Disconnect first so each fresh draw starts clean.
  const prev = _resizeObservers.get(el.id);
  if (prev) { try { prev.disconnect(); } catch (_) {} }
  const ro = new ResizeObserver(() => {
    const handle = _resizeDebounce.get(el.id);
    if (handle) clearTimeout(handle);
    _resizeDebounce.set(el.id, setTimeout(() => {
      _resizeDebounce.delete(el.id);
      // Element may have been detached (route change / template re-render).
      if (!el.isConnected) return;
      try {
        if (typeof Plotly !== 'undefined' && Plotly.Plots && Plotly.Plots.resize) {
          Plotly.Plots.resize(el);
        }
      } catch (_) {}
    }, 100));
  });
  try { ro.observe(el); } catch (_) { return; }
  _resizeObservers.set(el.id, ro);
}

function _detachResizeObserver(id) {
  const ro = _resizeObservers.get(id);
  if (ro) { try { ro.disconnect(); } catch (_) {} _resizeObservers.delete(id); }
  const handle = _resizeDebounce.get(id);
  if (handle) { clearTimeout(handle); _resizeDebounce.delete(id); }
}

// Single entry point for every chart mount. Bumps the container's token,
// waits for Plotly, then on resolution: re-checks the token, purges Plotly
// internal state on the element, wipes innerHTML (removes .chart-skel and
// any leftover empty-state), calls Plotly.newPlot, and attaches a debounced
// ResizeObserver so parent-size changes redraw at the correct dimensions.
function _drawChart(el, traces, layout, config) {
  if (!el || !el.id) return;
  const id = el.id;
  const myToken = _bumpToken(id);
  _waitForPlotly(() => {
    if (_drawTokens.get(id) !== myToken) return; // superseded by a newer call
    try { if (typeof Plotly !== 'undefined' && Plotly.purge) Plotly.purge(el); } catch (_) {}
    el.innerHTML = '';
    Plotly.newPlot(el, traces, layout, config);
    _attachResizeObserver(el);
  });
}

// Empty-state caller-facing helper: invalidate any pending draw + purge Plotly
// state so the caller can safely set its own innerHTML (e.g., empty-state).
// Without this, a deferred Plotly.newPlot fires after the empty-state write
// and renders the chart over the "No data" text.
function cancelPending(elOrId) {
  const el = typeof elOrId === 'string' ? document.getElementById(elOrId) : elOrId;
  if (!el || !el.id) return;
  _bumpToken(el.id);
  _detachResizeObserver(el.id);
  try { if (typeof Plotly !== 'undefined' && Plotly.purge) Plotly.purge(el); } catch (_) {}
}

// Sequential palette: periwinkle → teal → coral → yellow → blue → mint.
// Defined as a `let`-like mutable array so theme changes can rewrite the
// values in place — callers that captured a reference (Charts.PALETTE,
// PALETTE[2], etc.) keep pointing at the same array. _rebuildPalette()
// runs on every theme change to keep the entries in sync with CHART_COLORS.
const PALETTE = [];
function _rebuildPalette() {
  const next = [
    CHART_COLORS.brand,
    CHART_COLORS.accent2,
    CHART_COLORS.secondary2,
    CHART_COLORS.warning2,
    CHART_COLORS.info,
    CHART_COLORS.accent3,
    CHART_COLORS.brand3,
    CHART_COLORS.danger2,
  ];
  PALETTE.length = 0;
  next.forEach(c => PALETTE.push(c));
}
_rebuildPalette();

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

  _drawChart(el, [trace], layout, _chartConfig());
}

// Reused canvas for label width measurement — avoids creating a new context per call.
let _measureCanvas = null;
function _measureLabelWidth(labels, fontPx, fontFamily) {
  if (!labels || !labels.length) return 0;
  if (!_measureCanvas) _measureCanvas = document.createElement('canvas');
  const ctx = _measureCanvas.getContext('2d');
  ctx.font = `${fontPx}px ${fontFamily}`;
  let max = 0;
  for (const l of labels) {
    if (l == null) continue;
    const w = ctx.measureText(String(l)).width;
    if (w > max) max = w;
  }
  return max;
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

  // Pre-measure the longest y-label when requested so long names (e.g. full
  // Arabic names of sales reps) don't overflow into the bars. Cap at 300px.
  let leftMargin = 180;
  if (options.measureLabels === true) {
    const measured = _measureLabelWidth(data.y, 11, _chartFontFamily());
    if (measured > 0) {
      leftMargin = Math.min(300, Math.max(leftMargin, Math.ceil(measured) + 24));
    }
  }

  const layout = chartLayout({
    showlegend: false,
    margin: { l: leftMargin, r: 80, t: 24, b: 40 },
    bargap: 0.35,
    ...options,
  });

  _drawChart(el, [trace], layout, _chartConfig());
}

function drawDonut(containerId, data, options = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;

  // Single-category donut renders as a full ring; the legend is redundant
  // and overlaps the slice label. Drop it and shrink the bottom margin.
  const isSingle = !data.values || data.values.length <= 1;
  const showLegend = isSingle ? false : (options.showlegend !== false);

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
    showlegend: showLegend,
    legend: { orientation: 'h', y: -0.05, x: 0.5, xanchor: 'center' },
    margin: { t: 20, r: 20, b: showLegend ? 90 : 30, l: 20 },
    annotations: options.centerText ? [{
      text: options.centerText,
      showarrow: false,
      font: { size: 22, color: CHART_COLORS.text, family: _chartFontFamily(), weight: 700 },
    }] : [],
    ...options,
  });

  _drawChart(el, [trace], layout, _chartConfig());
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
  _drawChart(el, traces, layout, _chartConfig());
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

  // Scale number/tick fonts down on narrow viewports so the indicator fits
  // mobile screens without the value glyph overflowing the arc.
  const isNarrow = (typeof window !== 'undefined' && window.innerWidth < 600);
  const numSize = isNarrow ? 26 : 36;
  const tickSize = isNarrow ? 9 : 10;
  const margin = isNarrow ? { t: 14, r: 14, b: 14, l: 14 } : { t: 20, r: 20, b: 20, l: 20 };

  const trace = {
    type: 'indicator',
    mode: 'gauge+number',
    value: value,
    number: { suffix: '%', font: { color: CHART_COLORS.text, size: numSize, family: _chartFontFamily() } },
    gauge: {
      axis: { range: [0, 100], tickcolor: CHART_COLORS.muted, tickfont: { size: tickSize } },
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
    margin: margin,
    showlegend: false,
    ...options,
  });

  // Single-indicator gauges don't need the export modebar.
  _drawChart(el, [trace], layout, _chartConfig({ displayModeBar: false }));
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

  _drawChart(el, [trace], layout, _chartConfig());
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

  _drawChart(el, traces, layout, _chartConfig());
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

  _drawChart(el, [trace], layout, _chartConfig());
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

  _drawChart(el, [trace], layout, _chartConfig());
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

  _drawChart(el, [trace], layout, _chartConfig());
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

  _drawChart(el, [trace], layout, _chartConfig());
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

  _drawChart(el, traces, layout, _chartConfig());
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

// Wrap every public draw function so its call args are remembered against
// the container id. The themechange handler walks the registry and calls
// each closure to repaint with the active palette — pages don't need a
// per-template hook.
function _trackedDraw(fn) {
  return function trackedDraw(containerId, ...rest) {
    _registerRedraw(containerId, () => fn(containerId, ...rest));
    return fn(containerId, ...rest);
  };
}

window.Charts = {
  drawBarChart:     _trackedDraw(drawBarChart),
  drawHorizontalBar: _trackedDraw(drawHorizontalBar),
  drawDonut:        _trackedDraw(drawDonut),
  drawLineChart:    _trackedDraw(drawLineChart),
  drawAreaChart:    _trackedDraw(drawAreaChart),
  drawGauge:        _trackedDraw(drawGauge),
  drawRadarChart:   _trackedDraw(drawRadarChart),
  drawStackedBar:   _trackedDraw(drawStackedBar),
  drawGroupedBar:   _trackedDraw(drawGroupedBar),
  drawHeatmap:      _trackedDraw(drawHeatmap),
  drawTreemap:      _trackedDraw(drawTreemap),
  drawFunnel:       _trackedDraw(drawFunnel),
  drawScatter:      _trackedDraw(drawScatter),
  drawComboBarLine: _trackedDraw(drawComboBarLine),
  scoreColorHex,
  hexToRgba,
  cancelPending,
  COLORS: CHART_COLORS,
  PALETTE,
};

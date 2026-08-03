// cpausage dashboard — main logic. Uses helpers from helpers.js.
const rangeKey = 'cpa-usage-range-v1';
var fmt = new Intl.NumberFormat(typeof getFormatLocale === 'function' ? getFormatLocale() : 'zh-CN');
var _lastFmtLocale = 'zh-CN';
let summaryData = null;         // DashboardSummary from /dashboard-summary
let eventsData = null;          // EventsResult from /dashboard-events
let eventsDataUrl = '';
let modelPrices = {};
let manualModelPrices = {};
let selectedPriceReferenceModel = '';
let priceReferenceVisibleOptions = [];
let priceReferenceActiveIndex = -1;
let selectedApi = '';
let clientApiSort = 'requests';
let clientApiSelectMode = false;
let selectedClientApi = null;
let filteredSummaryData = null;
let filteredSummaryContext = '';
let filteredSummaryError = null;
let trendMetric = 'tokens'; // 'cost' | 'requests' | 'tokens' | 'rpm'
let pollTimer = null, pollFailures = 0;
let currentRange = '';
const eventsPageSize = 50;
const eventsLimit = eventsPageSize;
const apiDetailRecentPageSize = 50;
const apiDetailRecentLimit = apiDetailRecentPageSize;
const priceReferenceResultLimit = 100;
const visiblePollDelayMs = 30000;
const hiddenPollDelayMs = 300000;
const dashboardTimeZoneOffsetMs = 8 * 60 * 60 * 1000;
const tokenTrendVisibilityStorageKey = 'cpa-usage-token-trend-series-v1';
const tokenTrendSeriesCatalog = [
  { key: 'input', color: '#2563eb', labelKey: 'token_input' },
  { key: 'output', color: '#14b8a6', labelKey: 'token_output' },
  { key: 'cacheCreation', color: '#f59e0b', labelKey: 'token_cache_creation' },
  { key: 'cacheRead', color: '#8b5cf6', labelKey: 'token_cache_read' },
  { key: 'cacheRate', color: '#a855f7', labelKey: 'token_cache_rate', rate: true },
];
let tokenTrendVisibility = null;
let apiDetailSeq = 0;
const apiDetailCache = new Map();
const conditionalPayloadCache = new Map();
let apiDetailLastRender = null;
let eventsPageOffset = 0;
let apiDetailRecentOffset = 0;
let updatedState = { type: 'loading', generatedAt: null, message: '' };

function resetPaginationOffsets() {
  eventsPageOffset = 0;
  apiDetailRecentOffset = 0;
}

function updatePaginationButtonLabel(button, key) {
  if (!button) return;
  const label = t(key);
  button.setAttribute('aria-label', label);
  button.title = label;
}

function dashboardPanelData() {
  return selectedClientApi ? filteredSummaryData : summaryData;
}

function selectedClientApiSelector() {
  return selectedClientApi && selectedClientApi.selector ? selectedClientApi.selector : '';
}

function clientApiFilterContext() {
  return $('range').value + '|' + selectedClientApiSelector();
}

function manualPricesFromResponse(data) {
  return data && Object.prototype.hasOwnProperty.call(data, 'manual_prices') ? (data.manual_prices || {}) : modelPrices;
}

// Dom helpers
const $ = (id) => document.getElementById(id);
const setText = (id, value) => { $(id).textContent = value };
function currentLocale() { return typeof getFormatLocale === 'function' ? getFormatLocale() : 'zh-CN'; }
function localizedColon() { return String(typeof I18N_LANG === 'string' ? I18N_LANG : '').startsWith('zh') ? '：' : ': '; }
function withLabel(key, value) { return t(key) + localizedColon() + value; }
function formatInteger(value) { return fmt.format(num(value)); }
function formatDateTime(value) { return new Date(value).toLocaleString(currentLocale()); }
function formatTime(value) { return new Date(value).toLocaleTimeString(currentLocale()); }
function statusText(failed) { return failed ? t('failure_label') : t('success_label'); }
function renderUpdated() {
  const el = $('updated');
  if (!el) return;
  if (updatedState.type === 'success') {
    setText('updated', withLabel('updated_at', formatTime(updatedState.generatedAt || Date.now())));
    return;
  }
  if (updatedState.type === 'compat') {
    setText('updated', withLabel('updated_at', formatTime(updatedState.generatedAt || Date.now())) + ' (' + t('compat_mode') + ')');
    return;
  }
  if (updatedState.type === 'error') {
    setText('updated', updatedState.message || t('load_usage_failed'));
    return;
  }
  setText('updated', t('loading'));
}

// ---- 主题检测：跟随 CPA 日间/夜间模式 ----
// CPA 管理面板在 iframe 中加载此看板，父窗口通过 data-theme 属性控制主题：
//   data-theme="dark"  → 暗色模式
//   data-theme="white" → 浅色模式（CPA 使用 "white"，不是 "light"）
//   无属性             → 自动（跟随 OS 偏好）
// 同时也通过 localStorage key cli-proxy-theme 持久化。
(function() {
  try {
    var CPA_THEME_STORAGE_KEY = 'cli-proxy-theme';

    function getParentDocument() {
      try {
        if (window.parent && window.parent !== window && window.parent.document) {
          return window.parent.document;
        }
      } catch (e) { /* 跨域不可访问 */ }
      return null;
    }

    // 将 CPA 主题值映射为 dark/light
    // CPA 面板使用 "white" 表示浅色模式
    function cpaThemeToMode(value) {
      if (value === 'dark') return 'dark';
      if (value === 'white') return 'light';
      return null; // auto 或其他 → 回退到 OS 偏好
    }

    // 从父窗口 html data-theme 属性检测
    function detectFromParentDocument() {
      var parentDoc = getParentDocument();
      if (!parentDoc || !parentDoc.documentElement) return null;
      var theme = parentDoc.documentElement.getAttribute('data-theme');
      return cpaThemeToMode(theme);
    }

    // 从共享 localStorage 检测（与父窗口同源时可用）
    function detectFromLocalStorage() {
      try {
        var stored = localStorage.getItem(CPA_THEME_STORAGE_KEY);
        return cpaThemeToMode(stored);
      } catch (e) { return null; }
    }

    // 回退：OS 偏好
    function detectFromOS() {
      if (typeof window.matchMedia === 'function') {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      }
      return 'light';
    }

    function detectCPATheme() {
      // 策略 1（最优先）：父窗口 data-theme 属性
      var mode = detectFromParentDocument();
      if (mode) return mode;
      // 策略 2：共享 localStorage
      mode = detectFromLocalStorage();
      if (mode) return mode;
      // 策略 3：回退 OS 偏好
      return detectFromOS();
    }

    function applyTheme(theme) {
      if (document.documentElement && document.documentElement.setAttribute) {
        document.documentElement.setAttribute('data-cpa-theme', theme);
      }
    }

    function syncTheme() {
      applyTheme(detectCPATheme());
    }

    // 监听父窗口 data-theme 属性变化（MutationObserver on parent）
    if (typeof MutationObserver !== 'undefined') {
      var parentDoc = getParentDocument();
      if (parentDoc && parentDoc.documentElement) {
        var parentObserver = new MutationObserver(function() {
          var mode = detectFromParentDocument();
          if (mode) applyTheme(mode);
        });
        parentObserver.observe(parentDoc.documentElement, {
          attributes: true,
          attributeFilter: ['data-theme']
        });
      }
    }

    // 监听同源 localStorage 变化（父窗口切换主题时触发 storage 事件）
    if (typeof window.addEventListener === 'function') {
      window.addEventListener('storage', function(e) {
        if (e.key === CPA_THEME_STORAGE_KEY) {
          var mode = cpaThemeToMode(e.newValue);
          // auto → 回退到 OS 或父窗口
          if (!mode) {
            mode = detectFromParentDocument() || detectFromOS();
          }
          applyTheme(mode);
        }
      });
    }

    // 监听 OS 偏好变化（仅在 CPA 为 auto 模式时生效）
    if (typeof window.matchMedia === 'function') {
      var osDarkQuery = window.matchMedia('(prefers-color-scheme: dark)');
      var onOSChange = function(e) {
        // 仅在 CPA 主题为 auto 时跟随 OS
        var stored = detectFromLocalStorage();
        var parentMode = detectFromParentDocument();
        if (stored === null && parentMode === null) {
          applyTheme(e.matches ? 'dark' : 'light');
        }
      };
      if (osDarkQuery && osDarkQuery.addEventListener) {
        osDarkQuery.addEventListener('change', onOSChange);
      }
    }

    // 首次同步
    syncTheme();
  } catch (e) { /* 主题检测失败不影响页面功能 */ }
})();

function cloneHeaders(headers) {
  if (!headers) return {};
  if (Array.isArray(headers)) return Object.fromEntries(headers);
  if (typeof headers.forEach === 'function') {
    const cloned = {};
    headers.forEach((value, key) => { cloned[key] = value });
    return cloned;
  }
  return Object.assign({}, headers);
}

function headerValue(headers, name) {
  if (!headers) return '';
  if (typeof headers.get === 'function') return headers.get(name) || headers.get(String(name).toLowerCase()) || '';
  const target = String(name).toLowerCase();
  for (const [key, value] of Object.entries(headers)) {
    if (String(key).toLowerCase() !== target) continue;
    return Array.isArray(value) ? String(value[0] || '') : String(value || '');
  }
  return '';
}

function etagMatches(ifNoneMatch, etag) {
  const current = String(etag || '').trim();
  if (!current) return false;
  const weakValue = (value) => String(value || '').trim().replace(/^W\//i, '');
  return String(ifNoneMatch || '').split(',').some((candidate) => {
    const value = candidate.trim();
    return value === '*' || value === current || weakValue(value) === weakValue(current);
  });
}

async function fetchJsonPayloadWithMeta(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  const responseHeaders = {};
  const responseEtag = headerValue(response.headers, 'ETag');
  if (responseEtag) responseHeaders.ETag = [responseEtag];
  if (response.status === 304) {
    return { data: '', statusCode: 304, headers: responseHeaders };
  }
  if (!text && response.ok && responseEtag && etagMatches(headerValue(options && options.headers, 'If-None-Match'), responseEtag)) {
    return { data: '', statusCode: 304, headers: responseHeaders };
  }
  let payload = null;
  if (text) {
    try { payload = JSON.parse(text) } catch {
      if (!response.ok) throw new Error(text);
      throw new Error(t('response_not_json'));
    }
  }
  if (!response.ok) {
    const message = payload && payload.error && payload.error.message ? payload.error.message : (text || (t('request_failed_colon') + response.status));
    throw new Error(message);
  }
  const meta = unwrapPluginPayloadWithMeta(payload);
  meta.headers = Object.assign({}, meta.headers || {});
  if (responseEtag && !headerValue(meta.headers, 'ETag')) meta.headers.ETag = responseHeaders.ETag;
  if (!meta.statusCode) meta.statusCode = response.status || 200;
  return meta;
}

async function fetchJsonPayload(url, options) {
  const meta = await fetchJsonPayloadWithMeta(url, options);
  return meta.data;
}

async function fetchTextPayload(url, options) {
  const meta = await fetchTextPayloadWithMeta(url, options);
  return meta.data;
}

async function fetchTextPayloadWithMeta(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  if (!response.ok) throw new Error(text || (t('request_failed_colon') + response.status));
  if (!text) return { data: '', statusCode: response.status || 200, headers: {} };
  let payload = null;
  try { payload = JSON.parse(text) } catch { return { data: text, statusCode: response.status || 200, headers: {} } }
  const meta = unwrapPluginPayloadWithMeta(payload);
  if (meta.data == null) meta.data = '';
  meta.data = typeof meta.data === 'string' ? meta.data : JSON.stringify(meta.data);
  return meta;
}

async function fetchConditionalJsonPayload(cacheKey, url, options) {
  const cached = conditionalPayloadCache.get(cacheKey);
  const merged = Object.assign({}, options || {});
  const headers = cloneHeaders(merged.headers);
  if (cached && cached.etag && !headerValue(headers, 'If-None-Match')) headers['If-None-Match'] = cached.etag;
  merged.headers = headers;
  let meta = await fetchJsonPayloadWithMeta(url, merged);
  if (meta.statusCode === 304) {
    if (cached && Object.prototype.hasOwnProperty.call(cached, 'data')) return cached.data;
    const retryOptions = Object.assign({}, options || {});
    const retryHeaders = cloneHeaders(retryOptions.headers);
    delete retryHeaders['If-None-Match'];
    delete retryHeaders['if-none-match'];
    retryOptions.headers = retryHeaders;
    meta = await fetchJsonPayloadWithMeta(String(url) + (String(url).includes('?') ? '&' : '?') + '_ts=' + Date.now(), retryOptions);
    if (meta.statusCode === 304) throw new Error(t('no_304_cache'));
  }
  const etag = headerValue(meta.headers, 'ETag');
  if (etag) conditionalPayloadCache.set(cacheKey, { etag, data: meta.data });
  else conditionalPayloadCache.delete(cacheKey);
  return meta.data;
}

function requireObjectPayload(data, label) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error(label + ' ' + t('empty_response'));
  }
  return data;
}

function managementFetchOptions(options) {
  const merged = Object.assign({}, options || {});
  const headers = Object.assign({}, merged.headers || {});
  const key = currentManagementKey();
  if (key) {
    headers.Authorization = headers.Authorization || ('Bearer ' + key);
    headers['x-management-key'] = headers['x-management-key'] || key;
  }
  merged.headers = headers;
  return merged;
}

function fetchManagementJsonPayload(path, options) {
  return fetchJsonPayload(managementEndpoint(path), managementFetchOptions(options));
}

function pluginFetchOptions(options) {
  return managementFetchOptions(options);
}

async function loadModelPrices() {
  const data = await fetchJsonPayload(pluginEndpoint('model-prices'), pluginFetchOptions({ cache: 'no-store' }));
  modelPrices = (data && data.prices) || {};
  manualModelPrices = manualPricesFromResponse(data);
  return modelPrices;
}

async function saveModelPrice(model, price) {
  const data = await fetchManagementJsonPayload('model-prices', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, price })
  });
  modelPrices = (data && data.prices) || {};
  manualModelPrices = manualPricesFromResponse(data);
  return modelPrices;
}

async function deleteModelPrice(model) {
  const params = new URLSearchParams();
  params.set('model', model);
  const data = await fetchManagementJsonPayload('model-prices?' + params.toString(), { method: 'DELETE' });
  modelPrices = (data && data.prices) || {};
  manualModelPrices = manualPricesFromResponse(data);
  return modelPrices;
}

function drawSpark(id, values, color) {
  const svg = $(id); const w = svg.clientWidth || 320, h = 54; const max = Math.max(...values, 1); const points = values.map((v, i) => [i * (w / (Math.max(values.length - 1, 1))), h - 8 - (v / max) * (h - 16)]);
  const d = points.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1)).join(' ');
  const area = points.length ? d + ' L' + points[points.length - 1][0].toFixed(1) + ' ' + h + ' L' + points[0][0].toFixed(1) + ' ' + h + ' Z' : '';
  svg.setAttribute('viewBox', '0 0 ' + w + ' ' + h);
  svg.innerHTML = (area ? '<path d="' + area + '" fill="' + color + '" fill-opacity="0.1" stroke="none"/>' : '')
    + '<path d="' + d + '" fill="none" stroke="' + color + '" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>';
}

function renderStats() {
  if (!summaryData) return;
  const u = summaryData.usage;
  setText('totalRequests', fmt.format(u.total_requests));
  setText('successText', withLabel('success_requests', formatInteger(u.success_count)));
  setText('failureText', withLabel('failure_requests', formatInteger(u.failure_count)));
  setText('avgLatency', withLabel('avg_latency', formatMs(u.avg_latency_ms)));
  setText('totalTokens', compact(u.total_tokens));
  setText('cachedText', withLabel('cached_tokens', compact(u.cached_tokens)));
  setText('cacheWriteText', withLabel('cache_write_tokens', compact(u.cache_write_tokens)));
  setText('reasoningText', withLabel('reasoning_tokens', compact(u.reasoning_tokens)));
  // RPM: compute from hourly time series
  const hourValues = Object.values(u.requests_by_hour || {}).map(num);
  const recentHours = hourValues.slice(-1);
  const recentReq = recentHours.length ? recentHours[0] : 0;
  setText('rpm', (recentReq / 60).toFixed(2));
  setText('rpmMeta', withLabel('recent_requests_label', formatInteger(recentReq)));
  const cost = (summaryData.model_stats || []).reduce((s, m) => s + aggregateCost(m, modelPrices, manualModelPrices), 0);
  setText('totalCost', formatUsd(cost));
  setText('costMeta', withLabel('total_tokens_label', compact(u.total_tokens)));
  // Sparklines from hourly data
  const reqByHour = Array.from({ length: 24 }, (_, i) => {
    const k = String(i).padStart(2, '0');
    return num(u.requests_by_hour && u.requests_by_hour[k]) || 0;
  });
  const tokByHour = Array.from({ length: 24 }, (_, i) => {
    const k = String(i).padStart(2, '0');
    return num(u.tokens_by_hour && u.tokens_by_hour[k]) || 0;
  });
  drawSpark('requestSpark', reqByHour, '#3b82f6');
  drawSpark('tokenSpark', tokByHour, '#8b5cf6');
  drawSpark('rpmSpark', reqByHour.length ? reqByHour.map(v => v / 60) : [0], '#22c55e');
  drawSpark('costSpark', reqByHour.length ? reqByHour.map(v => (cost > 0 ? v / Math.max(u.total_requests || 1, 1) * cost : 0)) : [0], '#f59e0b');

  // Trend tag badge helper
  const updateCardTag = (cardId, firstHalfSum, secondHalfSum) => {
    const el = $(cardId);
    if (!el || !el.parentElement) return;
    let labelEl = el.parentElement.querySelector('.label');
    if (!labelEl) return;
    let tag = labelEl.querySelector('.trendTag');
    if (firstHalfSum > 0 && secondHalfSum !== undefined) {
      const pctVal = ((secondHalfSum - firstHalfSum) / firstHalfSum) * 100;
      const isUp = pctVal >= 0;
      const text = (isUp ? '+' : '') + pctVal.toFixed(1) + '% ' + (isUp ? '↑' : '↓');
      if (!tag) {
        tag = document.createElement('span');
        labelEl.appendChild(tag);
      }
      tag.className = 'trendTag ' + (isUp ? 'up' : 'down');
      tag.textContent = text;
    } else if (tag) {
      tag.remove();
    }
  };

  const reqH1 = reqByHour.slice(0, 12).reduce((a, b) => a + b, 0);
  const reqH2 = reqByHour.slice(12).reduce((a, b) => a + b, 0);
  const tokH1 = tokByHour.slice(0, 12).reduce((a, b) => a + b, 0);
  const tokH2 = tokByHour.slice(12).reduce((a, b) => a + b, 0);

  updateCardTag('totalRequests', reqH1, reqH2);
  updateCardTag('totalTokens', tokH1, tokH2);
}

function storageTitle() {
  return Array.from(arguments).filter(Boolean).join(' | ');
}

function formatStorageBytes(value) {
  let bytes = Math.max(0, num(value));
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let unit = 0;
  while (bytes >= 1024 && unit < units.length - 1) {
    bytes /= 1024;
    unit++;
  }
  return (unit === 0 ? formatInteger(bytes) : trimFixed(bytes, bytes >= 10 ? 1 : 2)) + ' ' + units[unit];
}

function renderStorageStatus() {
  const el = $('storageStatus');
  if (!el) return;
  const storage = summaryData && summaryData._meta && summaryData._meta.storage;
  el.className = 'storageStatus';
  el.title = '';
  if (!storage) {
    el.textContent = '';
    return;
  }
  if (!storage.enabled) {
    el.textContent = t('storage_disabled');
    el.classList.add('warn');
    el.title = storage.path || '';
    return;
  }
  if (storage.last_error) {
    el.textContent = t('storage_error');
    el.classList.add('bad');
    el.title = storage.last_error;
    return;
  }
  const titleParts = [];
  const databasePath = storage.database_path || storage.path || '';
  if (databasePath) titleParts.push(withLabel('storage_database_path', databasePath));
  titleParts.push(withLabel('storage_event_count', formatInteger(storage.event_count)));
  titleParts.push(withLabel('storage_database_size', formatStorageBytes(storage.database_size_bytes)));
  if (storage.last_write_at) titleParts.push(withLabel('storage_last_write', formatDateTime(storage.last_write_at)));
  const dropped = num(storage.dropped_events);
  if (dropped > 0) titleParts.push(withLabel('storage_dropped_events', formatInteger(dropped)));
  el.textContent = t('storage_enabled');
  el.classList.add('ok');
  el.title = storageTitle(...titleParts);
}

function renderHealth() {
  if (!summaryData) return;
  const grid = normalizeHealthGrid(summaryData.health_grid, summaryData.generated_at);
  const count = grid.length;
  let totalS = 0, totalF = 0;
  const cells = [], tooltips = [];
  grid.forEach((slot, i) => {
    totalS += slot.success; totalF += slot.failure;
    const total = slot.total;
    const rate = total ? slot.success / total : -1;
    const timeRange = formatDateTime(slot.start) + ' - ' + formatDateTime(slot.end);
    const tip = '<span>' + timeRange + '</span><br>' + (total ? '<span class="ok">' + t('success_label') + ' ' + formatInteger(slot.success) + '</span> <span class="bad">' + t('failure_label') + ' ' + formatInteger(slot.failure) + '</span> <span>' + t('success_rate') + ' ' + pct(rate * 100) + '</span>' : '<span>' + t('no_requests') + '</span>');
    tooltips.push(tip);
    cells.push('<div class="healthCell ' + (total ? 'active' : '') + '" data-health-idx="' + i + '" style="' + healthCellStyle(i, count, total, rate) + '"></div>');
  });
  $('healthGrid').innerHTML = cells.join('');
  const dateLabelsEl = $('healthDateLabels');
  if (dateLabelsEl) {
    const dateItems = [];
    for (let r = 0; r < 5; r++) {
      const slot = grid[r * 96];
      if (slot && slot.start) {
        const dStr = formatDateTime(slot.start).slice(5, 10);
        dateItems.push('<div class="healthDateLabel">' + esc(dStr) + '</div>');
      } else {
        dateItems.push('<div class="healthDateLabel">-</div>');
      }
    }
    dateLabelsEl.innerHTML = dateItems.join('');
  }
  const tip = $('tooltip');
  const showTip = function (cell) {
    if (!cell) return;
    const idx = parseInt(cell.dataset.healthIdx); if (isNaN(idx) || idx < 0 || idx >= count) { tip.classList.add('hidden'); return }
    tip.innerHTML = tooltips[idx]; tip.classList.remove('hidden');
    const r = cell.getBoundingClientRect(); let left = r.right + 8, top = r.top - 6;
    if (left + 260 > window.innerWidth) left = r.left - 268; if (top + 64 > window.innerHeight) top = window.innerHeight - 74; if (top < 6) top = 6;
    tip.style.left = left + 'px'; tip.style.top = top + 'px';
  };
  $('healthGrid').onmouseover = function (e) {
    const cell = e.target.closest('.healthCell');
    if (!cell) { tip.classList.add('hidden'); return }
    showTip(cell);
  };
  $('healthGrid').onmouseleave = function (e) {
    if (!e.relatedTarget || !e.relatedTarget.closest('.healthCell')) tip.classList.add('hidden');
  };
  $('healthGrid').onmouseout = function (e) { const t = e.relatedTarget; if (!t || !t.closest('.healthCell')) tip.classList.add('hidden') };
  const total = totalS + totalF; setText('healthRate', total ? pct(totalS / total * 100) : '-'); setText('healthSuccess', t('success_label') + ' ' + formatInteger(totalS)); setText('healthFailure', t('failure_label') + ' ' + formatInteger(totalF));
}

const healthGridCount = 480;
const healthGridStepMs = 15 * 60 * 1000;

function healthGridWindowEnd(value) {
  const ms = timestampMs(value) || Date.now();
  return Math.floor(ms / healthGridStepMs) * healthGridStepMs + healthGridStepMs;
}

function emptyHealthGrid(value) {
  const end = healthGridWindowEnd(value);
  const start = end - healthGridCount * healthGridStepMs;
  return Array.from({ length: healthGridCount }, (_, i) => {
    const slotStart = start + i * healthGridStepMs;
    return { slot: i, total: 0, success: 0, failure: 0, start: new Date(slotStart).toISOString(), end: new Date(slotStart + healthGridStepMs).toISOString() };
  });
}

function normalizeHealthGrid(grid, generatedAt) {
  const normalized = emptyHealthGrid(generatedAt);
  if (!Array.isArray(grid)) return normalized;
  const visibleGrid = grid.length > healthGridCount ? grid.slice(-healthGridCount) : grid;
  visibleGrid.forEach((slot, i) => {
    if (!slot || typeof slot !== 'object') return;
    const success = num(slot.success);
    const failure = num(slot.failure);
    normalized[i] = Object.assign({}, normalized[i], slot, {
      slot: i,
      success,
      failure,
      total: num(slot.total) || success + failure,
    });
  });
  return normalized;
}

function modelNames() {
  if (summaryData && summaryData.model_stats) return summaryData.model_stats.map(m => m.model).filter(Boolean).sort((a, b) => a.localeCompare(b));
  return [];
}

function normalizedPriceKey(value) {
  return String(value || '').trim().toLowerCase();
}

function uniquePriceKeys(values) {
  const keys = [];
  const seen = new Set();
  (values || []).forEach((value) => {
    const key = String(value || '').trim();
    const normalized = normalizedPriceKey(key);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    keys.push(key);
  });
  return keys;
}

function sortPriceKeys(values) {
  return values.sort((a, b) => {
    const aScoped = String(a).includes('/');
    const bScoped = String(b).includes('/');
    if (aScoped !== bScoped) return aScoped ? 1 : -1;
    return String(a).localeCompare(String(b));
  });
}

function providerModelPriceKey(model, provider) {
  const modelKey = String(model || '').trim();
  const providerKey = String(provider || '').trim();
  if (!modelKey || !providerKey) return '';
  const slash = modelKey.indexOf('/');
  if (slash > 0 && normalizedPriceKey(modelKey.slice(0, slash)) === normalizedPriceKey(providerKey)) return modelKey;
  return providerKey + '/' + modelKey;
}

function addProviderModelOptions(target, row, fallbackModel) {
  if (!row) return;
  const model = String(row.model || fallbackModel || '').trim();
  if (!model) return;
  const add = (provider) => {
    const key = providerModelPriceKey(model, provider);
    if (key) target.push(key);
  };
  add(row.provider);
  (Array.isArray(row.providers) ? row.providers : []).forEach((provider) => add(provider && provider.provider));
}

function upstreamPriceModelOptions() {
  const values = [];
  const addSummary = (summary) => {
    if (!summary || typeof summary !== 'object') return;
    (Array.isArray(summary.model_stats) ? summary.model_stats : []).forEach((row) => addProviderModelOptions(values, row));
    const apis = summary.usage && summary.usage.apis;
    Object.values(apis && typeof apis === 'object' ? apis : {}).forEach((api) => {
      const models = api && api.models;
      Object.entries(models && typeof models === 'object' ? models : {}).forEach(([model, row]) => addProviderModelOptions(values, row, model));
    });
    (Array.isArray(summary.client_api_stats) ? summary.client_api_stats : []).forEach((client) => {
      (Array.isArray(client && client.models) ? client.models : []).forEach((row) => addProviderModelOptions(values, row));
    });
  };
  addSummary(summaryData);
  if (filteredSummaryData && filteredSummaryData !== summaryData) addSummary(filteredSummaryData);
  (eventsData && Array.isArray(eventsData.events) ? eventsData.events : []).forEach((event) => addProviderModelOptions(values, event));
  return sortPriceKeys(uniquePriceKeys(values));
}

function priceModelOptions() {
  // The picker lists actual provider/model pairs seen in usage plus saved
  // provider-scoped manual keys. Bare keys remain a deliberate free-text
  // fallback so they are not accidentally applied to every upstream.
  const savedScoped = Object.keys(manualModelPrices || {}).filter((key) => String(key).includes('/'));
  return sortPriceKeys(uniquePriceKeys([...upstreamPriceModelOptions(), ...savedScoped]));
}

function priceReferenceOptions() {
  // Prefer the spelling of a manual key when an effective entry differs only
  // by case, then append source-only prices for read-only lookup.
  return sortPriceKeys(uniquePriceKeys([...Object.keys(manualModelPrices || {}), ...Object.keys(modelPrices || {})]));
}

function priceReferenceLookupKeys(model) {
  const keys = priceLookupKeys(model, '');
  const value = String(model || '').trim();
  const slash = value.indexOf('/');
  if (slash > 0 && slash < value.length - 1) {
    // A catalogue key can be provider/model where model itself contains a
    // slash (for example openrouter/openai/gpt-4.1). Re-run the normal lookup
    // with the first segment as provider so bare fallback prices behave like
    // the backend resolver.
    const provider = value.slice(0, slash);
    const modelName = value.slice(slash + 1);
    keys.push(...priceLookupKeys(modelName, provider));
  }
  return uniquePriceKeys(keys);
}

function priceMatchForReference(model) {
  const keys = priceReferenceLookupKeys(model);
  for (const key of keys) {
    const price = directPriceForModel(key, manualModelPrices);
    if (price) return { price, key, source: 'manual' };
  }
  for (const key of keys) {
    const price = directPriceForModel(key, modelPrices);
    if (price) return { price, key, source: 'default' };
  }
  return null;
}

function filterPriceReferenceOptions(options, query) {
  const normalized = normalizedPriceKey(query);
  if (!normalized) return (options || []).slice();
  return (options || []).filter((model) => normalizedPriceKey(model).includes(normalized));
}

function closePriceReferenceOptions() {
  const input = $('priceReferenceModel');
  const list = $('priceReferenceOptions');
  priceReferenceActiveIndex = -1;
  priceReferenceVisibleOptions = [];
  if (list) list.hidden = true;
  if (input) {
    input.setAttribute('aria-expanded', 'false');
    input.setAttribute('aria-activedescendant', '');
  }
}

function renderPriceReferenceOptions(query, open) {
  const input = $('priceReferenceModel');
  const list = $('priceReferenceOptions');
  if (!input || !list) return;
  const options = priceReferenceOptions();
  const matches = filterPriceReferenceOptions(options, query);
  priceReferenceVisibleOptions = matches.slice(0, priceReferenceResultLimit);
  if (priceReferenceActiveIndex >= priceReferenceVisibleOptions.length) priceReferenceActiveIndex = priceReferenceVisibleOptions.length - 1;
  const optionHtml = priceReferenceVisibleOptions.map((model, index) => {
    const active = index === priceReferenceActiveIndex;
    return '<button type="button" id="priceReferenceOption' + index + '" class="searchComboOption' + (active ? ' active' : '') + '" role="option" aria-selected="' + (active ? 'true' : 'false') + '" data-price-reference-option="' + esc(model) + '">' + esc(model) + '</button>';
  }).join('');
  const emptyHtml = optionHtml ? '' : '<div class="searchComboEmpty">' + esc(t(options.length ? 'price_reference_no_match' : 'price_reference_none')) + '</div>';
  const limitHtml = matches.length > priceReferenceResultLimit ? '<div class="searchComboStatus">' + esc(t('price_reference_result_limit', priceReferenceResultLimit, matches.length)) + '</div>' : '';
  list.innerHTML = optionHtml + emptyHtml + limitHtml;
  list.hidden = !open;
  input.setAttribute('aria-expanded', open ? 'true' : 'false');
  input.setAttribute('aria-activedescendant', open && priceReferenceActiveIndex >= 0 ? 'priceReferenceOption' + priceReferenceActiveIndex : '');
}

function renderPriceReferenceInfo(options) {
  const info = $('priceReferenceInfo');
  if (!info) return;
  options = options || priceReferenceOptions();
  if (!selectedPriceReferenceModel) {
    info.innerHTML = '<div class="empty">' + esc(options.length ? t('price_reference_empty') : t('price_reference_none')) + '</div>';
    return;
  }
  const match = priceMatchForReference(selectedPriceReferenceModel);
  if (!match) {
    info.innerHTML = '<div class="empty">' + esc(t('price_reference_missing')) + '</div>';
    return;
  }
  const sourceName = match.source === 'manual' ? t('price_source_manual') : t('price_source_default');
  const source = normalizedPriceKey(match.key) === normalizedPriceKey(selectedPriceReferenceModel) ? sourceName : sourceName + ' · ' + t('price_match_key', match.key);
  const fields = [
    ['input_price', 'prompt'],
    ['output_price', 'completion'],
    ['cache_price', 'cache'],
    ['cache_write_price', 'cache_write'],
  ];
  info.innerHTML = '<div class="priceReferenceCard"><div class="priceReferenceHead"><span class="priceReferenceModel">' + esc(selectedPriceReferenceModel) + '</span><span class="priceReferenceSource">' + esc(source) + '</span></div><div class="priceReferenceGrid">' + fields.map(([label, key]) => '<div class="priceReferenceValue"><span class="priceReferenceValueLabel">' + esc(t(label)) + '</span><span class="priceReferenceValueNumber">' + num(match.price[key]).toFixed(4) + '</span></div>').join('') + '</div></div>';
}

function selectPriceReferenceModel(model) {
  const input = $('priceReferenceModel');
  const options = priceReferenceOptions();
  selectedPriceReferenceModel = options.find((key) => normalizedPriceKey(key) === normalizedPriceKey(model)) || '';
  if (input) input.value = selectedPriceReferenceModel;
  renderPriceReferenceInfo(options);
  closePriceReferenceOptions();
}

function renderPriceReference() {
  const input = $('priceReferenceModel');
  if (!input) return;
  const options = priceReferenceOptions();
  selectedPriceReferenceModel = options.find((key) => normalizedPriceKey(key) === normalizedPriceKey(selectedPriceReferenceModel)) || '';
  input.value = selectedPriceReferenceModel;
  input.disabled = !options.length;
  renderPriceReferenceInfo(options);
  renderPriceReferenceOptions(input.value, false);
}

function movePriceReferenceOption(delta) {
  const input = $('priceReferenceModel');
  const list = $('priceReferenceOptions');
  if (!input || !list || input.disabled) return;
  if (list.hidden) {
    priceReferenceActiveIndex = -1;
    renderPriceReferenceOptions(input.value, true);
  }
  if (!priceReferenceVisibleOptions.length) return;
  if (delta > 0) priceReferenceActiveIndex = priceReferenceActiveIndex < priceReferenceVisibleOptions.length - 1 ? priceReferenceActiveIndex + 1 : 0;
  else priceReferenceActiveIndex = priceReferenceActiveIndex > 0 ? priceReferenceActiveIndex - 1 : priceReferenceVisibleOptions.length - 1;
  renderPriceReferenceOptions(input.value, true);
}

function handlePriceReferenceKeydown(event) {
  const input = $('priceReferenceModel');
  if (!input) return;
  const key = event && event.key;
  if (key === 'ArrowDown' || key === 'ArrowUp') {
    if (event && event.preventDefault) event.preventDefault();
    movePriceReferenceOption(key === 'ArrowDown' ? 1 : -1);
    return;
  }
  if (key === 'Enter') {
    const options = priceReferenceOptions();
    const exact = options.find((model) => normalizedPriceKey(model) === normalizedPriceKey(input.value));
    const selected = priceReferenceActiveIndex >= 0 ? priceReferenceVisibleOptions[priceReferenceActiveIndex] : exact || (priceReferenceVisibleOptions.length === 1 ? priceReferenceVisibleOptions[0] : '');
    if (selected) {
      if (event && event.preventDefault) event.preventDefault();
      selectPriceReferenceModel(selected);
    }
    return;
  }
  if (key === 'Escape') {
    if (event && event.preventDefault) event.preventDefault();
    input.value = selectedPriceReferenceModel;
    closePriceReferenceOptions();
    return;
  }
  if (key === 'Tab') closePriceReferenceOptions();
}

function elementContains(root, node) {
  while (node) {
    if (node === root) return true;
    node = node.parentNode;
  }
  return false;
}

function fillPriceForm(model) {
  const value = String(model || '').trim();
  $('priceModel').value = value;
  const p = value ? priceForModel(value, modelPrices, '', manualModelPrices) : null;
  $('pricePrompt').value = p ? (p.prompt ?? '') : '';
  $('priceCompletion').value = p ? (p.completion ?? '') : '';
  $('priceCache').value = p ? (p.cache ?? '') : '';
  $('priceCacheWrite').value = p ? (p.cache_write ?? '') : '';
}

function syncPriceFormForModel(model) {
  fillPriceForm(model);
}

function renderPrices() {
  const selected = $('priceModel').value;
  $('priceModelOptions').innerHTML = priceModelOptions().map((m) => '<option value="' + esc(m) + '"></option>').join('');
  $('priceModel').value = selected;
  renderPriceReference();
  const entries = Object.entries(manualModelPrices).sort(([a], [b]) => a.localeCompare(b));
  $('priceList').innerHTML = entries.length ? entries.map(([m, p]) => '<div class="priceItem"><div><strong>' + esc(m) + '</strong><div class="priceMeta"><span>' + t('input_price') + ' ' + num(p.prompt).toFixed(4) + '</span><span>' + t('output_price') + ' ' + num(p.completion).toFixed(4) + '</span><span>' + t('cache_price') + ' ' + num(p.cache).toFixed(4) + '</span><span>' + t('cache_write_price') + ' ' + num(p.cache_write).toFixed(4) + '</span></div></div><div class="priceActions"><button class="btn" data-edit-price="' + esc(m) + '">' + t('edit') + '</button><button class="btn danger" data-del-price="' + esc(m) + '">' + t('delete') + '</button></div></div>').join('') : '<div class="empty">' + t('no_prices') + '</div>';
  document.querySelectorAll('[data-edit-price]').forEach((btn) => btn.onclick = () => fillPriceForm(btn.dataset.editPrice));
  document.querySelectorAll('[data-del-price]').forEach((btn) => btn.onclick = async () => {
    try {
      await deleteModelPrice(btn.dataset.delPrice);
      if (normalizedPriceKey($('priceModel').value) === normalizedPriceKey(btn.dataset.delPrice)) fillPriceForm('');
      await rerender({ refreshEvents: false, refreshApiDetail: true });
    } catch (e) {
      alert(t('price_delete_failed') + (e && e.message ? e.message : t('unknown_error')));
    }
  });
}

function renderClientApiStats() {
  const stats = summaryData && summaryData.client_api_stats;
  const filterStatus = $('clientApiFilterStatus');
  if (filterStatus) {
    filterStatus.innerHTML = selectedClientApi ? '<span class="clientApiFilterBadge">' + esc(t('client_api_current_filter', selectedClientApi.label)) + '</span>' +
      (filteredSummaryError ? '<span class="clientApiFilterError">' + esc(t('client_api_filter_failed')) + '</span>' : '') : '';
  }
  document.querySelectorAll('[data-api-sort]').forEach((btn) => btn.classList.toggle('active', !clientApiSelectMode && btn.dataset.apiSort === clientApiSort));
  const selectButton = document.querySelectorAll('[data-client-api-select]')[0];
  if (selectButton) selectButton.classList.toggle('active', clientApiSelectMode);
  if (!stats || !stats.length) { $('clientApiStats').innerHTML = '<div class="empty">' + t('no_api_data') + '</div>'; return }
  const hasSelectableClientAPI = stats.some((r) => !!r.selector);
  if (filterStatus && clientApiSelectMode && !hasSelectableClientAPI) {
    filterStatus.innerHTML += '<span class="clientApiFilterError">' + esc(t('client_api_filter_compat_unavailable')) + '</span>';
  }
  let rows = stats.map((r) => ({
    name: clientApiLabel(r),
    label: clientApiLabel(r),
    selector: r.selector || '',
    hash: r.api_key_hash || '',
    requests: r.total_requests,
    success: r.success_count,
    failure: r.failure_count,
    tokens: r.total_tokens,
    cost: (r.models || []).reduce((s, m) => s + aggregateCost(m, modelPrices, manualModelPrices), 0)
  }));
  if (clientApiSort === 'tokens') rows.sort((a, b) => b.tokens - a.tokens);
  else if (clientApiSort === 'cost') rows.sort((a, b) => b.cost - a.cost);
  else rows.sort((a, b) => b.requests - a.requests);
  $('clientApiStats').innerHTML = rows.length ? '<div class="apiCardGrid">' + rows.map((r) => {
    const selected = !!(selectedClientApi && r.selector && selectedClientApi.selector === r.selector);
    const interactive = clientApiSelectMode && !!r.selector;
    return '<div class="apiCard' + (interactive ? ' selectable' : '') + (selected ? ' selected' : '') + '"' +
      (interactive ? ' role="button" tabindex="0" data-client-api-selector="' + esc(r.selector) + '" aria-pressed="' + (selected ? 'true' : 'false') + '"' : '') +
      '><div><div class="apiName">' + esc(r.name) + (selected ? '<span class="selectedBadge">' + esc(t('client_api_selected')) + '</span>' : '') + '</div><div class="apiChips"><span class="chip">' + withLabel('sort_requests', formatInteger(r.requests)) + ' (<span class="ok">' + formatInteger(r.success) + '</span>&nbsp;<span class="bad">' + formatInteger(r.failure) + '</span>)</span><span class="chip">' + withLabel('sort_tokens', compact(r.tokens)) + '</span><span class="chip">' + withLabel('sort_cost', formatUsd(r.cost)) + '</span></div></div></div>';
  }).join('') + '</div>' : '<div class="empty">' + t('no_api_data') + '</div>';
  document.querySelectorAll('[data-client-api-selector]').forEach((card) => {
    const activate = () => selectClientApiCard(card.getAttribute('data-client-api-selector') || '', rows);
    card.onclick = activate;
    card.onkeydown = (event) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); activate() } };
  });
}

async function selectClientApiCard(selector, rows) {
  if (!selector) return;
  if (selectedClientApi && selectedClientApi.selector === selector) {
    selectedClientApi = null;
    filteredSummaryData = null;
    filteredSummaryContext = '';
    filteredSummaryError = null;
  } else {
    const row = rows.find((item) => item.selector === selector);
    selectedClientApi = { selector, label: row ? row.name : t('unknown_api') };
    filteredSummaryData = null;
    filteredSummaryContext = '';
    filteredSummaryError = null;
    await refreshFilteredSummary();
  }
  resetPaginationOffsets();
  await rerender({ refreshEvents: true, refreshApiDetail: true });
}

async function refreshFilteredSummary() {
  if (!selectedClientApi) return null;
  const context = clientApiFilterContext();
  const params = new URLSearchParams();
  params.set('range', $('range').value);
  params.set('client_api', selectedClientApi.selector);
  const url = pluginEndpoint('dashboard-summary') + '?' + params.toString();
  try {
    const data = requireObjectPayload(await fetchConditionalJsonPayload('dashboard-summary:' + url, url, pluginFetchOptions({ cache: 'no-store' })), 'dashboard-summary');
    if (context !== clientApiFilterContext()) return null;
    filteredSummaryData = data;
    filteredSummaryContext = context;
    filteredSummaryError = null;
    return data;
  } catch (error) {
    if (context === clientApiFilterContext()) {
      if (filteredSummaryContext !== context) filteredSummaryData = null;
      filteredSummaryError = error;
    }
    return null;
  }
}

function renderApiStats() {
  const panelData = dashboardPanelData();
  const usage = panelData && panelData.usage;
  if (!usage || !usage.apis) {
    selectedApi = '';
    $('apiStats').innerHTML = '<div class="empty">' + (filteredSummaryError ? t('client_api_filter_failed') : t('no_upstream_data')) + '</div>';
    $('apiSelect').innerHTML = '<option value="">' + t('upstream_select_none') + '</option>';
    $('apiSelect').value = '';
    $('apiSelect').disabled = true;
    return;
  }
  const rows = Object.entries(usage.apis).map(([api, a]) => ({
    api,
    requests: a.total_requests,
    success: a.success_count,
    failure: a.failure_count,
    tokens: a.total_tokens,
    avgLatency: a.avg_latency_ms,
    successRate: a.total_requests ? a.success_count / a.total_requests * 100 : 100,
    modelCount: Object.keys(a.models || {}).length
  })).sort((a, b) => b.requests - a.requests);
  if (rows.length && (!selectedApi || !rows.some((r) => r.api === selectedApi))) {
    selectedApi = rows[0].api;
    apiDetailRecentOffset = 0;
  }
  if (!rows.length && selectedApi) {
    selectedApi = '';
    apiDetailRecentOffset = 0;
  }
  $('apiSelect').innerHTML = rows.length ? rows.map((r) => '<option value="' + esc(r.api) + '">' + esc(friendlyApiName(r.api)) + '</option>').join('') : '<option value="">' + t('upstream_select_none') + '</option>';
  $('apiSelect').value = selectedApi;
  $('apiSelect').disabled = !rows.length;
  $('apiSelect').onchange = () => {
    const nextApi = $('apiSelect').value;
    if (nextApi !== selectedApi) apiDetailRecentOffset = 0;
    selectedApi = nextApi;
    renderApiStats();
    renderApiDetail();
  };
  $('apiStats').innerHTML = rows.length ? '<table><thead><tr><th>' + t('col_api') + '</th><th>' + t('col_requests') + '</th><th>' + t('col_success_rate') + '</th><th>' + t('col_tokens') + '</th><th>' + t('col_avg_latency') + '</th><th>' + t('col_models') + '</th></tr></thead><tbody>' + rows.map((r) => '<tr class="clickableRow ' + (r.api === selectedApi ? 'selectedRow' : '') + '" data-api="' + esc(r.api) + '"><td class="nameCell">' + esc(friendlyApiName(r.api)) + '</td><td>' + formatInteger(r.requests) + ' <span class="ok">(' + formatInteger(r.success) + '</span> <span class="bad">' + formatInteger(r.failure) + ')</span></td><td class="' + (r.successRate >= 95 ? 'ok' : r.successRate >= 80 ? 'neutral' : 'bad') + '">' + pct(r.successRate) + '</td><td>' + compact(r.tokens) + '</td><td>' + formatMs(r.avgLatency) + '</td><td>' + formatInteger(r.modelCount) + ' ' + t('model_count') + '</td></tr>').join('') + '</tbody></table>' : '<div class="empty">' + t('no_upstream_data') + '</div>';
  document.querySelectorAll('[data-api]').forEach((row) => row.onclick = () => {
    const nextApi = row.getAttribute('data-api') || '';
    if (nextApi !== selectedApi) apiDetailRecentOffset = 0;
    selectedApi = nextApi;
    renderApiStats();
    renderApiDetail();
  });
}

function metricHtml(label, value, extra) {
  return '<div class="metric"><div class="metricLabel">' + esc(label) + '</div><div class="metricValue">' + value + '</div>' + (extra ? '<div class="subtle metricMeta">' + extra + '</div>' : '') + '</div>';
}

function barsHtml(title, rows, total, emptyText, showTokenUsage) {
  if (!rows.length) return '<div><div class="subtle" style="margin-bottom:8px">' + esc(title) + '</div><div class="empty">' + esc(emptyText) + '</div></div>';
  return '<div><div class="subtle" style="margin-bottom:8px">' + esc(title) + '</div><div class="barList">' + rows.slice(0, 8).map((r) => {
    const width = total ? Math.max(4, Math.round(r.requests / total * 100)) : 0;
    const tokens = showTokenUsage ? '<span class="barTokens">' + compact(r.tokens) + '</span>' : '';
    const cost = Number.isFinite(r.cost) ? '<span class="barCost">' + formatUsd(r.cost) + '</span>' : '';
    return '<div class="barItem"><div class="barLabel" title="' + esc(r.name) + '">' + esc(r.name) + tokens + cost + '</div><div class="barTrack"><div class="barFill" style="width:' + width + '%"></div></div><div class="barValue">' + formatInteger(r.requests) + ' ' + t('col_requests') + '</div></div>';
  }).join('') + '</div></div>';
}

function normalizeApiDetailEvent(d) {
  const tokens = d.tokens || {};
  return Object.assign({}, d, {
    timestamp_ms: timestampMs(d.timestamp),
    total_tokens: totalTokens(d),
    cached_tokens: cacheTokenTotal(tokens),
    cache_write_tokens: num(tokens.cache_write_tokens),
    reasoning_tokens: num(tokens.reasoning_tokens),
    cost: detailCost(d, modelPrices, manualModelPrices)
  });
}

async function fetchApiDetailData(api, recentOffset) {
  if (!Number.isFinite(Number(recentOffset)) || recentOffset < 0) recentOffset = 0;
  const params = new URLSearchParams();
  params.set('range', $('range').value);
  params.set('api', api);
  params.set('recent_limit', String(apiDetailRecentLimit));
  params.set('recent_offset', String(recentOffset));
  if (selectedClientApiSelector()) params.set('client_api', selectedClientApiSelector());
  const url = pluginEndpoint('dashboard-api-detail') + '?' + params.toString();
  const data = requireObjectPayload(await fetchConditionalJsonPayload('dashboard-api-detail:' + url, url, pluginFetchOptions({ cache: 'no-store' })), 'dashboard-api-detail');
  data.recent_events = (data.recent_events || []).map(normalizeApiDetailEvent);
  return data;
}

function apiDetailCacheKey(api, recentOffset) {
  const offset = Number.isFinite(Number(recentOffset)) && recentOffset >= 0 ? recentOffset : 0;
  return api + '|' + $('range').value + '|' + selectedClientApiSelector() + '|' + apiDetailRecentLimit + '|' + offset;
}

function apiDetailErrorHtml(errorRows, loading, error, knownFailureCount) {
  if (loading && !errorRows.length) return '<div><div class="subtle" style="margin-bottom:8px">' + t('error_stats') + '</div><div class="empty">' + t('loading_api_detail') + '</div></div>';
  if (error && !errorRows.length && num(knownFailureCount) === 0) return '<div><div class="subtle" style="margin-bottom:8px">' + t('error_stats') + '</div><div class="empty">' + t('no_failures') + '</div></div>';
  if (error && !errorRows.length) return '<div><div class="subtle" style="margin-bottom:8px">' + t('error_stats') + '</div><div class="empty">' + t('detail_load_failed_msg') + esc(error.message || t('unknown_error')) + '</div></div>';
  return '<div><div class="subtle" style="margin-bottom:8px">' + t('error_stats') + '</div>' +
    (errorRows.length ? '<div class="tableWrap"><table><thead><tr><th>' + t('col_status') + '</th><th>' + t('col_requests') + '</th><th>' + t('error_stats') + '</th></tr></thead><tbody>' + errorRows.slice(0, 10).map((r) => '<tr><td class="bad">' + esc(r.status_code || '-') + '</td><td>' + formatInteger(r.count) + '</td><td><span class="errorText">' + esc(r.failure || t('no_body_returned')) + '</span></td></tr>').join('') + '</tbody></table></div>' : '<div class="empty">' + t('no_failures') + '</div>') +
    '</div>';
}

function reasoningEffortText(detail) {
  const thinking = detail && detail.thinking || {};
  return String(thinking.intensity || thinking.level || thinking.mode || detail && detail.reasoning_effort || '').trim() || '-';
}

function generationSpeedText(detail) {
  const tokens = num(detail && detail.tokens && detail.tokens.output_tokens);
  const latencyMs = num(detail && detail.latency_ms);
  const ttftMs = Math.max(num(detail && detail.ttft_ms), 0);
  const generationMs = detail && detail.stream ? latencyMs - ttftMs : latencyMs;
  if (tokens <= 0 || generationMs <= 0) return '-';
  return (tokens * 1000 / generationMs).toFixed(1) + ' t/s';
}

function apiDetailRecentTotal(detail) {
  if (detail && Object.prototype.hasOwnProperty.call(detail, 'recent_total')) return num(detail.recent_total);
  return num(detail && detail.total_events);
}

function apiDetailRecentHtml(rows, loading, error, detail) {
  const total = apiDetailRecentTotal(detail);
  const limit = Math.max(1, num(detail && detail.recent_limit) || apiDetailRecentLimit);
  const offset = Math.max(0, num(detail && detail.recent_offset));
  const pageCount = total > 0 ? Math.ceil(total / limit) : 0;
  const page = pageCount > 0 ? Math.floor(offset / limit) + 1 : 0;
  const start = total > 0 ? offset + 1 : 0;
  const end = total > 0 ? Math.min(offset + rows.length, total) : 0;
  const pagination = '<div class="pagination apiDetailPagination" id="apiDetailRecentPagination">' +
    '<button id="apiDetailRecentPrev" class="btn" type="button" aria-label="' + esc(t('pagination_previous')) + '"' + (offset <= 0 ? ' disabled' : '') + '>&larr;</button>' +
    '<span id="apiDetailRecentPageLabel">' + esc(t('events_page', start, end, total, page, pageCount)) + '</span>' +
    '<button id="apiDetailRecentNext" class="btn" type="button" aria-label="' + esc(t('pagination_next')) + '"' + (offset + rows.length >= total || total === 0 ? ' disabled' : '') + '>&rarr;</button>' +
    '</div>';
  if (loading && !rows.length) return '<div><div class="subtle" style="margin-bottom:8px">' + t('recent_requests') + '</div><div class="empty">' + t('loading_api_detail') + '</div></div>';
  if (error && !rows.length) return '<div><div class="subtle" style="margin-bottom:8px">' + t('recent_requests') + '</div><div class="empty">' + t('detail_load_failed') + '</div>' + pagination + '</div>';
  return '<div><div class="subtle" style="margin-bottom:8px">' + t('recent_requests') + '</div>' +
    (rows.length ? '<div class="tableWrap"><table><thead><tr><th>' + t('col_time') + '</th><th>' + t('col_model') + '</th><th>' + t('col_reasoning_effort') + '</th><th>' + t('col_endpoint') + '</th><th>' + t('col_generation_speed') + '</th><th>' + t('col_result') + '</th><th>' + t('col_latency') + '</th><th>' + t('col_tokens') + '</th><th>' + t('col_source') + '</th></tr></thead><tbody>' + rows.map((d) => '<tr><td>' + formatDateTime(d.timestamp_ms) + '</td><td class="nameCell">' + esc(d.model) + '</td><td>' + esc(reasoningEffortText(d)) + '</td><td class="nameCell">' + esc(d.endpoint || '-') + '</td><td>' + esc(generationSpeedText(d)) + '</td><td class="' + (d.failed ? 'bad' : 'ok') + '">' + statusText(d.failed) + '</td><td>' + formatDurationAndTTFT(d.latency_ms, d.ttft_ms) + '</td><td>' + formatInteger(d.total_tokens) + '</td><td class="nameCell">' + esc(sourceLabel(d)) + '</td></tr>').join('') + '</tbody></table></div>' : '<div class="empty">' + t('no_detail') + '</div>') + pagination +
    '</div>';
}

function renderApiDetailContent(apiData, detailState) {
  apiDetailLastRender = { api: selectedApi, apiData, detailState };
  const detail = detailState && detailState.detail;
  const rows = (detail && detail.recent_events) || [];
  const loading = detailState && detailState.loading;
  const error = detailState && detailState.error;
  const summary = (detail && detail.summary) || apiData;
  const requests = num(summary.total_requests), success = num(summary.success_count), failure = num(summary.failure_count);
  const knownFailureCount = num(apiData && apiData.failure_count);
  const rate = requests ? success / requests * 100 : 100;
  const models = detail ? (detail.model_stats || []).map((m) => ({ name: m.model || 'unknown', requests: num(m.total_requests), success: num(m.success_count), failure: num(m.failure_count), tokens: num(m.total_tokens), total_tokens: num(m.total_tokens), input_tokens: num(m.input_tokens), output_tokens: num(m.output_tokens), cached_tokens: num(m.cached_tokens), cache_write_tokens: num(m.cache_write_tokens), reasoning_tokens: num(m.reasoning_tokens), providers: m.providers || [], avgLatency: num(m.avg_latency_ms) })) : Object.entries(apiData.models || {}).map(([name, m]) => ({ name, requests: num(m.total_requests), success: num(m.success_count), failure: num(m.failure_count), tokens: num(m.total_tokens), total_tokens: num(m.total_tokens), input_tokens: num(m.input_tokens), output_tokens: num(m.output_tokens), cached_tokens: num(m.cached_tokens), cache_write_tokens: num(m.cache_write_tokens), reasoning_tokens: num(m.reasoning_tokens), providers: m.providers || [], avgLatency: num(m.avg_latency_ms) }));
  models.forEach((m) => { m.cost = aggregateCost({ model: m.name, total_tokens: m.total_tokens, input_tokens: m.input_tokens, output_tokens: m.output_tokens, cached_tokens: m.cached_tokens, cache_write_tokens: m.cache_write_tokens, reasoning_tokens: m.reasoning_tokens, providers: m.providers }, modelPrices, manualModelPrices) });
  models.sort((a, b) => b.requests - a.requests);
  const sources = detail ? (detail.source_stats || []).map((s) => ({ name: sourceLabel({ api: detail.api, source: s.source, provider: s.provider }), requests: num(s.total_requests), success: num(s.success_count), failure: num(s.failure_count), tokens: num(s.total_tokens) })) : [];
  const errorRows = (detail && detail.error_stats) || [];
  const showErrorStats = errorRows.length > 0 || knownFailureCount > 0;
  const totalCost = models.reduce((s, m) => s + m.cost, 0);
  $('apiDetail').innerHTML = '<div class="detailGrid">' +
    metricHtml(t('requests_label'), formatInteger(requests), '<span class="ok">' + t('success_label') + ' ' + formatInteger(success) + '</span>&nbsp;<span class="bad">' + t('failure_label') + ' ' + formatInteger(failure) + '</span>') +
    metricHtml(t('success_rate'), '<span class="' + (rate >= 95 ? 'ok' : rate >= 80 ? 'neutral' : 'bad') + '">' + pct(rate) + '</span>') +
    metricHtml(t('total_tokens_label'), compact(summary.total_tokens), '<span>' + withLabel('cached_tokens', compact(summary.cached_tokens)) + '</span><span>' + withLabel('cache_write_tokens', compact(summary.cache_write_tokens)) + '</span><span>' + withLabel('reasoning_tokens', compact(summary.reasoning_tokens)) + '</span>') +
    metricHtml(t('avg_latency'), formatMs(summary.avg_latency_ms)) +
    metricHtml(t('model_count'), formatInteger(models.length), sources.length ? '<span>' + t('source_count') + ' ' + formatInteger(sources.length) + '</span>' : '') +
    metricHtml(t('total_cost'), formatUsd(totalCost), '<span>' + withLabel('total_tokens_label', compact(summary.total_tokens)) + '</span>') +
    '</div>' +
    '<div class="splitGrid">' +
    barsHtml(t('model_distribution'), models, requests, t('no_model_data'), true) +
    barsHtml(t('source_distribution'), sources, requests, loading ? t('loading_source_data') : t('no_source_data')) +
    '</div>' +
    '<div class="splitGrid detailActivityGrid">' + (showErrorStats ? apiDetailErrorHtml(errorRows, loading, error, knownFailureCount) : '') + apiDetailRecentHtml(rows, loading, error, detail) + '</div>';
  const previousButton = $('apiDetailRecentPrev');
  const nextButton = $('apiDetailRecentNext');
  if (previousButton) previousButton.onclick = () => {
    apiDetailRecentOffset = Math.max(0, apiDetailRecentOffset - apiDetailRecentPageSize);
    renderApiDetail();
  };
  if (nextButton) nextButton.onclick = () => {
    if (!detail || apiDetailRecentOffset + rows.length >= apiDetailRecentTotal(detail)) return;
    apiDetailRecentOffset += apiDetailRecentPageSize;
    renderApiDetail();
  };
}

async function renderApiDetail() {
  const panelData = dashboardPanelData();
  const usage = panelData && panelData.usage;
  const apiData = usage && usage.apis && usage.apis[selectedApi];
  if (!apiData) { apiDetailSeq++; apiDetailLastRender = null; setText('apiDetailTitle', t('upstream_detail_select_hint')); $('apiDetail').innerHTML = '<div class="empty">' + t('no_detail_data') + '</div>'; return }
  const api = selectedApi;
  const seq = ++apiDetailSeq;
  const requestedOffset = apiDetailRecentOffset;
  const cacheKey = apiDetailCacheKey(api, requestedOffset);
  const cached = apiDetailCache.get(cacheKey);
  setText('apiDetailTitle', friendlyApiName(api));
  renderApiDetailContent(apiData, cached ? { detail: cached, loading: true } : { loading: true });
  try {
    const result = await fetchApiDetailData(api, requestedOffset);
    if (seq !== apiDetailSeq || api !== selectedApi || requestedOffset !== apiDetailRecentOffset) return;
    const total = apiDetailRecentTotal(result);
    const limit = Math.max(1, num(result && result.recent_limit) || apiDetailRecentPageSize);
    if (total > 0 && requestedOffset >= total) {
      apiDetailRecentOffset = Math.floor((total - 1) / limit) * limit;
      if (apiDetailRecentOffset !== requestedOffset) return renderApiDetail();
    }
    apiDetailCache.set(cacheKey, result);
    renderApiDetailContent(apiData, { detail: result });
  } catch (e) {
    if (seq !== apiDetailSeq || api !== selectedApi || requestedOffset !== apiDetailRecentOffset) return;
    renderApiDetailContent(apiData, cached ? { detail: cached, error: e } : { error: e });
  }
}

function renderApiDetailFromCache() {
  const panelData = dashboardPanelData();
  const usage = panelData && panelData.usage;
  const apiData = usage && usage.apis && usage.apis[selectedApi];
  if (!apiData) {
    apiDetailSeq++;
    apiDetailLastRender = null;
    setText('apiDetailTitle', t('upstream_detail_select_hint'));
    $('apiDetail').innerHTML = '<div class="empty">' + t('no_detail_data') + '</div>';
    return;
  }
  setText('apiDetailTitle', friendlyApiName(selectedApi));
  if (apiDetailLastRender && apiDetailLastRender.api === selectedApi) {
    renderApiDetailContent(apiData, apiDetailLastRender.detailState);
    return;
  }
  const cached = apiDetailCache.get(apiDetailCacheKey(selectedApi, apiDetailRecentOffset));
  renderApiDetailContent(apiData, cached ? { detail: cached } : { loading: true });
}

function renderModelStats() {
  const panelData = dashboardPanelData();
  if (!panelData || !panelData.model_stats) { $('modelStats').innerHTML = '<div class="empty">' + (filteredSummaryError ? t('client_api_filter_failed') : t('no_model_data')) + '</div>'; return }
  const rows = panelData.model_stats;
  $('modelStats').innerHTML = rows.length ? '<table><thead><tr><th>' + t('col_model') + '</th><th>' + t('col_requests') + '</th><th>' + t('col_tokens') + '</th><th>' + t('col_avg_latency') + '</th><th>' + t('col_success_rate') + '</th><th>' + t('col_cache_rate') + '</th><th>' + t('col_cost') + '</th><th>' + t('col_cost_per_m') + '</th></tr></thead><tbody>' + rows.map((r) => {
    const rate = r.total_requests ? r.success_count / r.total_requests * 100 : 100;
    const cost = aggregateCost(r, modelPrices, manualModelPrices);
    const cRate = cacheRate(r);
    const cpM = costPerMillion(r, modelPrices, manualModelPrices);
    return '<tr><td class="nameCell">' + esc(r.model) + '</td><td>' + formatInteger(r.total_requests) + ' <span class="ok">(' + formatInteger(r.success_count) + '</span> <span class="bad">' + formatInteger(r.failure_count) + ')</span></td><td>' + compact(r.total_tokens) + '</td><td>' + formatMs(r.avg_latency_ms) + '</td><td class="' + (rate >= 95 ? 'ok' : rate >= 80 ? 'neutral' : 'bad') + '">' + pct(rate) + '</td><td class="' + (cRate >= 50 ? 'ok' : cRate >= 20 ? 'neutral' : '') + '">' + pct(cRate) + '</td><td>' + formatUsd(cost) + '</td><td>' + (cpM ? formatUsd(cpM) + ' ' + t('cost_per_m_unit') : '-') + '</td></tr>';
  }).join('') + '</tbody></table>' : '<div class="empty">' + t('no_model_data') + '</div>';
}

function dashboardTooltipTarget(event) {
  const target = event && event.target;
  if (!target) return null;
  return typeof target.closest === 'function' ? target.closest('[data-tooltip]') : null;
}

function dashboardTooltipMove(target, event) {
  const tip = $('tooltip');
  if (!tip || !target || typeof target.getAttribute !== 'function') return;
  const content = target.getAttribute('data-tooltip');
  if (!content) return;
  tip.innerHTML = content;
  tip.classList.remove('hidden');
  const rect = typeof target.getBoundingClientRect === 'function' ? target.getBoundingClientRect() : { left: 0, right: 0, top: 0, bottom: 0 };
  const hasPointer = event && Number.isFinite(Number(event.clientX)) && Number.isFinite(Number(event.clientY));
  const viewportWidth = typeof window !== 'undefined' && Number(window.innerWidth) > 0 ? Number(window.innerWidth) : 1280;
  const viewportHeight = typeof window !== 'undefined' && Number(window.innerHeight) > 0 ? Number(window.innerHeight) : 720;
  const width = tip.offsetWidth || 300;
  const height = tip.offsetHeight || 110;
  const pointerX = hasPointer ? Number(event.clientX) : Number(rect.right || rect.left || 0);
  const pointerY = hasPointer ? Number(event.clientY) : Number(rect.top || 0);
  let left = pointerX + 14;
  let top = pointerY + 14;
  if (left + width + 8 > viewportWidth) left = pointerX - width - 14;
  if (top + height + 8 > viewportHeight) top = pointerY - height - 14;
  left = Math.max(8, Math.min(left, Math.max(8, viewportWidth - width - 8)));
  top = Math.max(8, Math.min(top, Math.max(8, viewportHeight - height - 8)));
  tip.style.left = Math.round(left) + 'px';
  tip.style.top = Math.round(top) + 'px';
}

function hideDashboardTooltip() {
  const tip = $('tooltip');
  if (tip) tip.classList.add('hidden');
}

function bindDashboardTooltip(svg, hoverHandler) {
  if (!svg) return;
  const handleTarget = function (event, move) {
    const target = dashboardTooltipTarget(event);
    if (!target) {
      if (hoverHandler) hoverHandler(null, false);
      hideDashboardTooltip();
      return;
    }
    if (hoverHandler) hoverHandler(target, true);
    if (move) dashboardTooltipMove(target, event);
  };
  svg.onmouseover = function (event) { handleTarget(event, true); };
  svg.onmousemove = function (event) { handleTarget(event, true); };
  svg.onmouseleave = function () {
    if (hoverHandler) hoverHandler(null, false);
    hideDashboardTooltip();
  };
  svg.onfocusin = function (event) { handleTarget(event, true); };
  svg.onfocusout = function (event) {
    const next = dashboardTooltipTarget({ target: event && event.relatedTarget });
    if (next) return;
    if (hoverHandler) hoverHandler(null, false);
    hideDashboardTooltip();
  };
}

function distributionTooltip(row, totalTokens) {
  const tokens = Math.max(num(row && row.tokens), 0);
  const share = totalTokens > 0 ? tokens / totalTokens * 100 : 0;
  return '<div class="tooltipTitle">' + esc(row && row.name) + '</div>' +
    '<div class="tooltipGrid">' +
    '<div class="tooltipRow"><span>' + esc(t('col_requests')) + '</span><strong>' + esc(formatInteger(row && row.requests)) + '</strong></div>' +
    '<div class="tooltipRow"><span>' + esc(t('col_tokens')) + '</span><strong>' + esc(formatInteger(tokens)) + '</strong></div>' +
    '<div class="tooltipRow"><span>' + esc(t('distribution_cost')) + '</span><strong>' + esc(formatUsd(num(row && row.cost))) + '</strong></div>' +
    '<div class="tooltipRow"><span>' + esc(t('distribution_share')) + '</span><strong>' + esc(pct(share)) + '</strong></div>' +
    '</div>';
}

const distributionColors = ['#2563eb', '#14b8a6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#94a3b8'];

function distributionCost(row) {
  if (Array.isArray(row && row.models)) {
    return row.models.reduce((sum, model) => sum + aggregateCost(model, modelPrices, manualModelPrices), 0);
  }
  return aggregateCost(row, modelPrices, manualModelPrices);
}

function normalizeDashboardEndpoint(raw) {
  let value = String(raw || '').trim();
  if (!value || /^(unknown|未知|未知端点)$/i.test(value)) return '';
  const method = value.match(/^(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(\S+)/i);
  if (method) value = method[1];
  try {
    const parsed = new URL(value, 'http://dashboard.local');
    if (parsed.pathname) value = parsed.pathname;
  } catch (e) {
    const queryIndex = value.search(/[?#]/);
    if (queryIndex >= 0) value = value.slice(0, queryIndex);
  }
  value = value.trim();
  if (!value) return '';
  if (!value.startsWith('/')) value = '/' + value;
  value = value.replace(/\/{2,}/g, '/').replace(/\/$/, '');
  if (value === '/responses') value = '/v1/responses';
  return value === '/' ? '' : value;
}

function distributionRowsForPanel(panelData) {
  const usage = panelData && panelData.usage || {};
  const modelRows = (panelData && panelData.model_stats || []).map((row) => ({
    name: row.model || t('unknown_model'),
    requests: num(row.total_requests),
    tokens: num(row.total_tokens),
    cost: distributionCost(row),
  }));
  const upstreamRows = Object.entries(usage.apis || {}).map(([name, row]) => ({
    name,
    requests: num(row.total_requests),
    tokens: num(row.total_tokens),
    cost: distributionCost({ models: Object.entries(row.models || {}).map(([model, stat]) => Object.assign({ model }, stat)) }),
  }));
  const endpointRows = (panelData && panelData.endpoint_stats || []).map((row) => ({
    name: normalizeDashboardEndpoint(row.endpoint) || t('unknown_endpoint'),
    requests: num(row.total_requests),
    tokens: num(row.total_tokens),
    cost: distributionCost(row),
  }));
  return { modelRows, upstreamRows, endpointRows };
}

function renderDistributionDonut(svgId, totalId, rows) {
  const svg = $(svgId);
  const totalEl = $(totalId);
  if (!svg || !totalEl) return;
  const sorted = (rows || []).slice().sort((a, b) => b.tokens - a.tokens || b.requests - a.requests);
  const totalTokens = sorted.reduce((sum, row) => sum + Math.max(row.tokens, 0), 0);
  totalEl.textContent = totalTokens > 0 ? compact(totalTokens) : '-';
  if (!sorted.length || totalTokens <= 0) {
    svg.setAttribute('viewBox', '0 0 120 120');
    svg.innerHTML = '<circle cx="60" cy="60" r="43" fill="none" stroke="var(--cpa-border-light)" stroke-width="18"/>';
    bindDashboardTooltip(svg);
    return;
  }
  const visible = sorted.slice(0, 6);
  const other = sorted.slice(6).reduce((row, item) => ({
    name: t('distribution_other'),
    requests: row.requests + item.requests,
    tokens: row.tokens + item.tokens,
    cost: row.cost + item.cost,
  }), { name: t('distribution_other'), requests: 0, tokens: 0, cost: 0 });
  if (other.tokens > 0) visible.push(other);
  let offset = 0;
  const rings = visible.map((row, index) => {
    const share = row.tokens / totalTokens * 100;
    const color = distributionColors[index % distributionColors.length];
    const tooltip = distributionTooltip(row, totalTokens);
    const title = esc(row.name + ': ' + formatInteger(row.tokens) + ' (' + pct(share) + ')');
    const ring = '<circle class="distributionDonutSegment" cx="60" cy="60" r="43" fill="none" stroke="' + color + '" stroke-width="18" pathLength="100" stroke-dasharray="' + share.toFixed(3) + ' ' + (100 - share).toFixed(3) + '" stroke-dashoffset="-' + offset.toFixed(3) + '" transform="rotate(-90 60 60)" tabindex="0" focusable="true" role="img" aria-label="' + esc(row.name + ' ' + formatInteger(row.tokens)) + '" data-tooltip="' + esc(tooltip) + '"><title>' + title + '</title></circle>';
    offset += share;
    return ring;
  }).join('');
  svg.setAttribute('viewBox', '0 0 120 120');
  svg.innerHTML = '<circle cx="60" cy="60" r="43" fill="none" stroke="var(--cpa-border-light)" stroke-width="18"/>' + rings;
  bindDashboardTooltip(svg);
}

function renderDistributionTable(elementId, rows) {
  const el = $(elementId);
  if (!el) return;
  const sorted = (rows || []).slice().sort((a, b) => b.tokens - a.tokens || b.requests - a.requests);
  if (!sorted.length) {
    el.innerHTML = '<div class="distributionEmpty">' + esc(t('distribution_empty')) + '</div>';
    return;
  }
  const totalTokens = sorted.reduce((sum, item) => sum + num(item.tokens), 0);
  const visible = sorted.slice(0, 6);
  const other = sorted.slice(6).reduce((row, item) => ({
    name: t('distribution_other'),
    requests: row.requests + item.requests,
    tokens: row.tokens + item.tokens,
    cost: row.cost + item.cost,
  }), { name: t('distribution_other'), requests: 0, tokens: 0, cost: 0 });
  if (other.tokens > 0) visible.push(other);
  el.innerHTML = '<table><thead><tr><th>' + t('distribution_name') + '</th><th>' + t('col_requests') + '</th><th>' + t('col_tokens') + '</th><th>' + t('distribution_cost') + '</th></tr></thead><tbody>' + visible.map((row, index) => {
    const color = distributionColors[index % distributionColors.length];
    const pctVal = totalTokens > 0 ? Math.min(100, Math.max(0, (row.tokens / totalTokens) * 100)) : 0;
    return '<tr><td class="nameCell" title="' + esc(row.name) + '"><span class="distributionLegendDot" style="background:' + color + '"></span>' + esc(row.name) + '<div class="progressPillBg"><div class="progressPillFill" style="width:' + pctVal.toFixed(1) + '%;background:' + color + '"></div></div></td><td>' + formatInteger(row.requests) + '</td><td>' + compact(row.tokens) + '</td><td>' + formatUsd(row.cost) + '</td></tr>';
  }).join('') + '</tbody></table>';
}

function tokenTrendBucketValue(values, hour) {
  if (!values || typeof values !== 'object') return {};
  const n = Number(hour);
  if (!Number.isFinite(n)) return {};
  const padded = String(n).padStart(2, '0');
  const plain = String(n);
  if (Object.prototype.hasOwnProperty.call(values, padded)) return values[padded] || {};
  if (Object.prototype.hasOwnProperty.call(values, plain)) return values[plain] || {};
  return {};
}

function tokenTrendPoint(key, hourly, source, totals) {
  const row = hourly ? tokenTrendBucketValue(source, key) : (source[key] || {});
  const input = num(row.input_tokens);
  const output = num(row.output_tokens);
  const cacheCreation = num(row.cache_write_tokens);
  const cacheRead = num(row.cache_read_tokens);
  const aggregateTotal = hourly ? hourBucketValue(totals, key) : num(totals[key]);
  const label = hourly ? String(key).padStart(2, '0') + ':00' : (String(key).length > 7 ? String(key).slice(5) : String(key));
  return {
    key: String(key),
    label,
    tooltipLabel: hourly ? label : String(key),
    input,
    output,
    cacheCreation,
    cacheRead,
    total: aggregateTotal > 0 ? aggregateTotal : input + output + cacheCreation + cacheRead,
    cacheRate: input > 0 ? cacheRead / input * 100 : 0,
  };
}

function tokenTrendPoints(panelData) {
  const usage = panelData && panelData.usage || {};
  const hourly = $('range').value === '7h' || $('range').value === '24h';
  const source = hourly ? usage.token_parts_by_hour || {} : usage.token_parts_by_day || {};
  const totals = hourly ? usage.tokens_by_hour || {} : usage.tokens_by_day || {};
  let keys = Object.keys(source);
  if (hourly) keys = orderedRecentHours(keys, dashboardCurrentHour(panelData));
  else {
    keys.sort();
    if ($('range').value === 'all' && keys.length > 30) keys = keys.slice(-30);
  }
  if (!keys.length) {
    keys = Object.keys(totals);
    if (hourly) keys = orderedRecentHours(keys, dashboardCurrentHour(panelData));
    else {
      keys.sort();
      if ($('range').value === 'all' && keys.length > 30) keys = keys.slice(-30);
    }
    return keys.map((key) => {
      const total = hourly ? hourBucketValue(totals, key) : num(totals[key]);
      const label = hourly ? String(key).padStart(2, '0') + ':00' : (String(key).length > 7 ? String(key).slice(5) : String(key));
      return { key: String(key), label, tooltipLabel: hourly ? label : String(key), input: total, output: 0, cacheCreation: 0, cacheRead: 0, total, cacheRate: 0 };
    });
  }
  return keys.map((key) => tokenTrendPoint(key, hourly, source, totals));
}

function tokenTrendSeriesDefinitions() {
  return tokenTrendSeriesCatalog.map((item) => Object.assign({}, item, { label: t(item.labelKey) }));
}

function tokenTrendVisibilityState() {
  if (tokenTrendVisibility) return tokenTrendVisibility;
  tokenTrendVisibility = {};
  tokenTrendSeriesCatalog.forEach((item) => { tokenTrendVisibility[item.key] = true; });
  try {
    const saved = JSON.parse(localStorage.getItem(tokenTrendVisibilityStorageKey) || 'null');
    if (saved && typeof saved === 'object') {
      tokenTrendSeriesCatalog.forEach((item) => {
        if (typeof saved[item.key] === 'boolean') tokenTrendVisibility[item.key] = saved[item.key];
      });
    }
  } catch (e) { /* ignore unavailable or malformed local storage */ }
  return tokenTrendVisibility;
}

function saveTokenTrendVisibility() {
  try {
    localStorage.setItem(tokenTrendVisibilityStorageKey, JSON.stringify(tokenTrendVisibilityState()));
  } catch (e) { /* ignore unavailable local storage */ }
}

function tokenTrendSeriesEnabled(key) {
  return tokenTrendVisibilityState()[key] !== false;
}

function setTokenTrendSeriesVisible(key, visible) {
  const item = tokenTrendSeriesCatalog.find((series) => series.key === key);
  if (!item) return;
  const state = tokenTrendVisibilityState();
  state[key] = typeof visible === 'boolean' ? visible : !state[key];
  saveTokenTrendVisibility();
  renderTokenUsageChart(dashboardPanelData());
}

function tokenTrendTooltip(point, activeSeries) {
  const metric = function (label, value, color) {
    return '<div class="tooltipRow"><span><i class="tooltipSwatch" style="background:' + color + '"></i>' + esc(label) + '</span><strong>' + esc(formatInteger(value)) + '</strong></div>';
  };
  const series = activeSeries || tokenTrendSeriesDefinitions().filter((item) => tokenTrendSeriesEnabled(item.key));
  const rows = series.map((item) => item.rate
    ? '<div class="tooltipRow"><span><i class="tooltipSwatch" style="background:' + item.color + '"></i>' + esc(item.label) + '</span><strong>' + esc(pct(point.cacheRate)) + '</strong></div>'
    : metric(item.label, point[item.key], item.color)).join('');
  return '<div class="tooltipTitle">' + esc(point.tooltipLabel || point.label) + '</div>' +
    '<div class="tooltipGrid">' + rows +
    '<div class="tooltipRow tooltipTotal"><span>' + esc(t('token_total')) + '</span><strong>' + esc(formatInteger(point.total)) + '</strong></div>' +
    '</div>';
}

function tokenTrendHover(target, active) {
  const svg = target && (target.ownerSVGElement || target.parentNode);
  if (!svg || typeof svg.querySelector !== 'function') return;
  const line = svg.querySelector('.tokenTrendHoverLine');
  if (!line) return;
  if (!active || !target || typeof target.getAttribute !== 'function') {
    line.setAttribute('visibility', 'hidden');
    return;
  }
  const x = target.getAttribute('data-trend-x');
  if (!x) {
    line.setAttribute('visibility', 'hidden');
    return;
  }
  line.setAttribute('x1', x);
  line.setAttribute('x2', x);
  line.setAttribute('visibility', 'visible');
}

function tokenTrendMonotonePath(points, valueFn, x, y) {
  if (!points.length) return '';
  const coords = points.map((point, index) => ({
    x: x(index),
    y: y(valueFn(point)),
  }));
  if (coords.length === 1) return 'M' + coords[0].x.toFixed(2) + ' ' + coords[0].y.toFixed(2);

  const count = coords.length;
  const h = [];
  const delta = [];
  for (let i = 0; i < count - 1; i++) {
    h[i] = coords[i + 1].x - coords[i].x;
    delta[i] = h[i] ? (coords[i + 1].y - coords[i].y) / h[i] : 0;
  }
  const tangent = Array(count).fill(0);
  if (count === 2) {
    tangent[0] = delta[0];
    tangent[1] = delta[0];
  } else {
    const endpointSlope = function (h0, h1, d0, d1) {
      let slope = ((2 * h0 + h1) * d0 - h0 * d1) / (h0 + h1);
      if (Math.sign(slope) !== Math.sign(d0)) slope = 0;
      else if (Math.sign(d0) !== Math.sign(d1) && Math.abs(slope) > Math.abs(3 * d0)) slope = 3 * d0;
      return slope;
    };
    tangent[0] = endpointSlope(h[0], h[1], delta[0], delta[1]);
    tangent[count - 1] = endpointSlope(h[count - 2], h[count - 3], delta[count - 2], delta[count - 3]);
    for (let i = 1; i < count - 1; i++) {
      if (delta[i - 1] * delta[i] <= 0) tangent[i] = 0;
      else tangent[i] = (h[i - 1] + h[i]) / ((h[i - 1] + 2 * h[i]) / delta[i - 1] + (2 * h[i - 1] + h[i]) / delta[i]);
    }
  }
  for (let i = 0; i < count - 1; i++) {
    if (delta[i] === 0) {
      tangent[i] = 0;
      tangent[i + 1] = 0;
      continue;
    }
    const a = tangent[i] / delta[i];
    const b = tangent[i + 1] / delta[i];
    const norm = a * a + b * b;
    if (norm > 9) {
      const scale = 3 / Math.sqrt(norm);
      tangent[i] = scale * a * delta[i];
      tangent[i + 1] = scale * b * delta[i];
    }
  }

  let path = 'M' + coords[0].x.toFixed(2) + ' ' + coords[0].y.toFixed(2);
  for (let i = 0; i < count - 1; i++) {
    const third = h[i] / 3;
    path += ' C' + (coords[i].x + third).toFixed(2) + ' ' + (coords[i].y + tangent[i] * third).toFixed(2)
      + ' ' + (coords[i + 1].x - third).toFixed(2) + ' ' + (coords[i + 1].y - tangent[i + 1] * third).toFixed(2)
      + ' ' + coords[i + 1].x.toFixed(2) + ' ' + coords[i + 1].y.toFixed(2);
  }
  return path;
}

function tokenTrendAreaPath(points, valueFn, x, y, baseline) {
  if (!points.length) return '';
  const line = tokenTrendMonotonePath(points, valueFn, x, y);
  return line + ' L' + x(points.length - 1).toFixed(2) + ' ' + baseline.toFixed(2)
    + ' L' + x(0).toFixed(2) + ' ' + baseline.toFixed(2) + ' Z';
}

function renderTokenUsageChart(panelData) {
  const svg = $('tokenUsageTrend');
  const legend = $('tokenTrendLegend');
  if (!svg || !legend) return;
  const points = tokenTrendPoints(panelData);
  const series = tokenTrendSeriesDefinitions();
  const activeSeries = series.filter((item) => tokenTrendSeriesEnabled(item.key));
  const tokenSeries = activeSeries.filter((item) => !item.rate);
  const rateEnabled = activeSeries.some((item) => item.rate);
  legend.innerHTML = series.map((item) => {
    const enabled = tokenTrendSeriesEnabled(item.key);
    return '<button type="button" class="tokenTrendLegendItem' + (enabled ? '' : ' is-muted') + '" data-token-series="' + item.key + '" aria-pressed="' + String(enabled) + '" aria-label="' + esc(item.label) + '" title="' + esc(item.label) + '"><span class="distributionLegendDot" style="background:' + item.color + '"></span><span>' + esc(item.label) + '</span></button>';
  }).join('');
  legend.onclick = function (event) {
    const target = event && event.target;
    const button = target && typeof target.closest === 'function' ? target.closest('[data-token-series]') : target;
    if (!button || typeof button.getAttribute !== 'function') return;
    setTokenTrendSeriesVisible(button.getAttribute('data-token-series'));
  };
  if (!points.length) {
    svg.setAttribute('viewBox', '0 0 720 190');
    svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" class="distributionAxisText">' + esc(t('distribution_empty')) + '</text>';
    bindDashboardTooltip(svg);
    return;
  }
  const W = 720, H = 190, PL = 42, PR = 42, PT = 16, PB = 28;
  const chartW = W - PL - PR, chartH = H - PT - PB;
  const axisSeries = tokenSeries.length ? tokenSeries : series.filter((item) => !item.rate);
  const maxTokens = Math.max(1, ...points.flatMap((point) => axisSeries.map((item) => point[item.key])));
  const count = Math.max(points.length, 2);
  const x = (index) => PL + (index + 0.5) * chartW / count;
  const yToken = (value) => PT + chartH - value / maxTokens * chartH;
  const yRate = (value) => PT + chartH - value / 100 * chartH;
  let html = '';
  for (let i = 0; i <= 3; i++) {
    const y = PT + chartH - i / 3 * chartH;
    html += '<line x1="' + PL + '" x2="' + (W - PR) + '" y1="' + y + '" y2="' + y + '" class="distributionAxisLine"/><text x="' + (PL - 6) + '" y="' + (y + 3) + '" text-anchor="end" class="distributionAxisText">' + esc(compact(maxTokens * i / 3)) + '</text>' + (rateEnabled ? '<text x="' + (W - PR + 6) + '" y="' + (y + 3) + '" class="distributionAxisText">' + (i * 100 / 3).toFixed(0) + '%</text>' : '');
  }
  tokenSeries.forEach((item) => {
    const valueFn = (point) => point[item.key];
    const path = tokenTrendMonotonePath(points, valueFn, x, yToken);
    const area = tokenTrendAreaPath(points, valueFn, x, yToken, PT + chartH);
    html += '<path d="' + area + '" class="tokenTrendArea" data-token-series="' + item.key + '" fill="' + item.color + '" fill-opacity="0.1" stroke="none"></path>';
    html += '<path d="' + path + '" class="distributionLine tokenTrendSeries" data-token-series="' + item.key + '" stroke="' + item.color + '"></path>';
  });
  if (rateEnabled) {
    const ratePath = tokenTrendMonotonePath(points, (point) => point.cacheRate, x, yRate);
    html += '<path d="' + ratePath + '" class="distributionLine tokenTrendRate" data-token-series="cacheRate" stroke="#a855f7" stroke-dasharray="4 3"></path>';
  }
  html += '<line x1="' + x(0) + '" x2="' + x(0) + '" y1="' + PT + '" y2="' + (PT + chartH) + '" class="tokenTrendHoverLine" visibility="hidden"/>';
  const hitWidth = chartW / count;
  points.forEach((point, index) => {
    const tooltip = esc(tokenTrendTooltip(point, activeSeries));
    const hitX = Math.max(PL, x(index) - hitWidth / 2);
    html += '<rect class="tokenTrendHit" x="' + hitX + '" y="' + PT + '" width="' + hitWidth + '" height="' + chartH + '" data-trend-x="' + x(index) + '" data-tooltip="' + tooltip + '" tabindex="0" focusable="true" role="img" aria-label="' + esc(point.tooltipLabel || point.label) + ' ' + esc(t('token_total')) + ' ' + esc(formatInteger(point.total)) + '"/>';
  });
  points.forEach((point, index) => {
    const tooltip = esc(tokenTrendTooltip(point, activeSeries));
    tokenSeries.forEach((item) => { html += '<circle cx="' + x(index) + '" cy="' + yToken(point[item.key]) + '" r="3" class="distributionPoint tokenTrendPoint" data-token-series="' + item.key + '" data-trend-x="' + x(index) + '" data-tooltip="' + tooltip + '" tabindex="0" focusable="true" role="img" fill="' + item.color + '"><title>' + esc(point.label + ' ' + item.label + ': ' + compact(point[item.key])) + '</title></circle>'; });
    if (rateEnabled) html += '<circle cx="' + x(index) + '" cy="' + yRate(point.cacheRate) + '" r="3" class="distributionPoint tokenTrendPoint" data-token-series="cacheRate" data-trend-x="' + x(index) + '" data-tooltip="' + tooltip + '" tabindex="0" focusable="true" role="img" fill="#a855f7"><title>' + esc(point.label + ' ' + t('token_cache_rate') + ': ' + pct(point.cacheRate)) + '</title></circle>';
    if (index % Math.max(1, Math.ceil(points.length / 10)) === 0 || index === points.length - 1) html += '<text x="' + x(index) + '" y="' + (H - 6) + '" text-anchor="middle" class="distributionAxisText">' + esc(point.label) + '</text>';
  });
  svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  svg.innerHTML = html;
  bindDashboardTooltip(svg, tokenTrendHover);
}

function renderDistributionDashboard() {
  const panelData = dashboardPanelData();
  const containers = ['modelDistributionDonut', 'upstreamDistributionDonut', 'endpointDistributionDonut', 'tokenUsageTrend'];
  if (!panelData) {
    containers.forEach((id) => { const element = $(id); if (element) element.innerHTML = ''; });
    const legend = $('tokenTrendLegend');
    if (legend) legend.innerHTML = '';
    return;
  }
  const rows = distributionRowsForPanel(panelData);
  renderDistributionDonut('modelDistributionDonut', 'modelDistributionTotal', rows.modelRows);
  renderDistributionTable('modelDistribution', rows.modelRows);
  renderDistributionDonut('upstreamDistributionDonut', 'upstreamDistributionTotal', rows.upstreamRows);
  renderDistributionTable('upstreamDistribution', rows.upstreamRows);
  renderDistributionDonut('endpointDistributionDonut', 'endpointDistributionTotal', rows.endpointRows);
  renderDistributionTable('endpointDistribution', rows.endpointRows);
  renderTokenUsageChart(panelData);
}

function renderTrendChart() {
  var panelData = dashboardPanelData();
  var usage = panelData && panelData.usage;
  if (!usage) { $('trendChart').innerHTML = '<text x="50%" y="50%" text-anchor="middle" class="trendAxisText">' + (filteredSummaryError ? t('client_api_filter_failed') : t('no_trend_data')) + '</text>'; clearAnomalyBar(); return }

  var range = $('range').value;
  var useHourly = (range === '7h' || range === '24h');
  var color, barColor;
  if (trendMetric === 'cost') { color = '#f59e0b'; barColor = 'rgba(245,158,11,0.18)'; }
  else if (trendMetric === 'tokens') { color = '#8b5cf6'; barColor = 'rgba(139,92,246,0.18)'; }
  else if (trendMetric === 'rpm') { color = '#22c55e'; barColor = 'rgba(34,197,94,0.18)'; }
  else { color = '#3b82f6'; barColor = 'rgba(59,130,246,0.18)'; }

  // ---- hourly mode (7h / 24h) ----
  if (useHourly) {
    var reqHour = usage.requests_by_hour || {};
    var tokHour = usage.tokens_by_hour || {};
    var costHour = usage.cost_by_hour || {};
    var hours = Object.keys(reqHour).concat(Object.keys(tokHour)).concat(Object.keys(costHour));
    var hourSet = new Set();
    hours.forEach(function(k) { hourSet.add(k); });
    var ordered = orderedRecentHours(Array.from(hourSet), dashboardCurrentHour(panelData));
    if (!ordered.length) { $('trendChart').innerHTML = '<text x="50%" y="50%" text-anchor="middle" class="trendAxisText">' + t('no_trend_data') + '</text>'; clearAnomalyBar(); return }

    var totalCost = 0, totalToks = 0;
    (panelData.model_stats || []).forEach(function(r) {
      var t = num(r.total_tokens); if (t > 0) totalToks += t;
      var c = aggregateCost(r, modelPrices, manualModelPrices); if (Number.isFinite(c)) totalCost += c;
    });
    var blendedPrice = totalToks > 0 ? totalCost / totalToks * 1e6 : 0;

    var points = ordered.map(function(h) {
      var reqs = hourBucketValue(reqHour, h);
      var toks = hourBucketValue(tokHour, h);
      var cost = hourBucketValue(costHour, h);
      var hasCost = Object.prototype.hasOwnProperty.call(costHour, String(h).padStart(2, '0')) || Object.prototype.hasOwnProperty.call(costHour, String(h));
      var label = String(h).padStart(2, '0') + ':00';
      return { label: label, requests: reqs, tokens: toks, cost: hasCost ? cost : (blendedPrice && toks ? toks / 1e6 * blendedPrice : 0), rpm: reqs / 60 };
    });

    return renderTrendSvg(points, range, color, barColor, 'hour');
  }

  // ---- daily mode (7d / all) ----
  var tokensDay = usage.tokens_by_day || {};
  var requestsDay = usage.requests_by_day || {};
  var costDay = usage.cost_by_day || {};
  var allDays = new Set();
  Object.keys(tokensDay).forEach(function(k) { allDays.add(k); });
  Object.keys(requestsDay).forEach(function(k) { allDays.add(k); });
  Object.keys(costDay).forEach(function(k) { allDays.add(k); });
  var ordered = Array.from(allDays).sort();
  if (range === 'all' && ordered.length > 30) ordered = ordered.slice(-30);
  if (!ordered.length) { $('trendChart').innerHTML = '<text x="50%" y="50%" text-anchor="middle" class="trendAxisText">' + t('no_trend_data') + '</text>'; clearAnomalyBar(); return }

  var totalCost = 0, totalToks = 0;
  (panelData.model_stats || []).forEach(function(r) {
    var t = num(r.total_tokens); if (t > 0) totalToks += t;
    var c = aggregateCost(r, modelPrices, manualModelPrices); if (Number.isFinite(c)) totalCost += c;
  });
  var blendedPrice = totalToks > 0 ? totalCost / totalToks * 1e6 : 0;

  var points = ordered.map(function(day) {
    var reqs = num(requestsDay[day]);
    var toks = num(tokensDay[day]);
    var hasCost = Object.prototype.hasOwnProperty.call(costDay, day);
    return { label: day.length > 7 ? day.slice(5) : day, requests: reqs, tokens: toks, cost: hasCost ? num(costDay[day]) : (blendedPrice && toks ? toks / 1e6 * blendedPrice : 0), rpm: reqs / 1440 };
  });

  renderTrendSvg(points, range, color, barColor, 'day');
}

function clearAnomalyBar() {
  var bar = $('anomalyBar');
  if (!bar) return;
  bar.className = 'anomalyBar';
  bar.innerHTML = '';
}

function trendHover(target, active) {
  var svg = target && (target.ownerSVGElement || target.parentNode);
  if (!svg || typeof svg.querySelector !== 'function') return;
  var line = svg.querySelector('.trendHoverLine');
  if (!line) return;
  if (!active || !target || typeof target.getAttribute !== 'function') {
    line.setAttribute('visibility', 'hidden');
    return;
  }
  var x = target.getAttribute('data-trend-x');
  if (!x) {
    line.setAttribute('visibility', 'hidden');
    return;
  }
  line.setAttribute('x1', x);
  line.setAttribute('x2', x);
  line.setAttribute('visibility', 'visible');
}

function trendMetricLabel(metric) {
  switch (metric) {
    case 'requests': return t('trend_daily_requests') || t('col_requests');
    case 'tokens': return t('trend_daily_tokens') || t('col_tokens');
    case 'rpm': return t('trend_daily_rpm') || t('rpm');
    case 'cost': return t('trend_daily_cost') || t('distribution_cost');
    default: return t('col_tokens');
  }
}

function renderTrendSvg(points, range, color, barColor, mode) {
  var valueFn, formatVal;
  if (trendMetric === 'requests') {
    valueFn = function(p) { return p.requests; }; formatVal = function(v) { return formatInteger(Math.round(v)); };
  } else if (trendMetric === 'tokens') {
    valueFn = function(p) { return p.tokens; }; formatVal = function(v) { return compact(v); };
  } else if (trendMetric === 'rpm') {
    valueFn = function(p) { return p.rpm; }; formatVal = function(v) { return Number(v).toFixed(1); };
  } else {
    valueFn = function(p) { return p.cost; }; formatVal = function(v) { return formatUsd(v); };
  }

  var svg = $('trendChart');
  if (!svg) return;

  var values = points.map(valueFn);
  var realMax = values.length ? Math.max.apply(null, values) : 0;
  var maxVal = Math.max.apply(null, values.concat([1]));
  var minVal = Math.min.apply(null, values.concat([0]));
  var sumVal = values.reduce(function(a, b) { return a + b; }, 0);
  var avgVal = points.length ? sumVal / points.length : 0;

  var badgesEl = $('trendBadges');
  if (badgesEl) {
    badgesEl.innerHTML =
      '<div class="trendBadge">峰值 Peak: <strong>' + esc(formatVal(realMax)) + '</strong></div>' +
      '<div class="trendBadge">均值 Avg: <strong>' + esc(formatVal(avgVal)) + '</strong></div>';
  }

  var n = points.length;
  if (n < 2) n = 2;

  var W = 1200, H = 240, PL = 70, PR = 20, PT = 24, PB = 36;
  var chartW = W - PL - PR, chartH = H - PT - PB;

  var x = function(index) { return PL + (index + 0.5) * (chartW / n); };
  var yFn = function(value) { return PT + chartH - (maxVal > 0 ? (value / maxVal) * chartH : 0); };

  var gradId = 'trendGradientDynamic';
  var html = '<defs>' +
    '<linearGradient id="' + gradId + '" x1="0" y1="0" x2="0" y2="1">' +
      '<stop offset="0%" stop-color="' + color + '" stop-opacity="0.35"/>' +
      '<stop offset="100%" stop-color="' + color + '" stop-opacity="0.0"/>' +
    '</linearGradient>' +
  '</defs>';

  // Y axis grid lines
  var ySteps = 4;
  for (var i = 0; i <= ySteps; i++) {
    var yVal = minVal + (maxVal - minVal) * i / ySteps;
    var y = PT + chartH - (i / ySteps) * chartH;
    html += '<line x1="' + PL + '" x2="' + (W - PR) + '" y1="' + y + '" y2="' + y + '" class="trendAxisLine"/>';
    html += '<text x="' + (PL - 8) + '" y="' + (y + 4) + '" text-anchor="end" class="trendAxisText">' + esc(formatVal(yVal)) + '</text>';
  }

  // Hermite Spline smooth curve & area path
  var pathD = tokenTrendMonotonePath(points, valueFn, x, yFn);
  var areaD = tokenTrendAreaPath(points, valueFn, x, yFn, PT + chartH);

  html += '<path d="' + areaD + '" class="trendArea" fill="url(#' + gradId + ')" stroke="none"/>';
  html += '<path d="' + pathD + '" class="trendLine" fill="none" stroke="' + color + '" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>';

  // Crosshair hover line
  html += '<line x1="' + x(0) + '" x2="' + x(0) + '" y1="' + PT + '" y2="' + (PT + chartH) + '" class="trendHoverLine" visibility="hidden"/>';

  // Invisible hover rects
  var hitWidth = chartW / n;
  var metricName = trendMetricLabel(trendMetric);
  points.forEach(function(p, i) {
    var v = valueFn(p);
    var valStr = formatVal(v);
    var tooltipHtml = '<div class="tooltipTitle">' + esc(p.label) + '</div>' +
      '<div class="tooltipGrid">' +
      '<div class="tooltipRow"><span><i class="tooltipSwatch" style="background:' + color + '"></i>' + esc(metricName) + '</span><strong>' + esc(valStr) + '</strong></div>' +
      '</div>';
    var tooltip = esc(tooltipHtml);
    var hitX = Math.max(PL, x(i) - hitWidth / 2);
    html += '<rect class="trendHit" x="' + hitX + '" y="' + PT + '" width="' + hitWidth + '" height="' + chartH + '" data-trend-x="' + x(i) + '" data-tooltip="' + tooltip + '" tabindex="0" focusable="true" role="img" aria-label="' + esc(p.label + ': ' + valStr) + '"/>';
  });

  // Points & X axis labels
  var xStep = mode === 'hour' ? Math.max(1, Math.ceil(n / 12)) : Math.max(1, Math.ceil(n / 14));
  points.forEach(function(p, i) {
    var v = valueFn(p);
    var valStr = formatVal(v);
    var tooltipHtml = '<div class="tooltipTitle">' + esc(p.label) + '</div>' +
      '<div class="tooltipGrid">' +
      '<div class="tooltipRow"><span><i class="tooltipSwatch" style="background:' + color + '"></i>' + esc(metricName) + '</span><strong>' + esc(valStr) + '</strong></div>' +
      '</div>';
    var tooltip = esc(tooltipHtml);
    var cx = x(i);
    var cy = yFn(v);
    html += '<circle cx="' + cx + '" cy="' + cy + '" r="4" class="trendPoint" data-trend-x="' + cx + '" data-tooltip="' + tooltip + '" fill="var(--cpa-bg-surface)" stroke="' + color + '" stroke-width="2.5" tabindex="0" focusable="true" role="img" aria-label="' + esc(p.label + ': ' + valStr) + '"><title>' + esc(p.label + ': ' + valStr) + '</title></circle>';

    if (i % xStep === 0 || i === n - 1) {
      html += '<text x="' + cx + '" y="' + (H - 6) + '" text-anchor="middle" class="trendAxisText">' + esc(p.label) + '</text>';
    }
  });

  svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  svg.innerHTML = html;
  bindDashboardTooltip(svg, trendHover);

  clearAnomalyBar();
}

function initTrendChart() {
  var select = $('trendMetric');
  var options = [
    { value: 'cost', label: t('trend_daily_cost') },
    { value: 'requests', label: t('trend_daily_requests') },
    { value: 'tokens', label: t('trend_daily_tokens') },
    { value: 'rpm', label: t('trend_daily_rpm') }
  ];
  select.innerHTML = options.map(function(o) { return '<option value="' + o.value + '"' + (trendMetric === o.value ? ' selected' : '') + '>' + o.label + '</option>'; }).join('');
  select.value = trendMetric;
  select.onchange = function() { trendMetric = select.value; renderTrendChart(); };
}

function renderFilters() {
  if (!summaryData) return;
  const models = modelNames();
  const sources = (summaryData.source_stats || []).map(s => s.source);
  const summaryAuthIndexes = (summaryData.credential_stats || []).map((s) => s.auth_index).filter((v) => v && v !== '(空)');
  const pageAuthIndexes = eventsData && eventsData.events ? eventsData.events.map((d) => d.auth_index || '').filter(Boolean) : [];
  const authIndexes = [...new Set([...summaryAuthIndexes, ...pageAuthIndexes])].sort();
  const fill = (id, emptyLabel, values) => { const old = $(id).value; $(id).innerHTML = '<option value="">' + emptyLabel + '</option>' + values.map((v) => '<option value="' + esc(v) + '">' + esc(v) + '</option>').join(''); $(id).value = [...values, ''].includes(old) ? old : '' };
  fill('filterModel', t('filter_all_models'), models);
  fill('filterSource', t('filter_all_sources'), sources);
  fill('filterAuth', t('filter_all_credentials'), authIndexes);
}

function normalizeEventsPayload(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return { events: [], total: 0, limit: eventsLimit, offset: 0 };
  }
  return {
    events: Array.isArray(data.events) ? data.events : [],
    total: data.total || 0,
    limit: data.limit || eventsLimit,
    offset: data.offset || 0,
  };
}

function renderEventsContent() {
  const data = normalizeEventsPayload(eventsData);
  const rows = data.events;
  const total = data.total;
  setText('eventsCount', t('events_count', formatInteger(total), formatInteger(Math.min(rows.length, eventsLimit))));
  $('events').innerHTML = rows.length ? '<table><thead><tr><th>' + t('col_time') + '</th><th>' + t('col_model') + '</th><th>' + t('col_source') + '</th><th>' + t('col_credential') + '</th><th>' + t('col_result') + '</th><th>' + t('col_latency') + '</th><th>' + t('col_input') + '</th><th>' + t('col_output') + '</th><th>' + t('col_thinking') + '</th><th>' + t('col_cache') + '</th><th>' + t('col_cache_write') + '</th><th>' + t('col_total') + '</th></tr></thead><tbody>' + rows.map((d) => '<tr><td>' + formatDateTime(timestampMs(d.timestamp)) + '</td><td class="nameCell">' + esc(d.model) + '</td><td class="nameCell">' + esc(sourceLabel(d)) + '</td><td>' + esc(d.auth_index || '-') + '</td><td class="' + (d.failed ? 'bad' : 'ok') + '">' + statusText(d.failed) + '</td><td>' + formatDurationAndTTFT(d.latency_ms, d.ttft_ms) + '</td><td>' + formatInteger(uncachedInputTokens(d)) + '</td><td>' + formatInteger(num(d.tokens && d.tokens.output_tokens)) + '</td><td>' + formatInteger(num(d.tokens && d.tokens.reasoning_tokens)) + '</td><td>' + formatInteger(cacheReadTokens(d.tokens)) + '</td><td>' + formatInteger(num(d.tokens && d.tokens.cache_write_tokens)) + '</td><td>' + formatInteger(totalTokens(d)) + '</td></tr>').join('') + '</tbody></table>' : '<div class="empty">' + t('no_events') + '</div>';
  const limit = Math.max(1, data.limit || eventsPageSize);
  const offset = Math.max(0, data.offset || eventsPageOffset);
  const pageCount = total > 0 ? Math.ceil(total / limit) : 0;
  const page = pageCount > 0 ? Math.floor(offset / limit) + 1 : 0;
  const start = total > 0 ? offset + 1 : 0;
  const end = total > 0 ? Math.min(offset + rows.length, total) : 0;
  setText('eventsPageLabel', t('events_page', start, end, total, page, pageCount));
  const previousButton = $('eventsPrev');
  const nextButton = $('eventsNext');
  if (previousButton) previousButton.disabled = offset <= 0;
  if (nextButton) nextButton.disabled = total === 0 || offset + rows.length >= total;
  if (previousButton) previousButton.onclick = () => {
    eventsPageOffset = Math.max(0, offset - limit);
    renderEvents();
  };
  if (nextButton) nextButton.onclick = () => {
    if (total === 0 || offset + rows.length >= total) return;
    eventsPageOffset = offset + limit;
    renderEvents();
  };
  renderFilters();
}

async function renderEvents() {
  const params = new URLSearchParams();
  params.set('limit', String(eventsLimit));
  params.set('offset', String(eventsPageOffset));
  params.set('range', $('range').value);
  const fm = $('filterModel').value; if (fm) params.set('model', fm);
  const fs = $('filterSource').value; if (fs) params.set('source', fs);
  const fa = $('filterAuth').value; if (fa) params.set('auth', fa);
  if (selectedClientApiSelector()) params.set('client_api', selectedClientApiSelector());
  try {
    const url = pluginEndpoint('dashboard-events') + '?' + params.toString();
    eventsData = normalizeEventsPayload(await fetchConditionalJsonPayload('dashboard-events:' + url, url, pluginFetchOptions({ cache: 'no-store' })));
    eventsDataUrl = url;
  } catch (e) {
    const url = pluginEndpoint('dashboard-events') + '?' + params.toString();
    if (!eventsData || eventsDataUrl !== url) {
      eventsData = { events: [], total: 0, limit: eventsLimit, offset: eventsPageOffset };
      eventsDataUrl = url;
    }
  }
  const normalized = normalizeEventsPayload(eventsData);
  const effectiveLimit = Math.max(1, normalized.limit || eventsPageSize);
  if (normalized.total > 0 && eventsPageOffset >= normalized.total) {
    eventsPageOffset = Math.floor((normalized.total - 1) / effectiveLimit) * effectiveLimit;
    if (eventsPageOffset !== normalized.offset) return renderEvents();
  }
  renderEventsContent();
}

const downloadBlobRevokeDelayMs = 60000;
function download(name, text, type) {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.rel = 'noopener';
  a.style.display = 'none';
  const parent = document.body || document.documentElement;
  if (parent && typeof parent.appendChild === 'function') parent.appendChild(a);
  a.click();
  setTimeout(() => {
    if (a.parentNode && typeof a.parentNode.removeChild === 'function') a.parentNode.removeChild(a);
    URL.revokeObjectURL(url);
  }, downloadBlobRevokeDelayMs);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createExportJob(params) {
  return fetchJsonPayload(managementEndpoint('dashboard-events-export-jobs') + '?' + params.toString(), pluginFetchOptions({ method: 'POST', cache: 'no-store' }));
}

async function getExportJob(id) {
  return fetchJsonPayload(managementEndpoint('dashboard-events-export-jobs?id=' + encodeURIComponent(id)), pluginFetchOptions({ cache: 'no-store' }));
}

async function deleteExportJob(id) {
  try {
    await fetchJsonPayload(managementEndpoint('dashboard-events-export-jobs?id=' + encodeURIComponent(id)), pluginFetchOptions({ method: 'DELETE', cache: 'no-store' }));
  } catch {}
}

async function waitForExportJob(job) {
  let current = job;
  for (let i = 0; i < 120; i++) {
    if (current && current.status === 'succeeded') return current;
    if (current && current.status === 'failed') throw new Error(current.error || t('export_job_failed'));
    await delay(i < 10 ? 250 : 1000);
    current = await getExportJob(job.id);
  }
  throw new Error(t('export_job_timeout'));
}

async function fetchExportJobResult(params) {
  const job = await createExportJob(params);
  if (!job || !job.id) throw new Error(t('export_no_id'));
  try {
    const completed = await waitForExportJob(job);
    const downloadPath = completed.download_path || ('dashboard-events-export-download?id=' + encodeURIComponent(job.id));
    return await fetchTextPayloadWithMeta(managementEndpoint(downloadPath), pluginFetchOptions({ cache: 'no-store' }));
  } finally {
    await deleteExportJob(job.id);
  }
}

function rowsCsv(rows) {
  const head = [t('col_time'), t('col_model'), t('col_source'), t('col_credential'), t('col_result'), 'latency_ms', 'ttft_ms', t('col_input') + ' token', t('col_output') + ' token', t('col_thinking') + ' token', t('col_cache') + ' token', t('col_cache_write') + ' token', t('col_total') + ' token', t('col_status'), t('error_stats')];
  return [head, ...rows.map((d) => [d.timestamp, d.model, sourceLabel(d), d.auth_index || '', statusText(d.failed), num(d.latency_ms), num(d.ttft_ms), uncachedInputTokens(d), num(d.tokens && d.tokens.output_tokens), num(d.tokens && d.tokens.reasoning_tokens), cacheReadTokens(d.tokens), num(d.tokens && d.tokens.cache_write_tokens), totalTokens(d), d.status_code || '', d.failure || ''])].map((row) => row.map((v) => '"' + String(v ?? '').replace(/"/g, '""') + '"').join(',')).join('\n');
}

function makeCounterRow(name) { return { model: name, total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0, latency: [], providerMap: new Map() } }
function mergeProviderStat(target, stat) {
  if (!target || !target.providerMap || !stat) return;
  const provider = String(stat.provider || '').trim();
  const key = provider.toLowerCase();
  const row = target.providerMap.get(key) || { provider, total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0 };
  row.total_requests += num(stat.total_requests);
  row.success_count += num(stat.success_count);
  row.failure_count += num(stat.failure_count);
  row.total_tokens += num(stat.total_tokens);
  row.input_tokens += num(stat.input_tokens);
  row.output_tokens += num(stat.output_tokens);
  row.cached_tokens += num(stat.cached_tokens);
  row.cache_write_tokens += num(stat.cache_write_tokens);
  row.reasoning_tokens += num(stat.reasoning_tokens);
  target.providerMap.set(key, row);
}
function addDetailProviderToCounter(row, d) {
  if (!row || !row.providerMap) return;
  const tokens = d.tokens || {};
  mergeProviderStat(row, {
    provider: d.provider,
    total_requests: 1,
    success_count: d.failed ? 0 : 1,
    failure_count: d.failed ? 1 : 0,
    total_tokens: totalTokens(d),
    input_tokens: num(tokens.input_tokens),
    output_tokens: num(tokens.output_tokens),
    cached_tokens: cacheTokenTotal(tokens),
    cache_write_tokens: num(tokens.cache_write_tokens),
    reasoning_tokens: num(tokens.reasoning_tokens),
  });
}
function addDetailToCounter(row, d) {
  const tokens = d.tokens || {};
  row.total_requests++;
  d.failed ? row.failure_count++ : row.success_count++;
  row.total_tokens += totalTokens(d);
  row.input_tokens += num(tokens.input_tokens);
  row.output_tokens += num(tokens.output_tokens);
  row.cached_tokens += cacheTokenTotal(tokens);
  row.cache_write_tokens += num(tokens.cache_write_tokens);
  row.reasoning_tokens += num(tokens.reasoning_tokens);
  if (num(d.latency_ms) > 0) row.latency.push(num(d.latency_ms));
  addDetailProviderToCounter(row, d);
}
function providerHasValues(provider) {
  return num(provider.total_requests) > 0 || num(provider.total_tokens) > 0 || num(provider.input_tokens) > 0 || num(provider.output_tokens) > 0 || num(provider.cached_tokens) > 0 || num(provider.cache_write_tokens) > 0 || num(provider.reasoning_tokens) > 0;
}
function finalizeCounterRow(row) {
  if (row.latency && row.latency.length) row.avg_latency_ms = row.latency.reduce((a, b) => a + b, 0) / row.latency.length;
  if (row.providerMap) {
    const providers = [...row.providerMap.values()].filter(providerHasValues);
    const summed = providers.reduce((acc, p) => mergeCounterRow(acc, p), { total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0 });
    const remainder = {
      provider: '',
      total_requests: Math.max(num(row.total_requests) - num(summed.total_requests), 0),
      success_count: Math.max(num(row.success_count) - num(summed.success_count), 0),
      failure_count: Math.max(num(row.failure_count) - num(summed.failure_count), 0),
      total_tokens: Math.max(num(row.total_tokens) - num(summed.total_tokens), 0),
      input_tokens: Math.max(num(row.input_tokens) - num(summed.input_tokens), 0),
      output_tokens: Math.max(num(row.output_tokens) - num(summed.output_tokens), 0),
      cached_tokens: Math.max(num(row.cached_tokens) - num(summed.cached_tokens), 0),
      cache_write_tokens: Math.max(num(row.cache_write_tokens) - num(summed.cache_write_tokens), 0),
      reasoning_tokens: Math.max(num(row.reasoning_tokens) - num(summed.reasoning_tokens), 0),
    };
    if (providerHasValues(remainder)) providers.push(remainder);
    providers.sort((a, b) => num(b.total_requests) - num(a.total_requests) || String(a.provider || '').localeCompare(String(b.provider || '')));
    if (providers.length) row.providers = providers;
  }
  delete row.latency;
  delete row.providerMap;
  return row;
}
function applySnapshotCounter(row, raw) {
  if (!raw || typeof raw !== 'object') return row;
  ['total_requests', 'success_count', 'failure_count', 'total_tokens', 'input_tokens', 'output_tokens', 'cached_tokens', 'cache_write_tokens', 'reasoning_tokens', 'avg_latency_ms'].forEach((field) => {
    if (Object.prototype.hasOwnProperty.call(raw, field)) row[field] = num(raw[field]);
  });
  return row;
}
function applySnapshotProviders(row, raw) {
  if (!row || !raw || !Array.isArray(raw.providers) || !raw.providers.length) return row;
  row.providerMap = new Map();
  raw.providers.forEach((provider) => mergeProviderStat(row, provider));
  return row;
}
function mergeCounterRow(target, row) {
  target.total_requests += num(row.total_requests);
  target.success_count += num(row.success_count);
  target.failure_count += num(row.failure_count);
  target.total_tokens += num(row.total_tokens);
  target.input_tokens += num(row.input_tokens);
  target.output_tokens += num(row.output_tokens);
  target.cached_tokens += num(row.cached_tokens);
  target.cache_write_tokens += num(row.cache_write_tokens);
  target.reasoning_tokens += num(row.reasoning_tokens);
  if (target.providerMap && Array.isArray(row.providers)) row.providers.forEach((provider) => mergeProviderStat(target, provider));
  if (target.providerMap && row.providerMap) row.providerMap.forEach((provider) => mergeProviderStat(target, provider));
  if (Array.isArray(target.latency) && Array.isArray(row.latency)) row.latency.forEach((value) => { if (num(value) > 0) target.latency.push(num(value)); });
  return target;
}
function mergeProviderRows(targetProviders, sourceProviders) {
  if (!Array.isArray(sourceProviders) || !sourceProviders.length) return Array.isArray(targetProviders) ? targetProviders : [];
  const providers = new Map();
  (Array.isArray(targetProviders) ? targetProviders : []).forEach((provider) => {
    const key = String((provider && provider.provider) || '').trim().toLowerCase();
    providers.set(key, { ...provider });
  });
  sourceProviders.forEach((provider) => {
    const key = String((provider && provider.provider) || '').trim().toLowerCase();
    const existing = providers.get(key);
    if (!existing) {
      providers.set(key, { ...provider });
      return;
    }
    mergeCounterRow(existing, provider);
  });
  return [...providers.values()].sort((a, b) => num(b.total_requests) - num(a.total_requests) || String(a.provider || '').localeCompare(String(b.provider || '')));
}
function mergeClientApiModelRow(target, source) {
  mergeCounterRow(target, source);
  target.providers = mergeProviderRows(target.providers, source && source.providers);
}
function mergeClientApiRow(target, source) {
  mergeCounterRow(target, source);
  if (!String(target.api_key || '').trim()) target.api_key = source && source.api_key || '';
  if (!String(target.api_key_hash || '').trim()) target.api_key_hash = source && source.api_key_hash || '';
  const models = new Map();
  (Array.isArray(target.models) ? target.models : []).forEach((model) => models.set(String(model && model.model || ''), { ...model }));
  (Array.isArray(source && source.models) ? source.models : []).forEach((model) => {
    const key = String(model && model.model || '');
    const existing = models.get(key);
    if (existing) mergeClientApiModelRow(existing, model);
    else models.set(key, { ...model });
  });
  target.models = [...models.values()].sort((a, b) => num(b.total_requests) - num(a.total_requests));
}
function coalesceLegacyHashlessClientApiStats(rows) {
  if (!Array.isArray(rows) || rows.length < 2) return Array.isArray(rows) ? rows : [];
  const byLabel = new Map();
  rows.forEach((row, index) => {
    const label = String((row && row.api_key) || '').trim();
    if (!label || !label.includes('******')) return;
    const group = byLabel.get(label) || { indices: [], hashes: new Set(), hasHashless: false };
    group.indices.push(index);
    const hash = String((row && row.api_key_hash) || '').trim();
    if (hash) group.hashes.add(hash);
    else group.hasHashless = true;
    byLabel.set(label, group);
  });
  const removed = new Set();
  byLabel.forEach((group) => {
    if (group.indices.length < 2) return;
    if (group.hashes.size > 1 && !group.hasHashless) return;
    let targetIndex = group.indices[0];
    group.indices.slice(1).forEach((index) => {
      if (num(rows[index] && rows[index].total_requests) > num(rows[targetIndex] && rows[targetIndex].total_requests)) {
        targetIndex = index;
        return;
      }
      if (num(rows[index] && rows[index].total_requests) === num(rows[targetIndex] && rows[targetIndex].total_requests) &&
        !String(rows[targetIndex] && rows[targetIndex].api_key_hash || '').trim() &&
        String(rows[index] && rows[index].api_key_hash || '').trim()) targetIndex = index;
    });
    const target = rows[targetIndex];
    group.indices.forEach((sourceIndex) => {
      if (sourceIndex === targetIndex) return;
      mergeClientApiRow(target, rows[sourceIndex]);
      removed.add(sourceIndex);
    });
    if (group.hashes.size === 0) target.api_key_hash = '';
    else if (group.hashes.size === 1) target.api_key_hash = Array.from(group.hashes)[0];
    else target.api_key_hash = '';
  });
  if (!removed.size) return rows;
  return rows.filter((_, index) => !removed.has(index));
}
function dashboardRangeCutoffMs(rangeKey, referenceMs) {
  const now = num(referenceMs) || Date.now();
  switch (rangeKey) {
    case '7h': return now - 7 * 60 * 60 * 1000;
    case '24h': return now - 24 * 60 * 60 * 1000;
    case '7d': return now - 7 * 24 * 60 * 60 * 1000;
    default: return 0;
  }
}
function detailMatchesRange(d, cutoffMs) {
  if (!cutoffMs) return true;
  const ms = timestampMs(d && d.timestamp);
  // Exclude details whose timestamp we cannot parse: 0 means "invalid date"
  // and blindly including them would inflate range-scoped aggregates.
  return ms > 0 && ms >= cutoffMs;
}
function incrementSeriesValue(values, key, amount) {
  values[key] = num(values[key]) + num(amount);
}
function detailModelName(fallback, d) {
  const model = String(d && d.model || '').trim();
  if (model) return model;
  const fallbackModel = String(fallback || '').trim();
  return fallbackModel || 'unknown';
}
function credentialStatKey(d) {
  const raw = d && d.auth_index;
  const value = raw == null ? '' : String(raw);
  return value || '(空)';
}
function addDetailToCredentialAgg(credentialAgg, d) {
  const key = credentialStatKey(d);
  const row = credentialAgg.get(key) || { auth_index: key, total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0 };
  row.total_requests++;
  d.failed ? row.failure_count++ : row.success_count++;
  row.total_tokens += totalTokens(d);
  credentialAgg.set(key, row);
}
function detailSeriesBucket(d) {
  const raw = String(d && d.timestamp || '');
  const ms = timestampMs(raw);
  if (!ms) return null;
  const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw);
  if (!hasZone) {
    const match = raw.match(/^(\d{4}-\d{2}-\d{2})T([01]\d|2[0-3])/);
    if (match) return { day: match[1], hour: match[2] };
  }
  const china = new Date(ms + dashboardTimeZoneOffsetMs);
  return { day: china.toISOString().slice(0, 10), hour: String(china.getUTCHours()).padStart(2, '0') };
}
function addDetailToUsageTotals(usage, d, latency) {
  const tokens = d.tokens || {};
  usage.total_requests++;
  d.failed ? usage.failure_count++ : usage.success_count++;
  usage.total_tokens += totalTokens(d);
  usage.input_tokens += num(tokens.input_tokens);
  usage.output_tokens += num(tokens.output_tokens);
  usage.cached_tokens += cacheTokenTotal(tokens);
  usage.cache_write_tokens += num(tokens.cache_write_tokens);
  usage.reasoning_tokens += num(tokens.reasoning_tokens);
  if (latency && num(d.latency_ms) > 0) latency.push(num(d.latency_ms));
}
function addDetailToUsageSeries(usage, d) {
  const bucket = detailSeriesBucket(d);
  if (!bucket) return;
  const tokens = totalTokens(d);
  const cost = detailCost(d, modelPrices, manualModelPrices);
  incrementSeriesValue(usage.requests_by_day, bucket.day, 1);
  incrementSeriesValue(usage.requests_by_hour, bucket.hour, 1);
  incrementSeriesValue(usage.tokens_by_day, bucket.day, tokens);
  incrementSeriesValue(usage.tokens_by_hour, bucket.hour, tokens);
  incrementSeriesValue(usage.cost_by_day, bucket.day, cost);
  incrementSeriesValue(usage.cost_by_hour, bucket.hour, cost);
  addDetailToTokenParts(usage, d, bucket);
}
function addDetailToTokenParts(usage, d, bucket) {
  if (!bucket) return;
  const addTokenParts = (values, key) => {
    const row = values[key] || { input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0 };
    row.input_tokens += num(d.tokens && d.tokens.input_tokens);
    row.output_tokens += num(d.tokens && d.tokens.output_tokens);
    row.cache_read_tokens += cacheReadTokens(d.tokens || {});
    row.cache_write_tokens += num(d.tokens && d.tokens.cache_write_tokens);
    row.reasoning_tokens += num(d.tokens && d.tokens.reasoning_tokens);
    values[key] = row;
  };
  addTokenParts(usage.token_parts_by_day, bucket.day);
  addTokenParts(usage.token_parts_by_hour, bucket.hour);
}
function addDetailToEndpointAgg(endpointAgg, d) {
  const endpoint = normalizeDashboardEndpoint(d && d.endpoint) || 'unknown';
  const row = endpointAgg.get(endpoint) || {
    endpoint,
    total_requests: 0,
    success_count: 0,
    failure_count: 0,
    total_tokens: 0,
    input_tokens: 0,
    output_tokens: 0,
    cached_tokens: 0,
    cache_write_tokens: 0,
    reasoning_tokens: 0,
    modelMap: new Map(),
  };
  const model = detailModelName('', d);
  const modelRow = row.modelMap.get(model) || makeCounterRow(model);
  addDetailToCounter(modelRow, d);
  row.modelMap.set(model, modelRow);
  row.total_requests++;
  d.failed ? row.failure_count++ : row.success_count++;
  row.total_tokens += totalTokens(d);
  row.input_tokens += num(d.tokens && d.tokens.input_tokens);
  row.output_tokens += num(d.tokens && d.tokens.output_tokens);
  row.cached_tokens += cacheTokenTotal(d.tokens || {});
  row.cache_write_tokens += num(d.tokens && d.tokens.cache_write_tokens);
  row.reasoning_tokens += num(d.tokens && d.tokens.reasoning_tokens);
  endpointAgg.set(endpoint, row);
}
function finalizeEndpointAgg(endpointAgg) {
  return [...endpointAgg.values()].map((row) => {
    row.models = [...row.modelMap.values()].map(finalizeCounterRow).sort((a, b) => num(b.total_requests) - num(a.total_requests) || String(a.model || '').localeCompare(String(b.model || '')));
    delete row.modelMap;
    return row;
  }).sort((a, b) => num(b.total_requests) - num(a.total_requests) || String(a.endpoint || '').localeCompare(String(b.endpoint || '')));
}
function tokenPartsFromCostSeries(values, hourly) {
  if (!values || typeof values !== 'object') return {};
  const result = {};
  Object.entries(values).forEach(([key, rows]) => {
    const parts = { input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0 };
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      parts.input_tokens += num(row && row.input_tokens);
      parts.output_tokens += num(row && row.output_tokens);
      parts.cache_read_tokens += num(row && row.cached_tokens);
      parts.cache_write_tokens += num(row && row.cache_write_tokens);
      parts.reasoning_tokens += num(row && row.reasoning_tokens);
    });
    result[hourly ? String(key).padStart(2, '0') : key] = parts;
  });
  return result;
}
function buildSummaryFromFullUsage(data, rangeKey) {
  data = requireObjectPayload(data, 'dashboard-data');
  const rawUsage = data.usage || {};
  const cutoffMs = dashboardRangeCutoffMs(rangeKey, timestampMs(data.generated_at) || Date.now());
  const rangeScoped = cutoffMs > 0;
  const usage = {
    total_requests: rangeScoped ? 0 : rawUsage.total_requests || 0,
    success_count: rangeScoped ? 0 : rawUsage.success_count || 0,
    failure_count: rangeScoped ? 0 : rawUsage.failure_count || 0,
    total_tokens: rangeScoped ? 0 : rawUsage.total_tokens || 0,
    input_tokens: 0,
    output_tokens: 0,
    cached_tokens: 0,
    cache_write_tokens: 0,
    reasoning_tokens: 0,
    avg_latency_ms: 0,
    apis: {},
    requests_by_day: rangeScoped ? {} : rawUsage.requests_by_day || {},
    requests_by_hour: rangeScoped ? {} : rawUsage.requests_by_hour || {},
    tokens_by_day: rangeScoped ? {} : rawUsage.tokens_by_day || {},
    tokens_by_hour: rangeScoped ? {} : rawUsage.tokens_by_hour || {},
    cost_by_day: rangeScoped ? {} : rawUsage.cost_by_day || {},
    cost_by_hour: rangeScoped ? {} : rawUsage.cost_by_hour || {},
    token_parts_by_day: {},
    token_parts_by_hour: {}
  };
  const modelAgg = new Map(), sourceAgg = new Map(), endpointAgg = new Map(), credentialAgg = new Map(), clientAgg = new Map();
  const latency = [];
  const healthDetails = [];
  Object.entries(rawUsage.apis || {}).forEach(([api, a]) => {
    const apiRow = { total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0, avg_latency_ms: 0, models: {}, latency: [] };
    const apiModelRows = new Map();
    Object.entries(a.models || {}).forEach(([model, m]) => {
      const modelRow = makeCounterRow(model);
      (m.details || []).forEach((d) => {
        d.model = rangeScoped ? detailModelName(model, d) : (d.model || model);
        healthDetails.push(d);
        if (!detailMatchesRange(d, cutoffMs)) return;
        const tokens = d.tokens || {};
        const cached = cacheTokenTotal(tokens);
        const detailModelRow = rangeScoped ? (apiModelRows.get(d.model) || makeCounterRow(d.model)) : modelRow;
        addDetailToCounter(detailModelRow, d);
        if (rangeScoped) apiModelRows.set(d.model, detailModelRow);
        addDetailToCounter(apiRow, d);
        if (rangeScoped) {
          addDetailToUsageTotals(usage, d, latency);
          addDetailToUsageSeries(usage, d);
        } else {
          usage.input_tokens += num(tokens.input_tokens);
          usage.output_tokens += num(tokens.output_tokens);
          usage.cached_tokens += cached;
          usage.cache_write_tokens += num(tokens.cache_write_tokens);
          usage.reasoning_tokens += num(tokens.reasoning_tokens);
          if (num(d.latency_ms) > 0) latency.push(num(d.latency_ms));
          addDetailToTokenParts(usage, d, detailSeriesBucket(d));
        }

        const src = sourceLabel(d);
        const sourceRow = sourceAgg.get(src) || { source: src, provider: d.provider || '', total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0 };
        sourceRow.total_requests++; d.failed ? sourceRow.failure_count++ : sourceRow.success_count++; sourceRow.total_tokens += totalTokens(d);
        sourceAgg.set(src, sourceRow);

        addDetailToEndpointAgg(endpointAgg, d);

        addDetailToCredentialAgg(credentialAgg, d);

        const clientKey = clientApiGroupKey(d);
        const clientRow = clientAgg.get(clientKey) || { api_key: clientApiLabel(d), api_key_hash: d.api_key_hash || '', total_requests: 0, success_count: 0, failure_count: 0, total_tokens: 0, input_tokens: 0, output_tokens: 0, cached_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0, modelMap: new Map() };
        clientRow.total_requests++; d.failed ? clientRow.failure_count++ : clientRow.success_count++; clientRow.total_tokens += totalTokens(d); clientRow.input_tokens += num(tokens.input_tokens); clientRow.output_tokens += num(tokens.output_tokens); clientRow.cached_tokens += cached; clientRow.cache_write_tokens += num(tokens.cache_write_tokens); clientRow.reasoning_tokens += num(tokens.reasoning_tokens);
        const clientModel = clientRow.modelMap.get(d.model) || makeCounterRow(d.model);
        addDetailToCounter(clientModel, d);
        clientRow.modelMap.set(d.model, clientModel);
        clientAgg.set(clientKey, clientRow);
      });
      if (rangeScoped) return;
      applySnapshotCounter(modelRow, m);
      applySnapshotProviders(modelRow, m);
      const globalModel = modelAgg.get(model) || makeCounterRow(model);
      mergeCounterRow(globalModel, modelRow);
      modelAgg.set(model, globalModel);
      apiRow.models[model] = finalizeCounterRow(modelRow);
    });
    if (rangeScoped) {
      apiModelRows.forEach((modelRow, model) => {
        const globalModel = modelAgg.get(model) || makeCounterRow(model);
        mergeCounterRow(globalModel, modelRow);
        modelAgg.set(model, globalModel);
        const finalizedModel = finalizeCounterRow(modelRow);
        if (!finalizedModel.total_requests) return;
        apiRow.models[model] = finalizedModel;
      });
    }
    const finalizedAPI = finalizeCounterRow(rangeScoped ? apiRow : applySnapshotCounter(apiRow, a));
    if (!rangeScoped || finalizedAPI.total_requests) usage.apis[api] = finalizedAPI;
  });
  if (!rangeScoped) applySnapshotCounter(usage, rawUsage);
  if (!rangeScoped && Object.prototype.hasOwnProperty.call(rawUsage, 'token_parts_by_day')) usage.token_parts_by_day = rawUsage.token_parts_by_day || {};
  if (!rangeScoped && Object.prototype.hasOwnProperty.call(rawUsage, 'token_parts_by_hour')) usage.token_parts_by_hour = rawUsage.token_parts_by_hour || {};
  if (!rangeScoped && !Object.keys(usage.token_parts_by_day).length) usage.token_parts_by_day = tokenPartsFromCostSeries(rawUsage.cost_tokens_by_day, false);
  if (!rangeScoped && !Object.keys(usage.token_parts_by_hour).length) usage.token_parts_by_hour = tokenPartsFromCostSeries(rawUsage.cost_tokens_by_hour, true);
  usage.avg_latency_ms = latency.length ? latency.reduce((a, b) => a + b, 0) / latency.length : 0;
  if (!rangeScoped && Object.prototype.hasOwnProperty.call(rawUsage, 'avg_latency_ms')) usage.avg_latency_ms = num(rawUsage.avg_latency_ms);
  const credentialStats = [...credentialAgg.values()].sort((a, b) => b.total_requests - a.total_requests);
  const endpointStats = endpointAgg.size ? finalizeEndpointAgg(endpointAgg) : (Array.isArray(data.endpoint_stats) ? data.endpoint_stats : []);
  return {
    usage,
    health_grid: buildHealthGridFromDetails(healthDetails, data.generated_at),
    source_stats: [...sourceAgg.values()].sort((a, b) => b.total_requests - a.total_requests),
    endpoint_stats: endpointStats,
    credential_stats: rangeScoped ? credentialStats : [],
    client_api_stats: coalesceLegacyHashlessClientApiStats([...clientAgg.values()].map((r) => { r.models = [...r.modelMap.values()].map(finalizeCounterRow).sort((a, b) => b.total_requests - a.total_requests); delete r.modelMap; return r })).sort((a, b) => b.total_requests - a.total_requests),
    model_stats: [...modelAgg.values()].map(finalizeCounterRow).sort((a, b) => b.total_requests - a.total_requests),
    generated_at: data.generated_at || new Date().toISOString(),
    _meta: {}
  };
}

function buildHealthGridFromDetails(details, generatedAt) {
  const grid = emptyHealthGrid(generatedAt);
  if (!Array.isArray(details) || !details.length) return grid;
  const windowStart = timestampMs(grid[0].start);
  const windowEnd = timestampMs(grid[grid.length - 1].end);
  details.forEach((detail) => {
    const ms = timestampMs(detail && detail.timestamp);
    if (!ms || ms < windowStart || ms >= windowEnd) return;
    const idx = Math.floor((ms - windowStart) / healthGridStepMs);
    const slot = grid[idx];
    if (!slot) return;
    if (detail.failed) slot.failure++;
    else slot.success++;
    slot.total = slot.success + slot.failure;
  });
  return grid;
}

async function exportRows(kind) {
  const params = new URLSearchParams();
  params.set('range', $('range').value);
  const fm = $('filterModel').value; if (fm) params.set('model', fm);
  const fs = $('filterSource').value; if (fs) params.set('source', fs);
  const fa = $('filterAuth').value; if (fa) params.set('auth', fa);
  if (selectedClientApiSelector()) params.set('client_api', selectedClientApiSelector());
  try {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    if (kind === 'csv') {
      params.set('format', 'csv');
      const meta = await fetchExportJobResult(params);
      notifyExportTruncated(exportTruncationFromHeaders(meta.headers));
      download('usage-events-' + stamp + '.csv', meta.data, 'text/csv;charset=utf-8');
      return;
    }
    params.set('format', 'json');
    const meta = await fetchExportJobResult(params);
    const data = typeof meta.data === 'string' ? JSON.parse(meta.data || '{}') : meta.data;
    const rows = data.events || [];
    notifyExportTruncated({ truncated: !!data.truncated, total: data.total, exported: rows.length });
    if (kind === 'json') { download('usage-events-' + stamp + '.json', JSON.stringify(rows, null, 2), 'application/json;charset=utf-8'); return }
    download('usage-events-' + stamp + '.csv', rowsCsv(rows), 'text/csv;charset=utf-8');
  } catch (e) { alert(t('export_failed')); }
}

async function exportApiRows(kind) {
  if (!selectedApi) return;
  const params = new URLSearchParams();
  params.set('range', $('range').value);
  const fm = $('filterModel').value; if (fm) params.set('model', fm);
  const fs = $('filterSource').value; if (fs) params.set('source', fs);
  const fa = $('filterAuth').value; if (fa) params.set('auth', fa);
  if (selectedClientApiSelector()) params.set('client_api', selectedClientApiSelector());
  params.set('api', selectedApi);
  try {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const name = (friendlyApiName(selectedApi) || 'api').replace(/[\\/:*?"<>|\s]+/g, '-').slice(0, 80);
    if (kind === 'csv') {
      params.set('format', 'csv');
      const meta = await fetchExportJobResult(params);
      notifyExportTruncated(exportTruncationFromHeaders(meta.headers));
      download('usage-api-' + name + '-' + stamp + '.csv', meta.data, 'text/csv;charset=utf-8');
      return;
    }
    params.set('format', 'json');
    const meta = await fetchExportJobResult(params);
    const data = typeof meta.data === 'string' ? JSON.parse(meta.data || '{}') : meta.data;
    const rows = data.events || [];
    notifyExportTruncated({ truncated: !!data.truncated, total: data.total, exported: rows.length });
    if (kind === 'json') { download('usage-api-' + name + '-' + stamp + '.json', JSON.stringify(rows, null, 2), 'application/json;charset=utf-8'); return }
    download('usage-api-' + name + '-' + stamp + '.csv', rowsCsv(rows), 'text/csv;charset=utf-8');
  } catch (e) { alert(t('export_failed')); }
}

function exportTruncationFromHeaders(headers) {
  return {
    truncated: headerValue(headers, 'X-Export-Truncated') === 'true',
    total: num(headerValue(headers, 'X-Total-Count')),
    exported: num(headerValue(headers, 'X-Exported-Count')),
  };
}

function notifyExportTruncated(info) {
  if (!info || !info.truncated) return;
  alert(t('export_truncated', fmt.format(num(info.total)), fmt.format(num(info.exported))));
}

function summaryRecordKey(data) {
  if (!data) return '';
  const meta = data._meta || {};
  const usage = data.usage || {};
  return [
    meta.summary_version || '',
    meta.last_recorded_at || '',
    usage.total_requests || '',
    meta.current_detail_count || '',
    meta.evicted_total || '',
  ].join('|');
}

function shouldRefreshDetails(previousSummary, nextSummary, forceDetails) {
  if (forceDetails || !eventsData) return true;
  if (currentRange !== $('range').value) return true;
  const nextKey = summaryRecordKey(nextSummary);
  if (!nextKey) return true;
  return nextKey !== summaryRecordKey(previousSummary);
}

async function rerender(options) {
  const opts = Object.assign({ refreshEvents: true, refreshApiDetail: true }, options || {});
  const previousApi = selectedApi;

  // Refresh locale-aware formatters if language changed
  if (typeof getFormatLocale === 'function') {
    var newLocale = getFormatLocale();
    if (newLocale !== _lastFmtLocale) {
      fmt = new Intl.NumberFormat(newLocale);
      _lastFmtLocale = newLocale;
      if (typeof refreshMoneyFormatters === 'function') refreshMoneyFormatters();
    }
  }

  renderUpdated();
  renderStats();
  renderStorageStatus();
  renderDistributionDashboard();
  renderHealth();
  renderPrices();
  renderClientApiStats();
  renderApiStats();
  renderModelStats();
  initTrendChart();
  renderTrendChart();
  if (opts.refreshEvents) await renderEvents();
  else renderEventsContent();
  if (opts.refreshApiDetail || previousApi !== selectedApi) await renderApiDetail();
  else renderApiDetailFromCache();
  if (typeof applyI18N === 'function') applyI18N();
}

function pollDelay() { return document.visibilityState === 'hidden' ? hiddenPollDelayMs : visiblePollDelayMs }
function schedulePoll(delayMs) { if (pollTimer) clearTimeout(pollTimer); pollTimer = setTimeout(load, delayMs) }
function nextFailureDelay() { return Math.min(300000, [5000, 15000, 45000, 90000, 180000][Math.min(pollFailures - 1, 4)] || 300000) }

async function load(options) {
  const forceDetails = options && options.forceDetails;
  try {
    const previousSummary = summaryData;
    const selectedRange = $('range').value;
    // Try new summary endpoint first with current range
    const summaryUrl = pluginEndpoint('dashboard-summary') + '?range=' + encodeURIComponent(selectedRange);
    // A transient model-prices error must not send the page through the
    // compatibility data path; stale prices are better than replacing
    // range-scoped stats with reconstructed fallback data.
    const [data] = await Promise.all([
      fetchConditionalJsonPayload('dashboard-summary:' + summaryUrl, summaryUrl, pluginFetchOptions({ cache: 'no-store' })),
      loadModelPrices().catch(function() { /* prices failure tolerated; stale prices beat wrong stats */ }),
    ]);
    summaryData = requireObjectPayload(data, 'dashboard-summary');
    if (selectedClientApi) await refreshFilteredSummary();
    updatedState = { type: 'success', generatedAt: data.generated_at || Date.now(), message: '' };
    renderUpdated();
    const refreshDetails = !!selectedClientApi || shouldRefreshDetails(previousSummary, summaryData, forceDetails);
    await rerender({ refreshEvents: refreshDetails, refreshApiDetail: refreshDetails });
    currentRange = selectedRange;
    pollFailures = 0; schedulePoll(pollDelay());
  } catch (error) {
    // Fallback: try old dashboard-data endpoint
    try {
      const previousSummary = summaryData;
      const selectedRange = $('range').value;
      const [data] = await Promise.all([
        fetchJsonPayload(pluginEndpoint('dashboard-data'), pluginFetchOptions({ cache: 'no-store' })),
        loadModelPrices().catch(function() { /* prices failure tolerated */ }),
      ]);
      summaryData = buildSummaryFromFullUsage(data, selectedRange);
      if (selectedClientApi) {
        filteredSummaryData = null;
        filteredSummaryContext = '';
        filteredSummaryError = new Error(t('client_api_filter_compat_unavailable'));
      }
      updatedState = { type: 'compat', generatedAt: data.generated_at || Date.now(), message: '' };
      renderUpdated();
      const refreshDetails = shouldRefreshDetails(previousSummary, summaryData, forceDetails);
      await rerender({ refreshEvents: refreshDetails, refreshApiDetail: refreshDetails });
      currentRange = selectedRange;
      pollFailures = 0; schedulePoll(pollDelay());
    } catch (fallbackError) {
      updatedState = { type: 'error', generatedAt: null, message: (fallbackError && fallbackError.message) || (error && error.message) || '' };
      renderUpdated();
      pollFailures++; schedulePoll(nextFailureDelay());
    }
  }
}

function handleVisibilityChange() {
  if (document.visibilityState === 'visible') {
    load();
    return;
  }
  schedulePoll(hiddenPollDelayMs);
}

// Event bindings
$('range').value = localStorage.getItem(rangeKey) || '24h';
$('range').onchange = () => { localStorage.setItem(rangeKey, $('range').value); resetPaginationOffsets(); load({ forceDetails: true }) };
$('refreshBtn').onclick = () => load({ forceDetails: true });
$('savePrice').onclick = async () => {
  const m = $('priceModel').value.trim(); if (!m) return;
  const prompt = num($('pricePrompt').value), completion = num($('priceCompletion').value), cache = $('priceCache').value === '' ? prompt : num($('priceCache').value), cacheWrite = $('priceCacheWrite').value === '' ? 0 : num($('priceCacheWrite').value);
  try {
    await saveModelPrice(m, { prompt, completion, cache, cache_write: cacheWrite });
    fillPriceForm('');
    await rerender({ refreshEvents: false, refreshApiDetail: true });
  } catch (e) {
    alert(t('price_save_failed') + (e && e.message ? e.message : t('unknown_error')));
  }
};
$('priceModel').onchange = () => syncPriceFormForModel($('priceModel').value);
$('priceReferenceModel').onfocus = () => {
  if ($('priceReferenceModel').disabled) return;
  priceReferenceActiveIndex = -1;
  renderPriceReferenceOptions($('priceReferenceModel').value, true);
};
$('priceReferenceModel').oninput = () => {
  selectedPriceReferenceModel = '';
  priceReferenceActiveIndex = -1;
  renderPriceReferenceInfo(priceReferenceOptions());
  renderPriceReferenceOptions($('priceReferenceModel').value, true);
};
$('priceReferenceModel').onchange = () => {
  const value = $('priceReferenceModel').value;
  const exact = priceReferenceOptions().find((model) => normalizedPriceKey(model) === normalizedPriceKey(value));
  if (exact) selectPriceReferenceModel(exact);
};
$('priceReferenceModel').onkeydown = handlePriceReferenceKeydown;
$('priceReferenceOptions').onmousedown = (event) => {
  let target = event && event.target;
  while (target && target !== $('priceReferenceOptions') && !(target.dataset && target.dataset.priceReferenceOption)) target = target.parentNode;
  if (target && target.dataset && target.dataset.priceReferenceOption && event && event.preventDefault) event.preventDefault();
};
$('priceReferenceOptions').onclick = (event) => {
  let target = event && event.target;
  while (target && target !== $('priceReferenceOptions') && !(target.dataset && target.dataset.priceReferenceOption)) target = target.parentNode;
  if (target && target.dataset && target.dataset.priceReferenceOption) selectPriceReferenceModel(target.dataset.priceReferenceOption);
};
if (document.addEventListener) document.addEventListener('click', (event) => {
  if (!elementContains($('priceReferenceCombo'), event && event.target)) closePriceReferenceOptions();
});
document.querySelectorAll('[data-api-sort]').forEach((btn) => btn.onclick = async () => {
  clientApiSort = btn.dataset.apiSort || 'requests';
  clientApiSelectMode = false;
  selectedClientApi = null;
  filteredSummaryData = null;
  filteredSummaryContext = '';
  filteredSummaryError = null;
  resetPaginationOffsets();
  await rerender({ refreshEvents: true, refreshApiDetail: true });
});
const clientApiSelectButton = document.querySelectorAll('[data-client-api-select]')[0];
if (clientApiSelectButton) clientApiSelectButton.onclick = () => { clientApiSelectMode = true; renderClientApiStats() };
['filterModel', 'filterSource', 'filterAuth'].forEach((id) => $(id).onchange = () => { resetPaginationOffsets(); renderEvents() });
$('clearFilters').onclick = () => { ['filterModel', 'filterSource', 'filterAuth'].forEach((id) => $(id).value = ''); resetPaginationOffsets(); renderEvents() };
$('exportRowsCsv').onclick = () => exportRows('csv'); $('exportRowsJson').onclick = () => exportRows('json');
$('exportApiCsv').onclick = () => exportApiRows('csv'); $('exportApiJson').onclick = () => exportApiRows('json');
$('exportBtn').onclick = async () => {
  try {
    const data = await fetchJsonPayload(pluginEndpoint('usage/export'), pluginFetchOptions({ cache: 'no-store' }));
    download('usage-export-' + new Date().toISOString().replace(/[:.]/g, '-') + '.json', JSON.stringify(data, null, 2), 'application/json;charset=utf-8');
  } catch (e) { alert(t('export_failed_msg') + (e && e.message ? e.message : t('unknown_error'))) }
};
$('importBtn').onclick = () => $('importFile').click();
$('importFile').onchange = async (e) => {
  const file = e.target.files && e.target.files[0]; if (!file) return;
  try {
    const text = await file.text();
    if (!currentManagementKey()) throw new Error(t('import_no_key'));
    const result = await fetchManagementJsonPayload('usage/import', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: text });
    alert(t('import_complete', result.added || 0, result.skipped || 0, result.ignored_by_retention || 0));
    await load({ forceDetails: true });
  } catch (err) {
    alert(t('import_failed') + (err && err.message ? err.message : t('unknown_error')));
  } finally {
    e.target.value = '';
  }
};
if (document.addEventListener) document.addEventListener('visibilitychange', handleVisibilityChange);
renderUpdated();
initTrendChart();
load();

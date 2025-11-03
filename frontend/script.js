const state = {
  limit: 20,
  offset: 0,
  sort: 'created_at',
  order: 'desc',
  query: '',
  languages: [],
  statuses: [],
  minSubscribers: '',
  maxSubscribers: '',
  total: 0,
  rows: new Map(),
  eventSource: null,
  currentJobId: null,
  progress: { total: 0, completed: 0, errors: 0, pending: 0, durationSeconds: 0 },
};

const statusEl = document.getElementById('status');
const progressEl = document.getElementById('progress');
const batchSummaryEl = document.getElementById('batchSummary');
const statsSummaryEl = document.getElementById('statsSummary');
const tableBody = document.querySelector('#channelsTable tbody');
const pageInfo = document.getElementById('pageInfo');

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || 'Request failed');
  }
  return response.headers.get('content-type')?.includes('application/json') ? response.json() : response.text();
}

function parseKeywords(raw) {
  return raw
    .split(/[\n,]+/)
    .map((word) => word.trim())
    .filter((word) => word.length > 0);
}

function parseListInput(value) {
  return value
    .split(/[\s,;]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function buildQueryParams({ includePagination = true } = {}) {
  const params = new URLSearchParams();
  if (includePagination) {
    params.set('limit', state.limit);
    params.set('offset', state.offset);
  }
  params.set('sort', state.sort);
  params.set('order', state.order);
  if (state.query) {
    params.set('q', state.query);
  }
  state.languages.forEach((language) => params.append('language', language));
  state.statuses.forEach((status) => params.append('status', status));
  if (state.minSubscribers) {
    params.set('min_subscribers', state.minSubscribers);
  }
  if (state.maxSubscribers) {
    params.set('max_subscribers', state.maxSubscribers);
  }
  return params;
}

function setStatus(message, type = 'info') {
  statusEl.textContent = message;
  statusEl.dataset.type = type;
}

function setProgress(message) {
  progressEl.textContent = message;
}

function setBatchSummary(message) {
  batchSummaryEl.textContent = message;
}

function setStatsSummary(message) {
  statsSummaryEl.textContent = message;
}

function formatLanguage(item) {
  if (!item.language) {
    return '';
  }
  if (typeof item.language_confidence === 'number') {
    return `${item.language} (${Math.round(item.language_confidence * 100)}%)`;
  }
  return item.language;
}

function renderEmailsCell(td, emails) {
  if (!emails) {
    td.textContent = '';
    return;
  }
  const list = emails.split(/[,;]+/).map((email) => email.trim()).filter(Boolean).slice(0, 5);
  list.forEach((email, index) => {
    const span = document.createElement('div');
    span.textContent = email;
    if (index > 0) {
      span.style.marginTop = '0.1rem';
    }
    td.appendChild(span);
  });
}

function statusClass(status) {
  switch (status) {
    case 'processing':
      return 'processing';
    case 'completed':
      return 'success';
    case 'error':
      return 'error';
    case 'new':
    default:
      return 'neutral';
  }
}

function statusLabel(status) {
  if (!status) {
    return 'New';
  }
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function applyRowData(row, item) {
  row.innerHTML = '';
  const cells = [
    item.title || 'Unknown',
    item.url,
    item.subscribers ?? '',
    formatLanguage(item),
    item.emails || '',
    item.status || 'new',
    item.last_updated || '',
    item.status_reason || item.last_error || '',
  ];

  const nameCell = document.createElement('td');
  nameCell.textContent = cells[0];
  row.appendChild(nameCell);

  const linkCell = document.createElement('td');
  if (cells[1]) {
    const anchor = document.createElement('a');
    anchor.href = cells[1];
    anchor.target = '_blank';
    anchor.rel = 'noopener';
    anchor.textContent = 'Open';
    linkCell.appendChild(anchor);
  }
  row.appendChild(linkCell);

  const subsCell = document.createElement('td');
  subsCell.textContent = cells[2];
  row.appendChild(subsCell);

  const languageCell = document.createElement('td');
  languageCell.textContent = cells[3];
  row.appendChild(languageCell);

  const emailCell = document.createElement('td');
  renderEmailsCell(emailCell, cells[4]);
  row.appendChild(emailCell);

  const statusCell = document.createElement('td');
  const badge = document.createElement('span');
  badge.className = `status-badge ${statusClass(cells[5])}`;
  badge.textContent = statusLabel(cells[5]);
  statusCell.appendChild(badge);
  row.appendChild(statusCell);

  const updatedCell = document.createElement('td');
  updatedCell.textContent = cells[6];
  row.appendChild(updatedCell);

  const errorCell = document.createElement('td');
  if (cells[7]) {
    errorCell.classList.add('error-text');
    errorCell.textContent = cells[7];
  }
  row.appendChild(errorCell);
}

function renderTable(items) {
  tableBody.innerHTML = '';
  state.rows.clear();

  if (items.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.textContent = 'No channels yet. Try discovering some keywords.';
    row.appendChild(cell);
    tableBody.appendChild(row);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement('tr');
    row.dataset.channelId = item.channel_id;
    const storedItem = { ...item };
    applyRowData(row, storedItem);
    state.rows.set(item.channel_id, { element: row, item: storedItem });
    tableBody.appendChild(row);
  });
}

function updatePagination() {
  const currentPage = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${state.total} channels)`;
}

function updateChannelRowFromStream(update) {
  if (!update?.channelId) {
    return;
  }
  const entry = state.rows.get(update.channelId);
  if (!entry) {
    return;
  }
  const { item, element } = entry;
  if (typeof update.subscribers === 'number') {
    item.subscribers = update.subscribers;
  }
  if (update.language) {
    item.language = update.language;
  }
  if (typeof update.languageConfidence === 'number') {
    item.language_confidence = update.languageConfidence;
  }
  if (Array.isArray(update.emails)) {
    item.emails = update.emails.join(', ');
  }
  if (update.lastUpdated) {
    item.last_updated = update.lastUpdated;
  }
  if (update.lastStatusChange) {
    item.last_status_change = update.lastStatusChange;
  }
  if (update.status) {
    item.status = update.status;
  }
  if ('statusReason' in update) {
    item.status_reason = update.statusReason || '';
  }
  applyRowData(element, item);
}

function updateProgressText() {
  const { total, completed, errors, pending } = state.progress;
  if (!total && !completed && !errors && !pending) {
    setProgress('');
    return;
  }
  setProgress(`Enrichment: ${completed} completed · ${errors} error · ${pending} pending`);
}

function formatDuration(seconds) {
  if (typeof seconds !== 'number' || Number.isNaN(seconds)) {
    return '';
  }
  const totalSeconds = Math.max(0, Math.round(seconds));
  const minutes = Math.floor(totalSeconds / 60);
  const secs = totalSeconds % 60;
  if (minutes > 0) {
    return `${minutes}m ${secs.toString().padStart(2, '0')}s`;
  }
  return `${secs}s`;
}

function resetProgress() {
  state.progress = { total: 0, completed: 0, errors: 0, pending: 0, durationSeconds: 0 };
  updateProgressText();
}

function startEnrichmentStream(jobId, total) {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
  state.currentJobId = jobId;
  state.progress = { total, completed: 0, errors: 0, pending: total, durationSeconds: 0 };
  updateProgressText();
  setBatchSummary('');

  const eventSource = new EventSource(`/api/enrich/stream/${jobId}`);
  state.eventSource = eventSource;

  eventSource.onmessage = (event) => {
    if (!event.data) {
      return;
    }
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === 'channel') {
        updateChannelRowFromStream(payload);
      } else if (payload.type === 'progress') {
        state.progress = {
          total: payload.total ?? state.progress.total,
          completed: payload.completed ?? state.progress.completed,
          errors: payload.errors ?? state.progress.errors,
          pending: payload.pending ?? Math.max(0, (payload.total ?? state.progress.total) - (payload.completed ?? 0) - (payload.errors ?? 0)),
          durationSeconds: payload.durationSeconds ?? state.progress.durationSeconds,
        };
        updateProgressText();
        if (payload.done) {
          finalizeEnrichment();
        }
      }
    } catch (error) {
      console.error('Failed to process enrichment update', error);
    }
  };

  eventSource.onerror = () => {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    if (state.currentJobId) {
      setStatus('Connection to enrichment stream lost.', 'error');
    }
  };
}

function finalizeEnrichment() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
  const { completed, errors, durationSeconds } = state.progress;
  const durationText = formatDuration(durationSeconds);
  const summary = `${completed} ok, ${errors} error${durationText ? `, ${durationText}` : ''}`;
  setBatchSummary(summary);
  setStatus(`Enrichment finished: ${summary}.`, errors ? 'warning' : 'success');
  state.currentJobId = null;
  updateProgressText();
  loadChannels();
  pollStats();
}

async function loadChannels() {
  const params = buildQueryParams();
  try {
    const data = await fetchJSON(`/api/channels?${params.toString()}`);
    state.total = data.total;
    renderTable(data.items || []);
    updatePagination();
  } catch (error) {
    console.error(error);
    setStatus(`Failed to load channels: ${error.message}`, 'error');
  }
}

async function handleDiscover() {
  const keywords = parseKeywords(document.getElementById('keywords').value);
  const perKeyword = parseInt(document.getElementById('perKeyword').value, 10) || 5;
  if (keywords.length === 0) {
    setStatus('Please provide at least one keyword.', 'error');
    return;
  }
  setStatus('Discovering channels…');
  try {
    const response = await fetchJSON('/api/discover', {
      method: 'POST',
      body: JSON.stringify({ keywords, perKeyword }),
    });
    setStatus(`Found ${response.found} new channels. Total: ${response.uniqueTotal}.`, 'success');
    state.offset = 0;
    await loadChannels();
    await pollStats();
  } catch (error) {
    console.error(error);
    setStatus(`Discover failed: ${error.message}`, 'error');
  }
}

async function handleEnrich() {
  setStatus('Starting enrichment…');
  resetProgress();
  try {
    const response = await fetchJSON('/api/enrich', {
      method: 'POST',
      body: JSON.stringify({ limit: 40 }),
    });
    if (!response || typeof response.jobId !== 'string') {
      setStatus('Failed to start enrichment job.', 'error');
      return;
    }
    if (response.total === 0) {
      setStatus('No channels waiting for enrichment.', 'info');
      setBatchSummary('');
      resetProgress();
      return;
    }
    setStatus(`Enrichment started for ${response.total} channel${response.total === 1 ? '' : 's'}.`, 'info');
    startEnrichmentStream(response.jobId, response.total);
  } catch (error) {
    console.error(error);
    setStatus(`Enrichment failed: ${error.message}`, 'error');
  }
}

async function handleExport() {
  setStatus('Preparing CSV export…');
  try {
    const params = buildQueryParams({ includePagination: false });
    params.set('sort', state.sort);
    params.set('order', state.order);
    const response = await fetch(`/api/export/csv?${params.toString()}`);
    if (!response.ok) {
      throw new Error('Export failed');
    }
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'crypto-youtube-channels.csv';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
    setStatus('Export ready.', 'success');
  } catch (error) {
    console.error(error);
    setStatus(`Export failed: ${error.message}`, 'error');
  }
}

let statsInterval = null;

async function pollStats() {
  try {
    const stats = await fetchJSON('/api/stats');
    setStatsSummary(
      `${stats.total} stored · ${stats.new} new · ${stats.processing} processing · ${stats.completed} completed · ${stats.error} error`
    );
  } catch (error) {
    setStatsSummary('Unable to load stats');
  }
}

function initEvents() {
  document.getElementById('discoverBtn').addEventListener('click', handleDiscover);
  document.getElementById('enrichBtn').addEventListener('click', handleEnrich);
  document.getElementById('exportBtn').addEventListener('click', handleExport);

  document.getElementById('prevPage').addEventListener('click', () => {
    state.offset = Math.max(0, state.offset - state.limit);
    loadChannels();
  });
  document.getElementById('nextPage').addEventListener('click', () => {
    if (state.offset + state.limit < state.total) {
      state.offset += state.limit;
      loadChannels();
    }
  });

  document.getElementById('sort').addEventListener('change', (event) => {
    state.sort = event.target.value;
    state.offset = 0;
    loadChannels();
  });

  document.getElementById('order').addEventListener('change', (event) => {
    state.order = event.target.value;
    state.offset = 0;
    loadChannels();
  });

  const searchInput = document.getElementById('search');
  let searchTimer = null;
  searchInput.addEventListener('input', (event) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.query = event.target.value.trim();
      state.offset = 0;
      loadChannels();
    }, 250);
  });

  const languageInput = document.getElementById('languageFilter');
  let languageTimer = null;
  languageInput.addEventListener('input', (event) => {
    clearTimeout(languageTimer);
    languageTimer = setTimeout(() => {
      state.languages = parseListInput(event.target.value.toLowerCase());
      state.offset = 0;
      loadChannels();
    }, 300);
  });

  document.getElementById('minSubs').addEventListener('change', (event) => {
    state.minSubscribers = event.target.value.trim();
    state.offset = 0;
    loadChannels();
  });

  document.getElementById('maxSubs').addEventListener('change', (event) => {
    state.maxSubscribers = event.target.value.trim();
    state.offset = 0;
    loadChannels();
  });

  document.querySelectorAll('.status-options input[type="checkbox"]').forEach((checkbox) => {
    checkbox.addEventListener('change', () => {
      state.statuses = Array.from(document.querySelectorAll('.status-options input[type="checkbox"]'))
        .filter((cb) => cb.checked)
        .map((cb) => cb.value);
      state.offset = 0;
      loadChannels();
    });
  });
}

async function init() {
  initEvents();
  await loadChannels();
  await pollStats();
  statsInterval = setInterval(pollStats, 5000);
}

window.addEventListener('beforeunload', () => {
  if (state.eventSource) {
    state.eventSource.close();
  }
  if (statsInterval) {
    clearInterval(statsInterval);
  }
});

window.addEventListener('DOMContentLoaded', init);

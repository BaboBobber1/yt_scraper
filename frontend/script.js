const state = {
  limit: 20,
  offset: 0,
  sort: 'created_at',
  order: 'desc',
  search: '',
  total: 0,
};

const statusEl = document.getElementById('status');
const progressEl = document.getElementById('progress');
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

function setStatus(message, type = 'info') {
  statusEl.textContent = message;
  statusEl.dataset.type = type;
}

function setProgress(message) {
  progressEl.textContent = message;
}

async function loadChannels() {
  const params = new URLSearchParams({
    limit: state.limit,
    offset: state.offset,
    sort: state.sort,
    order: state.order,
  });
  if (state.search) {
    params.set('search', state.search);
  }
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

function renderTable(items) {
  tableBody.innerHTML = '';
  if (items.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 6;
    cell.textContent = 'No channels yet. Try discovering some keywords.';
    row.appendChild(cell);
    tableBody.appendChild(row);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement('tr');
    const emails = item.emails || '';
    const language = item.language
      ? `${item.language}${item.language_confidence ? ` (${(item.language_confidence * 100).toFixed(0)}%)` : ''}`
      : '';

    row.innerHTML = `
      <td>${item.title || 'Unknown'}</td>
      <td><a href="${item.url}" target="_blank" rel="noopener">Open</a></td>
      <td>${item.subscribers ?? ''}</td>
      <td>${language}</td>
      <td>${emails}</td>
      <td>${item.last_updated || ''}</td>
    `;

    tableBody.appendChild(row);
  });
}

function updatePagination() {
  const currentPage = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${state.total} channels)`;
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
  } catch (error) {
    console.error(error);
    setStatus(`Discover failed: ${error.message}`, 'error');
  }
}

async function handleEnrich() {
  setStatus('Enriching channels…');
  try {
    const response = await fetchJSON('/api/enrich', {
      method: 'POST',
      body: JSON.stringify({ limit: 25 }),
    });
    setStatus(`Processed ${response.processed} channels.`, 'success');
    await loadChannels();
  } catch (error) {
    console.error(error);
    setStatus(`Enrichment failed: ${error.message}`, 'error');
  }
}

async function handleExport() {
  setStatus('Preparing CSV export…');
  try {
    const response = await fetch('/api/export/csv');
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
    setProgress(`${stats.total} channels stored · ${stats.pending_enrichment} pending enrichment`);
  } catch (error) {
    setProgress('Unable to load stats');
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

  const searchInput = document.getElementById('search');
  let debounceTimer = null;
  searchInput.addEventListener('input', (event) => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      state.search = event.target.value;
      state.offset = 0;
      loadChannels();
    }, 300);
  });
}

async function init() {
  initEvents();
  await loadChannels();
  await pollStats();
  statsInterval = setInterval(pollStats, 5000);
}

window.addEventListener('DOMContentLoaded', init);

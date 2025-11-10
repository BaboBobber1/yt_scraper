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
  visibleCount: 0,
  rows: new Map(),
  eventSource: null,
  currentJobId: null,
  currentJobMode: null,
  progress: { total: 0, completed: 0, errors: 0, pending: 0, durationSeconds: 0 },
  emailsOnly: false,
  includeArchived: false,
  uniqueEmails: false,
  enrichmentBusy: false,
};

const statusEl = document.getElementById('status');
const progressEl = document.getElementById('progress');
const batchSummaryEl = document.getElementById('batchSummary');
const statsSummaryEl = document.getElementById('statsSummary');
const tableBody = document.querySelector('#channelsTable tbody');
const pageInfo = document.getElementById('pageInfo');
const archiveAllBtn = document.getElementById('archiveAllBtn');
const emailsToggle = document.getElementById('emailsToggle');
const uniqueEmailsToggle = document.getElementById('uniqueEmailsToggle');
const hideArchivedToggle = document.getElementById('hideArchivedToggle');
const enrichBtn = document.getElementById('enrichBtn');
const enrichEmailBtn = document.getElementById('enrichEmailBtn');
const archivingInFlight = new Set();
const importBlacklistBtn = document.getElementById('importBlacklistBtn');
const blacklistModal = document.getElementById('blacklistModal');
const blacklistForm = document.getElementById('blacklistForm');
const blacklistFileInput = document.getElementById('blacklistFile');
const blacklistSummary = document.getElementById('blacklistSummary');
const blacklistSubmitBtn = document.getElementById('blacklistSubmitBtn');
const blacklistCloseBtn = document.getElementById('blacklistCloseBtn');
const blacklistCancelBtn = document.getElementById('blacklistCancelBtn');

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

const EMAIL_REGEX = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/gi;

function extractEmails(text) {
  if (!text) {
    return [];
  }
  const matches = text.match(EMAIL_REGEX);
  return matches ? matches.map((email) => email.trim()).filter(Boolean) : [];
}

function applyUniqueEmailFilter(items) {
  const seen = new Set();
  const filtered = [];
  items.forEach((item) => {
    const emails = extractEmails(item.emails || '');
    if (emails.length === 0) {
      return;
    }
    const uniqueEmails = [];
    emails.forEach((email) => {
      const key = email.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        uniqueEmails.push(email);
      }
    });
    if (uniqueEmails.length > 0) {
      filtered.push({ ...item, emails: uniqueEmails.join(', ') });
    }
  });
  return filtered;
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
  params.set('emails_only', state.emailsOnly ? 'true' : 'false');
  params.set('include_archived', state.includeArchived ? 'true' : 'false');
  params.set('unique_emails', state.uniqueEmails ? 'true' : 'false');
  return params;
}

function setStatus(message, type = 'info') {
  statusEl.textContent = message;
  statusEl.dataset.type = type;
}

function describeMode(mode) {
  switch (mode) {
    case 'email_only':
      return 'Email-only';
    case 'full':
    default:
      return 'Full';
  }
}

function setEnrichmentBusy(disabled) {
  state.enrichmentBusy = disabled;
  if (enrichBtn) {
    enrichBtn.disabled = disabled;
  }
  if (enrichEmailBtn) {
    enrichEmailBtn.disabled = disabled;
  }
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

function syncUniqueToggle() {
  if (!uniqueEmailsToggle) {
    return;
  }
  uniqueEmailsToggle.disabled = !state.emailsOnly;
  if (!state.emailsOnly) {
    uniqueEmailsToggle.checked = false;
  } else {
    uniqueEmailsToggle.checked = state.uniqueEmails;
  }
}

function resetBlacklistModal() {
  if (blacklistForm) {
    blacklistForm.reset();
  }
  if (blacklistSummary) {
    blacklistSummary.textContent = '';
    blacklistSummary.dataset.type = '';
  }
  if (blacklistSubmitBtn) {
    blacklistSubmitBtn.disabled = false;
  }
  if (blacklistCancelBtn) {
    blacklistCancelBtn.disabled = false;
  }
}

function openBlacklistModal() {
  if (!blacklistModal) {
    return;
  }
  resetBlacklistModal();
  blacklistModal.classList.add('open');
  blacklistModal.setAttribute('aria-hidden', 'false');
}

function closeBlacklistModal() {
  if (!blacklistModal) {
    return;
  }
  blacklistModal.classList.remove('open');
  blacklistModal.setAttribute('aria-hidden', 'true');
}

async function handleBlacklistImport(event) {
  event.preventDefault();
  if (!blacklistFileInput || !blacklistFileInput.files || blacklistFileInput.files.length === 0) {
    if (blacklistSummary) {
      blacklistSummary.textContent = 'Please choose a CSV file to import.';
      blacklistSummary.dataset.type = 'error';
    }
    return;
  }

  const file = blacklistFileInput.files[0];
  const formData = new FormData();
  formData.append('file', file);

  if (blacklistSummary) {
    blacklistSummary.textContent = 'Importing blacklist…';
    blacklistSummary.dataset.type = '';
  }
  if (blacklistSubmitBtn) {
    blacklistSubmitBtn.disabled = true;
  }
  if (blacklistCancelBtn) {
    blacklistCancelBtn.disabled = true;
  }

  try {
    const response = await fetch('/api/blacklist/import', {
      method: 'POST',
      body: formData,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || 'Import failed');
    }
    let result = {};
    try {
      result = await response.json();
    } catch (error) {
      console.warn('Failed to parse blacklist import response as JSON', error);
    }
    const created = Number(result.created) || 0;
    const updated = Number(result.updated) || 0;
    const skipped = Number(result.skipped) || 0;
    const unresolved = Number(result.unresolved) || 0;
    const imported = created + updated;
    const skippedText = skipped
      ? `, skipped: ${skipped}${unresolved ? ` (unresolved: ${unresolved})` : ''}`
      : '';
    const summaryText = `Imported ${imported} channel${imported === 1 ? '' : 's'} (updated: ${updated}, created: ${created}${skippedText}).`;
    if (blacklistSummary) {
      blacklistSummary.textContent = summaryText;
      blacklistSummary.dataset.type = 'success';
    }
    setStatus(summaryText, 'success');
    if (blacklistFileInput) {
      blacklistFileInput.value = '';
    }
    await loadChannels();
    await pollStats();
  } catch (error) {
    console.error('Blacklist import failed', error);
    if (blacklistSummary) {
      blacklistSummary.textContent = `Import failed: ${error.message}`;
      blacklistSummary.dataset.type = 'error';
    }
    setStatus(`Blacklist import failed: ${error.message}`, 'error');
  } finally {
    if (blacklistSubmitBtn) {
      blacklistSubmitBtn.disabled = false;
    }
    if (blacklistCancelBtn) {
      blacklistCancelBtn.disabled = false;
    }
  }
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
  const list = extractEmails(emails).slice(0, 5);
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
  row.dataset.channelId = item.channel_id;
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

  row.classList.toggle('archived-row', Boolean(item.archived));

  const nameCell = document.createElement('td');
  const nameText = document.createElement('span');
  nameText.textContent = cells[0];
  nameCell.appendChild(nameText);
  if (item.archived) {
    const archivedBadge = document.createElement('span');
    archivedBadge.className = 'archived-badge';
    archivedBadge.textContent = 'Archived';
    nameCell.appendChild(archivedBadge);
  }
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

  const actionsCell = document.createElement('td');
  actionsCell.classList.add('actions-cell');
  const archiveBtn = document.createElement('button');
  archiveBtn.className = 'ghost-button archive-btn';
  if (item.archived) {
    archiveBtn.textContent = 'Archived';
    archiveBtn.disabled = true;
  } else {
    archiveBtn.textContent = 'Archive';
    archiveBtn.addEventListener('click', () => handleArchiveChannel(item.channel_id));
  }
  actionsCell.appendChild(archiveBtn);
  row.appendChild(actionsCell);
}

function renderTable(items) {
  tableBody.innerHTML = '';
  state.rows.clear();

  if (items.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 9;
    cell.textContent = 'No channels yet. Try discovering some keywords.';
    row.appendChild(cell);
    tableBody.appendChild(row);
    updateArchiveControls();
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
  updateArchiveControls();
}

function updatePagination() {
  const currentPage = Math.floor(state.offset / state.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  const totalLabel = state.emailsOnly && state.uniqueEmails
    ? `${state.visibleCount} unique email${state.visibleCount === 1 ? '' : 's'}`
    : `${state.total} channels`;
  pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${totalLabel})`;
}

function updateArchiveControls() {
  if (!archiveAllBtn) {
    return;
  }
  const hasArchivable = Array.from(state.rows.values()).some(({ item }) => !item.archived);
  archiveAllBtn.disabled = !hasArchivable;
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
  const isEmailOnly = update.mode === 'email_only';
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
  if (!isEmailOnly && update.lastStatusChange) {
    item.last_status_change = update.lastStatusChange;
  }
  if (!isEmailOnly && update.status) {
    item.status = update.status;
  }
  if (!isEmailOnly && 'statusReason' in update) {
    item.status_reason = update.statusReason || '';
  }
  if (typeof update.archived === 'boolean') {
    item.archived = update.archived;
  }
  applyRowData(element, item);
  state.rows.set(update.channelId, { element, item });
  updateArchiveControls();
}

function markChannelArchived(channelId, archivedAt) {
  const entry = state.rows.get(channelId);
  if (!entry) {
    return { removed: false };
  }
  const { item, element } = entry;
  item.archived = true;
  item.archived_at = archivedAt;
  let removed = false;
  if (!state.includeArchived) {
    element.remove();
    state.rows.delete(channelId);
    state.total = Math.max(0, state.total - 1);
    removed = true;
    if (state.rows.size === 0 && state.total === 0) {
      tableBody.innerHTML = '';
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 9;
      cell.textContent = 'No channels match the current filters.';
      row.appendChild(cell);
      tableBody.appendChild(row);
    }
  } else {
    applyRowData(element, item);
    state.rows.set(channelId, { element, item });
  }
  state.visibleCount = state.rows.size;
  updatePagination();
  updateArchiveControls();
  return { removed };
}

async function handleArchiveChannel(channelId) {
  if (archivingInFlight.has(channelId)) {
    return;
  }
  const entry = state.rows.get(channelId);
  if (!entry || entry.item.archived) {
    return;
  }

  const button = entry.element.querySelector('.archive-btn');
  archivingInFlight.add(channelId);
  if (button) {
    button.disabled = true;
    button.textContent = 'Archiving…';
  }

  try {
    const response = await fetchJSON(`/api/channels/${channelId}/archive`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
    const archivedAt = response?.archivedAt || new Date().toISOString();
    const { removed } = markChannelArchived(channelId, archivedAt);
    setStatus('Channel archived.', 'success');
    if (removed && state.rows.size === 0 && state.total > 0) {
      await loadChannels();
    }
    await pollStats();
  } catch (error) {
    console.error(error);
    if (button) {
      button.disabled = false;
      button.textContent = 'Archive';
    }
    setStatus(`Failed to archive channel: ${error.message}`, 'error');
  } finally {
    archivingInFlight.delete(channelId);
  }
}

async function handleArchiveBulk() {
  if (!archiveAllBtn) {
    return;
  }
  const candidates = Array.from(state.rows.values())
    .map(({ item }) => item)
    .filter((item) => !item.archived)
    .map((item) => item.channel_id);
  if (candidates.length === 0) {
    return;
  }

  archiveAllBtn.disabled = true;
  try {
    const params = buildQueryParams();
    const response = await fetchJSON(`/api/channels/archive_bulk?${params.toString()}`, {
      method: 'POST',
      body: JSON.stringify({ channel_ids: candidates }),
    });
    const archivedIds = Array.isArray(response?.archivedIds) ? response.archivedIds : candidates;
    const archivedAt = response?.archivedAt || new Date().toISOString();
    archivedIds.forEach((id) => {
      markChannelArchived(id, archivedAt);
    });
    const totalArchived = archivedIds.length;
    setStatus(`Archived ${totalArchived} channel${totalArchived === 1 ? '' : 's'}.`, 'success');
    if (!state.includeArchived && state.rows.size === 0 && state.total > 0) {
      await loadChannels();
    }
    await pollStats();
  } catch (error) {
    console.error(error);
    setStatus(`Failed to archive channels: ${error.message}`, 'error');
  } finally {
    updateArchiveControls();
  }
}

function updateProgressText() {
  const { total, completed, errors, pending } = state.progress;
  if (!total && !completed && !errors && !pending) {
    setProgress('');
    return;
  }
  const modeLabel = state.currentJobMode ? ` (${describeMode(state.currentJobMode)})` : '';
  setProgress(`Enrichment${modeLabel}: ${completed} completed · ${errors} error · ${pending} pending`);
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
  state.currentJobMode = null;
  updateProgressText();
}

function startEnrichmentStream(jobId, total, mode) {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
  state.currentJobId = jobId;
  state.currentJobMode = mode || state.currentJobMode || 'full';
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
        if (payload.mode) {
          state.currentJobMode = payload.mode;
        }
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
    setEnrichmentBusy(false);
    const hadJob = Boolean(state.currentJobId);
    state.currentJobId = null;
    state.currentJobMode = null;
    resetProgress();
    if (hadJob) {
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
  const modeLabel = describeMode(state.currentJobMode || 'full');
  setBatchSummary(`${modeLabel} job: ${summary}`);
  setStatus(`${modeLabel} enrichment finished: ${summary}.`, errors ? 'warning' : 'success');
  state.currentJobId = null;
  setEnrichmentBusy(false);
  resetProgress();
  loadChannels();
  pollStats();
}

async function loadChannels() {
  const params = buildQueryParams();
  try {
    const data = await fetchJSON(`/api/channels?${params.toString()}`);
    let items = Array.isArray(data.items) ? data.items : [];
    if (state.emailsOnly && state.uniqueEmails) {
      items = applyUniqueEmailFilter(items);
    }
    state.total = data.total;
    state.visibleCount = items.length;
    renderTable(items);
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

async function startEnrichment(mode) {
  if (state.enrichmentBusy) {
    return;
  }
  setEnrichmentBusy(true);
  resetProgress();
  setBatchSummary('');
  const modeLabel = describeMode(mode);
  setStatus(`${modeLabel} enrichment starting…`);
  try {
    const response = await fetchJSON('/api/enrich', {
      method: 'POST',
      body: JSON.stringify({ limit: 40, mode }),
    });
    if (!response || typeof response.jobId !== 'string') {
      setStatus('Failed to start enrichment job.', 'error');
      setEnrichmentBusy(false);
      resetProgress();
      return;
    }
    const jobMode = response.mode || mode;
    const jobModeLabel = describeMode(jobMode);
    if (response.total === 0) {
      setStatus(`No channels waiting for ${jobModeLabel.toLowerCase()} enrichment.`, 'info');
      setBatchSummary('');
      setEnrichmentBusy(false);
      resetProgress();
      return;
    }
    state.currentJobMode = jobMode;
    setStatus(`${jobModeLabel} enrichment started for ${response.total} channel${response.total === 1 ? '' : 's'}.`, 'info');
    startEnrichmentStream(response.jobId, response.total, jobMode);
  } catch (error) {
    console.error(error);
    setStatus(`${modeLabel} enrichment failed: ${error.message}`, 'error');
    setEnrichmentBusy(false);
    resetProgress();
  }
}

async function handleEnrich() {
  await startEnrichment('full');
}

async function handleEnrichEmailOnly() {
  await startEnrichment('email_only');
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
  if (enrichBtn) {
    enrichBtn.addEventListener('click', handleEnrich);
  }
  if (enrichEmailBtn) {
    enrichEmailBtn.addEventListener('click', handleEnrichEmailOnly);
  }
  document.getElementById('exportBtn').addEventListener('click', handleExport);
  if (archiveAllBtn) {
    archiveAllBtn.addEventListener('click', handleArchiveBulk);
  }
  if (importBlacklistBtn) {
    importBlacklistBtn.addEventListener('click', openBlacklistModal);
  }
  if (blacklistCloseBtn) {
    blacklistCloseBtn.addEventListener('click', () => {
      closeBlacklistModal();
    });
  }
  if (blacklistCancelBtn) {
    blacklistCancelBtn.addEventListener('click', () => {
      closeBlacklistModal();
    });
  }
  if (blacklistModal) {
    blacklistModal.addEventListener('click', (event) => {
      if (event.target === blacklistModal) {
        closeBlacklistModal();
      }
    });
  }
  if (blacklistForm) {
    blacklistForm.addEventListener('submit', handleBlacklistImport);
  }
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && blacklistModal?.classList.contains('open')) {
      closeBlacklistModal();
    }
  });

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

  if (emailsToggle) {
    emailsToggle.addEventListener('change', (event) => {
      state.emailsOnly = event.target.checked;
      if (!state.emailsOnly && state.uniqueEmails) {
        state.uniqueEmails = false;
      }
      syncUniqueToggle();
      state.offset = 0;
      loadChannels();
    });
  }

  if (uniqueEmailsToggle) {
    uniqueEmailsToggle.addEventListener('change', (event) => {
      if (uniqueEmailsToggle.disabled) {
        return;
      }
      state.uniqueEmails = event.target.checked;
      state.offset = 0;
      loadChannels();
    });
  }

  if (hideArchivedToggle) {
    hideArchivedToggle.addEventListener('change', (event) => {
      state.includeArchived = !event.target.checked;
      state.offset = 0;
      loadChannels();
    });
  }
}

async function init() {
  initEvents();
  if (emailsToggle) {
    emailsToggle.checked = state.emailsOnly;
  }
  if (uniqueEmailsToggle) {
    uniqueEmailsToggle.checked = state.uniqueEmails;
  }
  if (hideArchivedToggle) {
    hideArchivedToggle.checked = !state.includeArchived;
  }
  syncUniqueToggle();
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

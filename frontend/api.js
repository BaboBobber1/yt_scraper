const JSON_HEADERS = { 'Content-Type': 'application/json' };

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      ...JSON_HEADERS,
      ...(options.headers || {}),
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return response.json();
  }
  return response.text();
}

function buildQuery(filters, category, sort, order, limit, page) {
  const params = new URLSearchParams();
  params.set('category', category);
  params.set('sort', sort);
  params.set('order', order);
  params.set('limit', String(limit));
  params.set('offset', String(page * limit));
  if (filters.query) {
    params.set('q', filters.query);
  }
  filters.languages.forEach((value) => params.append('language', value));
  filters.statuses.forEach((value) => params.append('status', value));
  if (filters.minSubscribers != null && filters.minSubscribers !== '') {
    params.set('min_subscribers', String(filters.minSubscribers));
  }
  if (filters.maxSubscribers != null && filters.maxSubscribers !== '') {
    params.set('max_subscribers', String(filters.maxSubscribers));
  }
  params.set('emails_only', filters.emailsOnly ? 'true' : 'false');
  params.set('unique_emails', filters.uniqueEmails ? 'true' : 'false');
  params.set('email_gate_only', filters.emailGateOnly ? 'true' : 'false');
  return params.toString();
}

export async function fetchChannels(category, filters, sort, order, limit, page) {
  const query = buildQuery(filters, category, sort, order, limit, page);
  return request(`/api/channels?${query}`);
}

export async function archiveChannels(channelIds) {
  return request('/api/channels/archive_bulk', {
    method: 'POST',
    body: JSON.stringify({ channel_ids: channelIds }),
  });
}

export async function archiveByFilter(category, filters, sort, order, limit, page) {
  const query = buildQuery(filters, category, sort, order, limit, page);
  const body = {};
  if (filters.emailsOnly) {
    body.filter = 'emails_only';
  }
  return request(`/api/channels/archive_bulk?${query}`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function blacklistChannels(channelIds, category) {
  const query = category ? `?category=${encodeURIComponent(category)}` : '';
  return request(`/api/channels/blacklist_bulk${query}`, {
    method: 'POST',
    body: JSON.stringify({ channel_ids: channelIds }),
  });
}

export async function blacklistByFilter(category, filters, sort, order, limit, page) {
  const query = buildQuery(filters, category, sort, order, limit, page);
  const body = {};
  if (filters.emailsOnly) {
    body.filter = 'emails_only';
  }
  return request(`/api/channels/blacklist_bulk?${query}`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function restoreChannels(channelIds, category) {
  const query = `?category=${encodeURIComponent(category)}`;
  return request(`/api/channels/restore_bulk${query}`, {
    method: 'POST',
    body: JSON.stringify({ channel_ids: channelIds }),
  });
}

export async function restoreByFilter(category, filters, sort, order, limit, page) {
  const query = buildQuery(filters, category, sort, order, limit, page);
  const body = {};
  if (filters.emailsOnly) {
    body.filter = 'emails_only';
  }
  return request(`/api/channels/restore_bulk?${query}`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function discoverChannels(keywords, perKeyword) {
  return request('/api/discover', {
    method: 'POST',
    body: JSON.stringify({ keywords, perKeyword }),
  });
}

export async function startEnrichment(mode, limit, options = {}) {
  return request('/api/enrich', {
    method: 'POST',
    body: JSON.stringify({
      mode,
      limit: limit ?? null,
      forceRun: Boolean(options.forceRun),
      neverReenrich: Boolean(options.neverReenrich),
    }),
  });
}

export async function fetchStats() {
  return request('/api/stats');
}

export async function downloadCsv(category, filters, sort, order) {
  const query = buildQuery(filters, category, sort, order, 10000, 0);
  const response = await fetch(`/api/export/csv?${query}`, {
    headers: {
      ...JSON_HEADERS,
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || 'Failed to export CSV');
  }
  return response.text();
}

export async function importBlacklist(file) {
  const formData = new FormData();
  formData.append('file', file);
  const response = await fetch('/api/blacklist/import', {
    method: 'POST',
    body: formData,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || 'Import failed');
  }
  return response.json();
}

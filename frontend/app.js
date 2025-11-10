import {
  archiveByFilter,
  archiveChannels,
  blacklistByFilter,
  blacklistChannels,
  discoverChannels,
  downloadCsv,
  fetchChannels,
  fetchStats,
  importBlacklist,
  restoreByFilter,
  restoreChannels,
  startEnrichment,
} from './api.js';

const Category = {
  ACTIVE: 'active',
  ARCHIVED: 'archived',
  BLACKLISTED: 'blacklisted',
};

const CategoryInfo = {
  [Category.ACTIVE]: {
    icon: '🟢',
    label: 'Active',
    title: 'Active channels',
    subtitle: 'Fresh leads waiting for outreach.',
    primaryAction: { text: '🟡 Archive (filtered)', tone: 'yellow' },
    secondaryAction: { text: '🔴 Blacklist (filtered)', tone: 'red' },
  },
  [Category.ARCHIVED]: {
    icon: '🟡',
    label: 'Archived',
    title: 'Archived channels',
    subtitle: 'Previously contacted or paused leads.',
    primaryAction: { text: '🟢 Restore (filtered)', tone: 'green' },
    secondaryAction: { text: '🔴 Blacklist (filtered)', tone: 'red' },
  },
  [Category.BLACKLISTED]: {
    icon: '🔴',
    label: 'Blacklisted',
    title: 'Blacklisted channels',
    subtitle: 'Channels hidden from future discovery.',
    primaryAction: { text: '🟢 Restore (filtered)', tone: 'green' },
    secondaryAction: null,
  },
};

const StatusToneClass = {
  info: 'status-bar--info',
  success: 'status-bar--success',
  error: 'status-bar--error',
};

const StatusBadgeClass = {
  [Category.ACTIVE]: 'status-chip--success',
  [Category.ARCHIVED]: 'status-chip--warning',
  [Category.BLACKLISTED]: 'status-chip--danger',
};

const StatusBadgeIcon = {
  [Category.ACTIVE]: '🟢',
  [Category.ARCHIVED]: '🟡',
  [Category.BLACKLISTED]: '🔴',
};

function defaultFilters() {
  return {
    query: '',
    languages: [],
    statuses: [],
    minSubscribers: '',
    maxSubscribers: '',
    emailsOnly: false,
    uniqueEmails: false,
  };
}

function createTableState() {
  return {
    filters: defaultFilters(),
    sort: 'created_at',
    order: 'desc',
    page: 0,
    limit: 50,
    rows: [],
    total: 0,
    loading: false,
    error: null,
  };
}

function formatNumber(value) {
  if (value == null) {
    return '—';
  }
  return new Intl.NumberFormat().format(value);
}

function formatDate(value) {
  if (!value) {
    return '—';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
}

function parseList(value) {
  if (!value) {
    return [];
  }
  return value
    .split(/[\s,;]+/)
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

function escapeHtml(value) {
  if (value == null) {
    return '';
  }
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
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
  for (const item of items) {
    const emails = extractEmails(item.emails || '');
    if (!emails.length) {
      continue;
    }
    const unique = [];
    for (const email of emails) {
      const key = email.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(email);
      }
    }
    if (unique.length) {
      filtered.push({ ...item, emails: unique.join(', ') });
    }
  }
  return filtered;
}

class Dashboard {
  constructor(root) {
    this.root = root;
    this.activeTab = Category.ACTIVE;
    this.tables = {
      [Category.ACTIVE]: createTableState(),
      [Category.ARCHIVED]: createTableState(),
      [Category.BLACKLISTED]: createTableState(),
    };
    this.stats = null;
    this.statusTimeout = null;
    this.eventSource = null;
    this.enrichmentBusy = false;
    this.filterDebounce = null;
  }

  init() {
    this.cacheElements();
    this.bindEvents();
    this.renderTabs();
    this.applyFiltersToUI(this.tables[this.activeTab].filters);
    this.updateSortInputs();
    this.updatePagination();
    this.renderTable(true);
    this.updateQuickStats();
    this.updateStatusBar('', 'info');
    this.loadStats();
    this.loadTable(this.activeTab);
  }

  cacheElements() {
    this.el = {
      quickStats: this.root.querySelector('#quickStats'),
      tabButtons: Array.from(this.root.querySelectorAll('[data-category]')),
      tableTitle: this.root.querySelector('#tableTitle'),
      tableSubtitle: this.root.querySelector('#tableSubtitle'),
      bulkPrimaryBtn: this.root.querySelector('#bulkPrimaryBtn'),
      bulkSecondaryBtn: this.root.querySelector('#bulkSecondaryBtn'),
      exportCsvBtn: this.root.querySelector('#exportCsvBtn'),
      sortSelect: this.root.querySelector('#sortSelect'),
      orderSelect: this.root.querySelector('#orderSelect'),
      tableBody: this.root.querySelector('#tableBody'),
      tableStatus: this.root.querySelector('#tableStatus'),
      paginationLabel: this.root.querySelector('#paginationLabel'),
      prevPageBtn: this.root.querySelector('#prevPageBtn'),
      nextPageBtn: this.root.querySelector('#nextPageBtn'),
      statusBar: this.root.querySelector('#statusBar'),
      progressBar: this.root.querySelector('#progressBar'),
      summaryBar: this.root.querySelector('#summaryBar'),
      discoverKeywords: this.root.querySelector('#discoverKeywords'),
      discoverPerKeyword: this.root.querySelector('#discoverPerKeyword'),
      discoverBtn: this.root.querySelector('#discoverBtn'),
      enrichBtn: this.root.querySelector('#enrichBtn'),
      enrichEmailBtn: this.root.querySelector('#enrichEmailBtn'),
      enrichLimit: this.root.querySelector('#enrichLimit'),
      importBlacklistBtn: this.root.querySelector('#importBlacklistBtn'),
      modal: document.getElementById('blacklistModal'),
      modalClose: document.getElementById('blacklistCloseBtn'),
      modalCancel: document.getElementById('blacklistCancelBtn'),
      modalForm: document.getElementById('blacklistForm'),
      modalFile: document.getElementById('blacklistFile'),
      modalSummary: document.getElementById('blacklistSummary'),
      modalSubmit: document.getElementById('blacklistSubmitBtn'),
      filterQuery: this.root.querySelector('#filterQuery'),
      filterLanguages: this.root.querySelector('#filterLanguages'),
      filterMinSubs: this.root.querySelector('#filterMinSubs'),
      filterMaxSubs: this.root.querySelector('#filterMaxSubs'),
      filterEmailsOnly: this.root.querySelector('#filterEmailsOnly'),
      filterUniqueEmails: this.root.querySelector('#filterUniqueEmails'),
      filterStatusCheckboxes: Array.from(this.root.querySelectorAll('input[name="statusFilter"]')),
    };
  }

  bindEvents() {
    this.el.tabButtons.forEach((button) => {
      button.addEventListener('click', () => {
        const category = button.dataset.category;
        if (category && category !== this.activeTab) {
          this.changeTab(category);
        }
      });
    });

    this.el.sortSelect.addEventListener('change', () => {
      this.updateSort(this.el.sortSelect.value);
    });
    this.el.orderSelect.addEventListener('change', () => {
      this.updateOrder(this.el.orderSelect.value);
    });

    const scheduleFilters = () => {
      clearTimeout(this.filterDebounce);
      this.filterDebounce = setTimeout(() => {
        this.applyFiltersFromUI();
      }, 250);
    };

    this.el.filterQuery.addEventListener('input', scheduleFilters);
    this.el.filterLanguages.addEventListener('change', scheduleFilters);
    this.el.filterMinSubs.addEventListener('change', scheduleFilters);
    this.el.filterMaxSubs.addEventListener('change', scheduleFilters);
    this.el.filterEmailsOnly.addEventListener('change', () => {
      this.el.filterUniqueEmails.disabled = !this.el.filterEmailsOnly.checked;
      if (!this.el.filterEmailsOnly.checked) {
        this.el.filterUniqueEmails.checked = false;
      }
      scheduleFilters();
    });
    this.el.filterUniqueEmails.addEventListener('change', scheduleFilters);
    this.el.filterStatusCheckboxes.forEach((checkbox) => {
      checkbox.addEventListener('change', scheduleFilters);
    });

    this.el.prevPageBtn.addEventListener('click', () => this.changePage(-1));
    this.el.nextPageBtn.addEventListener('click', () => this.changePage(1));

    this.el.tableBody.addEventListener('click', (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const button = target.closest('button[data-action]');
      if (!button) {
        return;
      }
      const action = button.dataset.action;
      const channelId = button.dataset.id;
      if (!action || !channelId) {
        return;
      }
      this.handleRowAction(action, channelId);
    });

    this.el.bulkPrimaryBtn.addEventListener('click', () => this.handleBulkPrimary());
    this.el.bulkSecondaryBtn.addEventListener('click', () => this.handleBulkSecondary());
    this.el.exportCsvBtn.addEventListener('click', () => this.handleExportCsv());

    this.el.discoverBtn.addEventListener('click', () => this.handleDiscover());
    this.el.enrichBtn.addEventListener('click', () => this.handleEnrich('full'));
    this.el.enrichEmailBtn.addEventListener('click', () => this.handleEnrich('email_only'));

    this.el.importBlacklistBtn.addEventListener('click', () => this.openModal());
    this.el.modalClose.addEventListener('click', () => this.closeModal());
    this.el.modalCancel.addEventListener('click', () => this.closeModal());
    this.el.modal.addEventListener('click', (event) => {
      if (event.target === this.el.modal) {
        this.closeModal();
      }
    });

    this.el.modalForm.addEventListener('submit', (event) => {
      event.preventDefault();
      this.handleBlacklistImport();
    });
  }

  renderTabs() {
    this.el.tabButtons.forEach((button) => {
      const category = button.dataset.category;
      if (!category) {
        return;
      }
      const info = CategoryInfo[category];
      if (!info) {
        return;
      }
      button.innerHTML = `${info.icon} ${info.label} <span class="tab-count" data-count-for="${category}">(0)</span>`;
      button.classList.toggle('is-active', category === this.activeTab);
    });
  }

  applyFiltersToUI(filters) {
    this.el.filterQuery.value = filters.query || '';
    this.el.filterLanguages.value = filters.languages.join(', ');
    this.el.filterMinSubs.value = filters.minSubscribers ?? '';
    this.el.filterMaxSubs.value = filters.maxSubscribers ?? '';
    this.el.filterEmailsOnly.checked = Boolean(filters.emailsOnly);
    this.el.filterUniqueEmails.checked = Boolean(filters.uniqueEmails);
    this.el.filterUniqueEmails.disabled = !filters.emailsOnly;
    this.el.filterStatusCheckboxes.forEach((checkbox) => {
      checkbox.checked = filters.statuses.includes(checkbox.value);
    });
  }

  updateSortInputs() {
    const table = this.tables[this.activeTab];
    this.el.sortSelect.value = table.sort;
    this.el.orderSelect.value = table.order;
  }

  updateQuickStats() {
    if (!this.el.quickStats) {
      return;
    }
    if (!this.stats) {
      this.el.quickStats.innerHTML = '<span class="quick-stats__loading">Loading stats…</span>';
      return;
    }
    const active = formatNumber(this.stats[Category.ACTIVE] || 0);
    const archived = formatNumber(this.stats[Category.ARCHIVED] || 0);
    const blacklisted = formatNumber(this.stats[Category.BLACKLISTED] || 0);
    const unique = formatNumber(this.stats.unique_emails || 0);
    this.el.quickStats.innerHTML = `
      <span>🟢 Active: ${active}</span>
      <span>🟡 Archived: ${archived}</span>
      <span>🔴 Blacklisted: ${blacklisted}</span>
      <span>Unique emails: ${unique}</span>
    `;
    this.el.tabButtons.forEach((button) => {
      const category = button.dataset.category;
      const countEl = button.querySelector('[data-count-for]');
      if (countEl && category) {
        countEl.textContent = `(${formatNumber(this.stats[category] || 0)})`;
      }
    });
  }

  updateStatusBar(message, tone = 'info') {
    if (!this.el.statusBar) {
      return;
    }
    clearTimeout(this.statusTimeout);
    if (!message) {
      this.el.statusBar.textContent = '';
      this.el.statusBar.hidden = true;
      this.el.statusBar.className = 'status-bar';
      return;
    }
    this.el.statusBar.textContent = message;
    this.el.statusBar.hidden = false;
    this.el.statusBar.className = `status-bar ${StatusToneClass[tone] || StatusToneClass.info}`;
    this.statusTimeout = setTimeout(() => {
      this.el.statusBar.hidden = true;
    }, 6000);
  }

  setProgress(message) {
    if (!this.el.progressBar) {
      return;
    }
    if (!message) {
      this.el.progressBar.hidden = true;
      this.el.progressBar.textContent = '';
    } else {
      this.el.progressBar.hidden = false;
      this.el.progressBar.textContent = message;
      this.el.progressBar.className = 'status-bar status-bar--info';
    }
  }

  setSummary(message, tone = 'info') {
    if (!this.el.summaryBar) {
      return;
    }
    if (!message) {
      this.el.summaryBar.hidden = true;
      this.el.summaryBar.textContent = '';
    } else {
      this.el.summaryBar.hidden = false;
      this.el.summaryBar.textContent = message;
      this.el.summaryBar.className = `status-bar ${StatusToneClass[tone] || StatusToneClass.info}`;
    }
  }

  async loadStats() {
    try {
      const stats = await fetchStats();
      this.stats = stats;
      this.updateQuickStats();
    } catch (error) {
      console.error('Failed to load stats', error);
      this.updateStatusBar('Failed to load statistics.', 'error');
    }
  }

  async loadTable(category, mutate) {
    const table = this.tables[category];
    const snapshot = mutate ? mutate({ ...table, filters: { ...table.filters } }) : { ...table, filters: { ...table.filters } };
    snapshot.loading = true;
    this.tables[category] = snapshot;
    if (category === this.activeTab) {
      this.renderTable(true);
    }
    try {
      const { items, total } = await fetchChannels(
        category,
        snapshot.filters,
        snapshot.sort,
        snapshot.order,
        snapshot.limit,
        snapshot.page,
      );
      let rows = items;
      if (snapshot.filters.emailsOnly && snapshot.filters.uniqueEmails) {
        rows = applyUniqueEmailFilter(items);
      }
      const effectiveTotal = snapshot.filters.emailsOnly && snapshot.filters.uniqueEmails ? rows.length : total;
      const maxPage = effectiveTotal > 0 ? Math.max(0, Math.ceil(effectiveTotal / snapshot.limit) - 1) : 0;
      if (snapshot.page > maxPage) {
        const newPage = Math.max(0, maxPage);
        const nextState = { ...snapshot, page: newPage, loading: true };
        this.tables[category] = nextState;
        if (category === this.activeTab) {
          this.renderTable(true);
        }
        await this.loadTable(category, (state) => ({ ...state, page: newPage, loading: true }));
        return;
      }
      this.tables[category] = {
        ...snapshot,
        rows,
        total: effectiveTotal,
        loading: false,
        error: null,
      };
    } catch (error) {
      this.tables[category] = {
        ...snapshot,
        rows: [],
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to load channels',
      };
      console.error('Failed to load channels', error);
    }
    if (category === this.activeTab) {
      this.renderTable();
      this.updatePagination();
    }
  }

  renderTable(loadingState = false) {
    const table = this.tables[this.activeTab];
    const info = CategoryInfo[this.activeTab];
    if (this.el.tableTitle) {
      this.el.tableTitle.textContent = `${info.icon} ${info.title}`;
    }
    if (this.el.tableSubtitle) {
      this.el.tableSubtitle.textContent = info.subtitle;
    }

    if (this.el.tableStatus) {
      if (loadingState) {
        this.el.tableStatus.textContent = 'Loading…';
      } else if (!table.rows.length) {
        this.el.tableStatus.textContent = 'No matching channels';
      } else {
        const startIndex = table.page * table.limit + 1;
        const endIndex = startIndex + table.rows.length - 1;
        this.el.tableStatus.textContent = `Showing ${startIndex} – ${endIndex} of ${formatNumber(table.total)}`;
      }
    }

    const primary = info.primaryAction;
    if (primary) {
      this.el.bulkPrimaryBtn.hidden = false;
      this.el.bulkPrimaryBtn.textContent = primary.text;
      this.el.bulkPrimaryBtn.className = `btn btn--${primary.tone}`;
      this.el.bulkPrimaryBtn.disabled = loadingState || this.enrichmentBusy;
    } else {
      this.el.bulkPrimaryBtn.hidden = true;
    }
    const secondary = info.secondaryAction;
    if (secondary) {
      this.el.bulkSecondaryBtn.hidden = false;
      this.el.bulkSecondaryBtn.textContent = secondary.text;
      this.el.bulkSecondaryBtn.className = `btn btn--${secondary.tone}`;
      this.el.bulkSecondaryBtn.disabled = loadingState || this.enrichmentBusy;
    } else {
      this.el.bulkSecondaryBtn.hidden = true;
    }
    if (this.el.exportCsvBtn) {
      this.el.exportCsvBtn.disabled = loadingState;
    }

    this.el.tabButtons.forEach((button) => {
      button.classList.toggle('is-active', button.dataset.category === this.activeTab);
    });

    if (loadingState) {
      this.el.tableBody.innerHTML = '<tr><td colspan="7" class="table-loading">Loading channels…</td></tr>';
      return;
    }

    if (table.error) {
      if (this.el.tableStatus) {
        this.el.tableStatus.textContent = 'Failed to load channels';
      }
      this.el.tableBody.innerHTML = `<tr><td colspan="7" class="table-error">${table.error}</td></tr>`;
      return;
    }

    if (!table.rows.length) {
      this.el.tableBody.innerHTML = '<tr><td colspan="7" class="table-empty">No channels match the current filters.</td></tr>';
      return;
    }

    const rowsHtml = table.rows
      .map((row) => this.renderRow(row))
      .join('');
    this.el.tableBody.innerHTML = rowsHtml;
    this.updatePagination();
  }

  renderRow(row) {
    const category = this.activeTab;
    const badgeClass = StatusBadgeClass[category] || 'status-chip';
    const badgeIcon = StatusBadgeIcon[category] || '';
    const statusText = row.status ? row.status.toUpperCase() : 'UNKNOWN';
    const statusReason = row.status_reason || row.last_error || '';
    const actions = [];
    if (category === Category.ACTIVE) {
      actions.push({ action: 'archive', label: '🟡 Archive', tone: 'yellow' });
      actions.push({ action: 'blacklist', label: '🔴 Blacklist', tone: 'red' });
    } else if (category === Category.ARCHIVED) {
      actions.push({ action: 'restore', label: '🟢 Restore', tone: 'green' });
      actions.push({ action: 'blacklist', label: '🔴 Blacklist', tone: 'red' });
    } else if (category === Category.BLACKLISTED) {
      actions.push({ action: 'restore', label: '🟢 Restore', tone: 'green' });
    }

    const safeIdAttr = escapeHtml(row.channel_id);
    const actionButtons = actions
      .map(
        (action) =>
          `<button class="btn btn--${action.tone}" data-action="${action.action}" data-id="${safeIdAttr}">${action.label}</button>`
      )
      .join('');

    const language = row.language ? escapeHtml(row.language.toUpperCase()) : '—';
    const subscribers = row.subscribers != null ? formatNumber(row.subscribers) : '—';
    const rawUrl = row.url && /^https?:/i.test(row.url)
      ? row.url
      : `https://www.youtube.com/channel/${row.channel_id}`;
    const url = escapeHtml(rawUrl);
    const name = escapeHtml(row.name || 'Unnamed channel');
    const channelId = escapeHtml(row.channel_id);
    const emails = row.emails ? escapeHtml(row.emails) : '—';
    const reason = statusReason ? escapeHtml(statusReason) : '';

    return `
      <tr>
        <td class="name-cell">
          <span class="name-primary">${name}</span>
          <a class="name-link" href="${url}" target="_blank" rel="noopener noreferrer">${channelId}</a>
        </td>
        <td>${subscribers}</td>
        <td>${language}</td>
        <td class="emails-cell">${emails}</td>
        <td>
          <span class="status-chip ${badgeClass}">${badgeIcon} ${escapeHtml(statusText)}</span>
          ${reason ? `<span class="status-reason">${reason}</span>` : ''}
        </td>
        <td>${formatDate(row.last_updated || row.created_at)}</td>
        <td class="actions-cell">${actionButtons}</td>
      </tr>
    `;
  }

  updatePagination() {
    const table = this.tables[this.activeTab];
    const totalPages = Math.max(1, Math.ceil(table.total / table.limit));
    const currentPage = Math.min(table.page + 1, totalPages);
    this.el.paginationLabel.textContent = `Page ${currentPage} of ${totalPages} • Showing ${table.rows.length} / ${formatNumber(table.total)} results`;
    this.el.prevPageBtn.disabled = table.page === 0;
    this.el.nextPageBtn.disabled = table.page + 1 >= totalPages;
  }

  applyFiltersFromUI() {
    const filters = {
      query: this.el.filterQuery.value.trim(),
      languages: parseList(this.el.filterLanguages.value),
      minSubscribers: this.el.filterMinSubs.value ? Number(this.el.filterMinSubs.value) : '',
      maxSubscribers: this.el.filterMaxSubs.value ? Number(this.el.filterMaxSubs.value) : '',
      emailsOnly: this.el.filterEmailsOnly.checked,
      uniqueEmails: this.el.filterUniqueEmails.checked,
      statuses: this.el.filterStatusCheckboxes.filter((c) => c.checked).map((c) => c.value),
    };
    this.tables[this.activeTab].filters = filters;
    this.tables[this.activeTab].page = 0;
    this.loadTable(this.activeTab, (state) => ({
      ...state,
      filters,
      page: 0,
      loading: true,
    }));
  }

  changeTab(category) {
    this.activeTab = category;
    this.renderTabs();
    this.applyFiltersToUI(this.tables[category].filters);
    this.updateSortInputs();
    this.renderTable();
    this.updatePagination();
    if (!this.tables[category].rows.length && !this.tables[category].loading) {
      this.loadTable(category, (state) => ({ ...state, loading: true }));
    }
  }

  updateSort(sort) {
    const table = this.tables[this.activeTab];
    table.sort = sort;
    table.page = 0;
    this.loadTable(this.activeTab, (state) => ({ ...state, sort, page: 0, loading: true }));
  }

  updateOrder(order) {
    const table = this.tables[this.activeTab];
    table.order = order;
    table.page = 0;
    this.loadTable(this.activeTab, (state) => ({ ...state, order, page: 0, loading: true }));
  }

  changePage(delta) {
    const table = this.tables[this.activeTab];
    const nextPage = table.page + delta;
    if (nextPage < 0) {
      return;
    }
    const totalPages = Math.ceil(table.total / table.limit);
    if (totalPages && nextPage >= totalPages) {
      return;
    }
    table.page = nextPage;
    this.loadTable(this.activeTab, (state) => ({ ...state, page: nextPage, loading: true }));
  }

  async handleRowAction(action, channelId) {
    try {
      if (action === 'archive') {
        await archiveChannels([channelId]);
        this.updateStatusBar('Channel archived.', 'success');
        await this.afterMove(Category.ACTIVE, Category.ARCHIVED);
      } else if (action === 'blacklist') {
        await blacklistChannels([channelId], this.activeTab);
        this.updateStatusBar('Channel blacklisted.', 'success');
        await this.afterMove(this.activeTab, Category.BLACKLISTED);
      } else if (action === 'restore') {
        await restoreChannels([channelId], this.activeTab);
        this.updateStatusBar('Channel restored to Active.', 'success');
        await this.afterMove(this.activeTab, Category.ACTIVE);
      }
    } catch (error) {
      console.error('Row action failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'Action failed.', 'error');
    }
  }

  async handleBulkPrimary() {
    const info = CategoryInfo[this.activeTab];
    if (!info || !info.primaryAction) {
      return;
    }
    try {
      if (this.activeTab === Category.ACTIVE) {
        await archiveByFilter(
          this.activeTab,
          this.tables[this.activeTab].filters,
          this.tables[this.activeTab].sort,
          this.tables[this.activeTab].order,
          this.tables[this.activeTab].limit,
          this.tables[this.activeTab].page,
        );
        this.updateStatusBar('Filtered channels archived.', 'success');
        await this.afterMove(Category.ACTIVE, Category.ARCHIVED);
      } else if (this.activeTab === Category.ARCHIVED) {
        await restoreByFilter(
          this.activeTab,
          this.tables[this.activeTab].filters,
          this.tables[this.activeTab].sort,
          this.tables[this.activeTab].order,
          this.tables[this.activeTab].limit,
          this.tables[this.activeTab].page,
        );
        this.updateStatusBar('Filtered channels restored.', 'success');
        await this.afterMove(Category.ARCHIVED, Category.ACTIVE);
      } else if (this.activeTab === Category.BLACKLISTED) {
        await restoreByFilter(
          this.activeTab,
          this.tables[this.activeTab].filters,
          this.tables[this.activeTab].sort,
          this.tables[this.activeTab].order,
          this.tables[this.activeTab].limit,
          this.tables[this.activeTab].page,
        );
        this.updateStatusBar('Blacklisted channels restored to Active.', 'success');
        await this.afterMove(Category.BLACKLISTED, Category.ACTIVE);
      }
    } catch (error) {
      console.error('Bulk primary action failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'Bulk action failed.', 'error');
    }
  }

  async handleBulkSecondary() {
    const info = CategoryInfo[this.activeTab];
    if (!info || !info.secondaryAction) {
      return;
    }
    try {
      if (this.activeTab === Category.ACTIVE) {
        await blacklistByFilter(
          this.activeTab,
          this.tables[this.activeTab].filters,
          this.tables[this.activeTab].sort,
          this.tables[this.activeTab].order,
          this.tables[this.activeTab].limit,
          this.tables[this.activeTab].page,
        );
        this.updateStatusBar('Filtered channels blacklisted.', 'success');
        await this.afterMove(Category.ACTIVE, Category.BLACKLISTED);
      } else if (this.activeTab === Category.ARCHIVED) {
        await blacklistByFilter(
          this.activeTab,
          this.tables[this.activeTab].filters,
          this.tables[this.activeTab].sort,
          this.tables[this.activeTab].order,
          this.tables[this.activeTab].limit,
          this.tables[this.activeTab].page,
        );
        this.updateStatusBar('Archived channels blacklisted.', 'success');
        await this.afterMove(Category.ARCHIVED, Category.BLACKLISTED);
      }
    } catch (error) {
      console.error('Bulk secondary action failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'Bulk action failed.', 'error');
    }
  }

  async afterMove(source, destination) {
    await this.loadStats();
    await this.loadTable(source, (state) => ({ ...state, loading: true }));
    if (destination && destination !== source) {
      await this.loadTable(destination, (state) => ({ ...state, loading: true }));
    }
  }

  async handleExportCsv() {
    try {
      const csv = await downloadCsv(
        this.activeTab,
        this.tables[this.activeTab].filters,
        this.tables[this.activeTab].sort,
        this.tables[this.activeTab].order,
      );
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      link.href = url;
      link.download = `${this.activeTab}-channels-${timestamp}.csv`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      this.updateStatusBar('CSV exported successfully.', 'success');
    } catch (error) {
      console.error('CSV export failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'CSV export failed.', 'error');
    }
  }

  parseKeywordsInput() {
    return this.el.discoverKeywords.value
      .split(/[\n,]+/)
      .map((word) => word.trim())
      .filter(Boolean);
  }

  async handleDiscover() {
    const keywords = this.parseKeywordsInput();
    const perKeyword = Number(this.el.discoverPerKeyword.value) || 5;
    if (!keywords.length) {
      this.updateStatusBar('Please provide at least one keyword.', 'error');
      return;
    }
    try {
      this.updateStatusBar('Starting discovery…', 'info');
      const response = await discoverChannels(keywords, perKeyword);
      this.updateStatusBar(`Discovered ${response.found} new channels.`, 'success');
      await this.loadStats();
      await this.loadTable(Category.ACTIVE, (state) => ({ ...state, loading: true }));
    } catch (error) {
      console.error('Discovery failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'Discovery failed.', 'error');
    }
  }

  async handleEnrich(mode) {
    if (this.enrichmentBusy) {
      return;
    }
    const limitValue = this.el.enrichLimit.value ? Number(this.el.enrichLimit.value) : null;
    if (limitValue != null && (Number.isNaN(limitValue) || limitValue <= 0)) {
      this.updateStatusBar('Enrichment limit must be a positive number.', 'error');
      return;
    }
    try {
      this.enrichmentBusy = true;
      this.el.enrichBtn.disabled = true;
      this.el.enrichEmailBtn.disabled = true;
      this.renderTable();
      const { jobId, total } = await startEnrichment(mode, limitValue);
      this.updateStatusBar(`Enrichment job ${jobId} started.`, 'success');
      this.setSummary('', 'info');
      this.openEventSource(jobId, total);
    } catch (error) {
      this.enrichmentBusy = false;
      this.el.enrichBtn.disabled = false;
      this.el.enrichEmailBtn.disabled = false;
      this.renderTable();
      console.error('Enrichment failed', error);
      this.updateStatusBar(error instanceof Error ? error.message : 'Failed to start enrichment.', 'error');
    }
  }

  openEventSource(jobId, total) {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    this.setProgress(`Job ${jobId} queued…`);
    this.eventSource = new EventSource(`/api/enrich/stream/${jobId}`);
    this.eventSource.onmessage = async (event) => {
      if (!event.data) {
        return;
      }
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'progress') {
          const completed = payload.completed ?? 0;
          const pending = payload.pending ?? Math.max(0, (payload.total ?? total ?? 0) - completed);
          this.setProgress(
            `Job ${payload.jobId}: ${completed}/${payload.total ?? total ?? '?'} complete • ${pending} pending • ${payload.errors ?? 0} errors`
          );
          if (payload.done) {
            this.eventSource?.close();
            this.eventSource = null;
            this.enrichmentBusy = false;
            this.el.enrichBtn.disabled = false;
            this.el.enrichEmailBtn.disabled = false;
            this.setProgress('');
            this.setSummary('Enrichment job completed.', 'success');
            await this.loadStats();
            await this.loadTable(Category.ACTIVE, (state) => ({ ...state, loading: true }));
            if (this.activeTab !== Category.ACTIVE) {
              await this.loadTable(this.activeTab, (state) => ({ ...state, loading: true }));
            }
          }
        } else if (payload.type === 'error') {
          this.updateStatusBar(payload.message || 'Enrichment error encountered.', 'error');
        }
      } catch (err) {
        console.error('Failed to parse SSE payload', err);
      }
    };
    this.eventSource.onerror = (event) => {
      console.error('EventSource error', event);
      this.setProgress('');
      this.updateStatusBar('Connection lost to enrichment stream.', 'error');
      this.eventSource?.close();
      this.eventSource = null;
      this.enrichmentBusy = false;
      this.el.enrichBtn.disabled = false;
      this.el.enrichEmailBtn.disabled = false;
      this.renderTable();
    };
  }

  openModal() {
    this.el.modal.removeAttribute('hidden');
    this.el.modalSummary.innerHTML = '';
    this.el.modalSummary.className = 'modal-summary';
    this.el.modalForm.reset();
    if (this.el.modalFile) {
      this.el.modalFile.value = '';
      requestAnimationFrame(() => {
        try {
          if (typeof this.el.modalFile.showPicker === 'function') {
            this.el.modalFile.showPicker();
          } else {
            this.el.modalFile.click();
          }
        } catch (err) {
          console.warn('File picker showPicker failed, falling back to click()', err);
          this.el.modalFile.click();
        }
      });
    }
  }

  closeModal() {
    this.el.modal.setAttribute('hidden', 'true');
  }

  renderBlacklistImportResult(result) {
    const summaryEl = this.el.modalSummary;
    if (!summaryEl) {
      return;
    }
    if (!result || typeof result !== 'object') {
      summaryEl.textContent = 'Import completed.';
      summaryEl.className = 'modal-summary modal-summary--success';
      return;
    }

    const countsSource = result.counts || {};
    const counts = {
      created:
        typeof countsSource.created === 'number'
          ? countsSource.created
          : Array.isArray(result.created)
            ? result.created.length
            : 0,
      updated:
        typeof countsSource.updated === 'number'
          ? countsSource.updated
          : Array.isArray(result.updated)
            ? result.updated.length
            : 0,
      skipped:
        typeof countsSource.skipped === 'number'
          ? countsSource.skipped
          : Array.isArray(result.skipped)
            ? result.skipped.length
            : 0,
      unresolved:
        typeof countsSource.unresolved === 'number'
          ? countsSource.unresolved
          : Array.isArray(result.unresolved)
            ? result.unresolved.length
            : 0,
      processed:
        typeof countsSource.processed === 'number' ? countsSource.processed : null,
    };

    const summaryParts = [
      `${counts.created} created`,
      `${counts.updated} updated`,
      `${counts.skipped} skipped`,
      `${counts.unresolved} unresolved`,
    ];
    const summaryLine = `Imported: ${summaryParts.join(' • ')}`;
    let html = `<p class="modal-summary__headline">${escapeHtml(summaryLine)}</p>`;

    if (counts.processed != null) {
      const processedText = `Processed ${counts.processed} row${counts.processed === 1 ? '' : 's'}.`;
      html += `<p class="modal-summary__meta-line">${escapeHtml(processedText)}</p>`;
    }

    if (counts.skipped > 0) {
      html +=
        '<p class="modal-summary__meta-line">Skipped entries include duplicates or already blacklisted channels.</p>';
    }

    const unresolvedItems = Array.isArray(result.unresolved) ? result.unresolved : [];
    if (unresolvedItems.length) {
      const listItems = unresolvedItems
        .map((item) => {
          const normalized = typeof item?.normalized === 'string' ? item.normalized : '';
          const original = typeof item?.input === 'string' ? item.input : '';
          const inputValueRaw = normalized || original;
          const inputValue = escapeHtml(inputValueRaw || '[empty]');
          const metaParts = [];
          if (typeof item?.row === 'number') {
            metaParts.push(`row ${item.row}`);
          }
          if (typeof item?.column === 'string' && item.column) {
            metaParts.push(item.column);
          }
          const metaLabel = metaParts.length
            ? `<span class="modal-summary__meta">${escapeHtml(metaParts.join(' · '))}</span>`
            : '';
          const messageText = typeof item?.message === 'string' && item.message
            ? escapeHtml(item.message)
            : 'Unable to resolve channel.';
          const reasonText =
            typeof item?.reason === 'string' && item.reason && item.reason !== item.message
              ? `<span class="modal-summary__reason">[${escapeHtml(item.reason)}]</span>`
              : '';
          return `
            <li class="modal-summary__item">
              <div class="modal-summary__item-header">⚠️ <code class="modal-summary__code">${inputValue}</code>${metaLabel}</div>
              <div class="modal-summary__message">${messageText}${reasonText}</div>
            </li>
          `;
        })
        .join('');
      const openAttr = unresolvedItems.length <= 3 ? ' open' : '';
      html += `
        <details class="modal-summary__details"${openAttr}>
          <summary>Review unresolved (${unresolvedItems.length})</summary>
          <ul class="modal-summary__list">${listItems}</ul>
        </details>
      `;
    }

    summaryEl.innerHTML = html;
    const classes = ['modal-summary'];
    if (counts.created > 0 || counts.updated > 0) {
      classes.push('modal-summary--success');
    } else if (counts.unresolved > 0) {
      classes.push('modal-summary--error');
    }
    summaryEl.className = classes.join(' ');
  }

  async handleBlacklistImport() {
    const file = this.el.modalFile.files?.[0];
    if (!file) {
      this.el.modalSummary.textContent = 'Please choose a CSV file to import.';
      this.el.modalSummary.className = 'modal-summary modal-summary--error';
      return;
    }
    this.el.modalSubmit.disabled = true;
    this.el.modalCancel.disabled = true;
    this.el.modalSummary.innerHTML = '<p class="modal-summary__headline">Importing…</p>';
    this.el.modalSummary.className = 'modal-summary';
    try {
      const result = await importBlacklist(file);
      this.renderBlacklistImportResult(result);
      await this.loadStats();
      await this.loadTable(Category.BLACKLISTED, (state) => ({ ...state, loading: true }));
      this.updateStatusBar('Blacklist import completed.', 'success');
    } catch (error) {
      console.error('Blacklist import failed', error);
      this.el.modalSummary.textContent = error instanceof Error ? error.message : 'Import failed.';
      this.el.modalSummary.className = 'modal-summary modal-summary--error';
    } finally {
      this.el.modalSubmit.disabled = false;
      this.el.modalCancel.disabled = false;
    }
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const root = document.getElementById('app');
  if (!root) {
    throw new Error('App root not found');
  }
  const dashboard = new Dashboard(root);
  dashboard.init();
});


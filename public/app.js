(() => {
  const state = {
    tree: null,
    rootName: '',
    expanded: new Set(['/']),
    selectedPath: null,
    filters: [],
    suggestions: [],
    suggestionIndex: -1,
    searchQuery: '',
    searchResults: []
  };

  const els = {
    searchInput: document.getElementById('searchInput'),
    suggestions: document.getElementById('suggestions'),
    filters: document.getElementById('activeFilters'),
    tree: document.getElementById('tree'),
    entryMeta: document.getElementById('entryMeta'),
    tagSection: document.getElementById('tagSection'),
    tagList: document.getElementById('tagList'),
    tagForm: document.getElementById('tagForm'),
    tagKeyInput: document.getElementById('tagKeyInput'),
    tagValueInput: document.getElementById('tagValueInput'),
    tagFeedback: document.getElementById('tagFeedback'),
    refreshTags: document.getElementById('refreshTags'),
    results: document.getElementById('searchResults'),
    status: document.getElementById('statusMessage')
  };

  let reconnectTimer = null;
  let refreshTimer = null;
  let suggestionRequestId = 0;
  let lastEntry = null;

  function setStatus(message) {
    if (els.status) {
      els.status.textContent = message;
    }
  }

  async function fetchJSON(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || response.statusText);
    }
    return response.json();
  }

  function tokenizeSearch() {
    const value = els.searchInput.value;
    if (!value.trim()) {
      return [];
    }
    return value.trim().split(/\s+/);
  }

  function formatBytes(bytes) {
    if (typeof bytes !== 'number' || Number.isNaN(bytes)) {
      return '—';
    }
    if (bytes === 0) {
      return '0 B';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const num = bytes / Math.pow(1024, exponent);
    return `${num.toFixed(num >= 10 ? 0 : 1)} ${units[exponent]}`;
  }

  function collectPaths(node, set) {
    if (!node) return;
    set.add(node.path);
    if (node.children) {
      for (const child of node.children) {
        collectPaths(child, set);
      }
    }
  }

  async function fetchTree() {
    try {
      const data = await fetchJSON('/api/tree');
      state.tree = data.root || null;
      state.rootName = data.rootName || '';
      const available = new Set();
      if (state.tree) {
        collectPaths(state.tree, available);
      }
      const retained = new Set(['/']);
      for (const path of state.expanded) {
        if (available.has(path)) {
          retained.add(path);
        }
      }
      state.expanded = retained;
      if (state.selectedPath && !available.has(state.selectedPath)) {
        state.selectedPath = '/';
      }
      renderTree();
    } catch (err) {
      console.error('Failed to load tree', err);
      setStatus('Failed to load tree');
    }
  }

  function renderTree() {
    const container = els.tree;
    container.innerHTML = '';
    if (!state.tree) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'No files indexed yet.';
      container.appendChild(empty);
      return;
    }
    container.appendChild(buildTreeList(state.tree));
  }

  function buildTreeList(node) {
    const ul = document.createElement('ul');
    ul.appendChild(buildTreeItem(node));
    return ul;
  }

  function buildTreeItem(node) {
    const li = document.createElement('li');
    li.className = 'tree-item';

    const row = document.createElement('div');
    row.className = 'tree-row';
    if (state.selectedPath === node.path) {
      row.classList.add('selected');
    }

    if (node.type === 'directory') {
      const toggle = document.createElement('button');
      toggle.className = 'toggle';
      toggle.textContent = state.expanded.has(node.path) ? '▾' : '▸';
      toggle.addEventListener('click', (event) => {
        event.stopPropagation();
        toggleExpand(node.path);
      });
      row.appendChild(toggle);
    } else {
      const spacer = document.createElement('span');
      spacer.className = 'toggle spacer';
      spacer.textContent = '•';
      spacer.style.visibility = 'hidden';
      row.appendChild(spacer);
    }

    const label = document.createElement('span');
    label.className = 'tree-label';
    label.textContent = node.name || node.path || 'root';
    row.appendChild(label);

    const type = document.createElement('span');
    type.className = 'tree-type';
    type.textContent = node.type;
    row.appendChild(type);

    row.addEventListener('click', () => {
      selectPath(node.path);
    });

    li.appendChild(row);

    if (node.children && node.children.length && node.type === 'directory' && state.expanded.has(node.path)) {
      const childList = document.createElement('ul');
      for (const child of node.children) {
        childList.appendChild(buildTreeItem(child));
      }
      li.appendChild(childList);
    }

    return li;
  }

  function toggleExpand(path) {
    if (state.expanded.has(path)) {
      state.expanded.delete(path);
    } else {
      state.expanded.add(path);
    }
    renderTree();
  }

  function expandToPath(path) {
    if (!path || path === '/') {
      return;
    }
    const segments = path.split('/');
    let current = '';
    for (let i = 0; i < segments.length - 1; i += 1) {
      const segment = segments[i];
      if (!segment) continue;
      current = current ? `${current}/${segment}` : segment;
      state.expanded.add(current);
    }
    state.expanded.add('/');
  }

  async function selectPath(path) {
    if (!path) return;
    expandToPath(path);
    state.selectedPath = path;
    renderTree();
    await loadEntry(path);
  }

  async function loadEntry(path) {
    try {
      const data = await fetchJSON(`/api/entry?path=${encodeURIComponent(path)}`);
      lastEntry = data.entry;
      renderEntry(data.entry);
    } catch (err) {
      console.error('Failed to load entry', err);
      lastEntry = null;
      renderEntry(null, err.message);
    }
  }

  function renderEntry(entry, error) {
    if (!entry) {
      els.entryMeta.classList.add('empty');
      els.entryMeta.innerHTML = error ? `Error: ${error}` : 'Select a file or directory.';
      els.tagSection.classList.add('hidden');
      return;
    }

    els.entryMeta.classList.remove('empty');
    els.entryMeta.innerHTML = '';

    const nameRow = document.createElement('div');
    nameRow.innerHTML = `<strong>Name:</strong> ${entry.name}`;
    els.entryMeta.appendChild(nameRow);

    const pathRow = document.createElement('div');
    pathRow.className = 'path';
    pathRow.textContent = entry.path;
    els.entryMeta.appendChild(pathRow);

    const typeRow = document.createElement('div');
    typeRow.innerHTML = `<strong>Type:</strong> ${entry.type}`;
    els.entryMeta.appendChild(typeRow);

    const sizeRow = document.createElement('div');
    sizeRow.innerHTML = `<strong>Size:</strong> ${entry.type === 'file' && typeof entry.size === 'number' ? formatBytes(entry.size) : '—'}`;
    els.entryMeta.appendChild(sizeRow);

    const timeRow = document.createElement('div');
    const date = entry.mtime ? new Date(entry.mtime) : null;
    timeRow.innerHTML = `<strong>Modified:</strong> ${date ? date.toLocaleString() : '—'}`;
    els.entryMeta.appendChild(timeRow);

    els.tagSection.classList.remove('hidden');
    renderTags(entry.tags || []);
  }

  function renderTags(tags) {
    els.tagList.innerHTML = '';
    if (!tags.length) {
      const empty = document.createElement('div');
      empty.className = 'feedback';
      empty.textContent = 'No tags yet.';
      els.tagList.appendChild(empty);
      return;
    }
    for (const tag of tags) {
      const pill = document.createElement('span');
      pill.className = 'tag-pill';
      pill.textContent = `${tag.key}:${tag.value}`;
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = '×';
      button.addEventListener('click', async (event) => {
        event.stopPropagation();
        await removeTag(tag);
      });
      pill.appendChild(button);
      els.tagList.appendChild(pill);
    }
  }

  async function removeTag(tag) {
    if (!state.selectedPath) return;
    try {
      await fetchJSON('/api/tags', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: state.selectedPath, key: tag.key, value: tag.value })
      });
      els.tagFeedback.textContent = 'Tag removed';
      els.tagFeedback.classList.remove('error');
      await loadEntry(state.selectedPath);
      await updateSearchResults();
    } catch (err) {
      els.tagFeedback.textContent = `Tag removal failed: ${err.message}`;
      els.tagFeedback.classList.add('error');
    }
  }

  function renderFilters() {
    els.filters.innerHTML = '';
    if (!state.filters.length) {
      return;
    }
    for (const filter of state.filters) {
      const pill = document.createElement('span');
      pill.className = 'filter';
      pill.textContent = `${filter.key}:${filter.value}`;

      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = '×';
      button.addEventListener('click', () => {
        removeFilter(filter.key, filter.value);
      });

      pill.appendChild(button);
      els.filters.appendChild(pill);
    }
  }

  function addFilter(raw) {
    const [key, value] = raw.split(':');
    if (!key || !value) {
      return;
    }
    const exists = state.filters.some((item) => item.key === key && item.value === value);
    if (!exists) {
      state.filters.push({ key, value });
      renderFilters();
      updateSearchResults();
    }
  }

  function removeFilter(key, value) {
    state.filters = state.filters.filter((item) => !(item.key === key && item.value === value));
    renderFilters();
    updateSearchResults();
  }

  async function updateSearchResults() {
    const params = new URLSearchParams();
    const query = state.searchQuery.trim();
    if (query) {
      params.set('q', query);
    }
    if (state.filters.length) {
      params.set('tags', state.filters.map((item) => `${item.key}:${item.value}`).join(','));
    }
    const qs = params.toString();
    const url = qs ? `/api/search?${qs}` : '/api/search';
    try {
      const data = await fetchJSON(url);
      state.searchResults = data.results || [];
      renderSearchResults();
    } catch (err) {
      console.error('Search failed', err);
      setStatus('Search failed');
    }
  }

  function renderSearchResults() {
    els.results.innerHTML = '';
    if (!state.searchResults.length) {
      const empty = document.createElement('div');
      empty.className = 'feedback';
      empty.textContent = 'No results';
      els.results.appendChild(empty);
      return;
    }

    for (const result of state.searchResults) {
      const item = document.createElement('div');
      item.className = 'result-item';

      const title = document.createElement('div');
      title.className = 'result-title';
      title.textContent = `${result.name} (${result.type})`;
      item.appendChild(title);

      const path = document.createElement('div');
      path.className = 'result-path';
      path.textContent = result.path;
      item.appendChild(path);

      if (result.tags && result.tags.length) {
        const tagsRow = document.createElement('div');
        tagsRow.className = 'result-tags';
        for (const tag of result.tags) {
          const span = document.createElement('span');
          span.className = 'tag-pill';
          span.textContent = `${tag.key}:${tag.value}`;
          tagsRow.appendChild(span);
        }
        item.appendChild(tagsRow);
      }

      item.addEventListener('click', () => {
        selectPath(result.path);
      });

      els.results.appendChild(item);
    }
  }

  function hideSuggestions() {
    els.suggestions.classList.add('hidden');
  }

  function renderSuggestions() {
    const container = els.suggestions;
    container.innerHTML = '';
    if (!state.suggestions.length) {
      container.classList.add('hidden');
      return;
    }
    container.classList.remove('hidden');
    state.suggestions.forEach((suggestion, index) => {
      const row = document.createElement('div');
      row.className = 'suggestion-item';
      if (index === state.suggestionIndex) {
        row.classList.add('active');
      }
      const label = document.createElement('span');
      label.textContent = suggestion.value;
      const type = document.createElement('span');
      type.className = 'type';
      type.textContent = suggestion.type;
      row.appendChild(label);
      row.appendChild(type);
      row.addEventListener('mousedown', (event) => {
        event.preventDefault();
        applySuggestion(suggestion);
      });
      container.appendChild(row);
    });
  }

  async function updateSuggestions(query) {
    const currentId = ++suggestionRequestId;
    try {
      const data = await fetchJSON(`/api/suggestions?q=${encodeURIComponent(query)}`);
      if (currentId !== suggestionRequestId) {
        return;
      }
      state.suggestions = data.suggestions || [];
      state.suggestionIndex = state.suggestions.length ? 0 : -1;
      renderSuggestions();
    } catch (err) {
      console.error('Suggestion fetch failed', err);
      hideSuggestions();
    }
  }

  function applySuggestion(suggestion) {
    if (!suggestion) return;
    const tokens = tokenizeSearch();
    if (suggestion.type === 'tag') {
      if (tokens.length) {
        tokens.pop();
      }
      els.searchInput.value = tokens.join(' ');
      state.searchQuery = els.searchInput.value;
      hideSuggestions();
      addFilter(suggestion.value);
      els.searchInput.focus();
      return;
    }
    if (suggestion.type === 'tagKey') {
      if (!tokens.length) {
        tokens.push(suggestion.value);
      } else {
        tokens[tokens.length - 1] = suggestion.value;
      }
      const updated = tokens.join(' ');
      els.searchInput.value = updated;
      state.searchQuery = updated;
      els.searchInput.focus();
      updateSuggestions(updated);
      return;
    }
    if (suggestion.type === 'path') {
      els.searchInput.value = suggestion.value;
      state.searchQuery = suggestion.value;
      hideSuggestions();
      updateSearchResults();
      selectPath(suggestion.value);
    }
  }

  function handleSearchInput() {
    state.searchQuery = els.searchInput.value;
    updateSuggestions(state.searchQuery);
  }

  function handleSearchKeydown(event) {
    if (!state.suggestions.length) {
      if (event.key === 'Enter') {
        event.preventDefault();
        updateSearchResults();
      }
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      state.suggestionIndex = (state.suggestionIndex + 1) % state.suggestions.length;
      renderSuggestions();
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      state.suggestionIndex = (state.suggestionIndex - 1 + state.suggestions.length) % state.suggestions.length;
      renderSuggestions();
    } else if (event.key === 'Tab') {
      if (state.suggestionIndex >= 0) {
        event.preventDefault();
        applySuggestion(state.suggestions[state.suggestionIndex]);
      }
    } else if (event.key === 'Enter') {
      event.preventDefault();
      if (state.suggestionIndex >= 0) {
        applySuggestion(state.suggestions[state.suggestionIndex]);
      } else {
        updateSearchResults();
      }
    } else if (event.key === 'Escape') {
      hideSuggestions();
      state.suggestions = [];
      state.suggestionIndex = -1;
    }
  }

  async function handleAddTag(event) {
    event.preventDefault();
    if (!state.selectedPath) return;
    const key = els.tagKeyInput.value.trim();
    const value = els.tagValueInput.value.trim();
    if (!key || !value) {
      els.tagFeedback.textContent = 'Both key and value are required';
      els.tagFeedback.classList.add('error');
      return;
    }
    try {
      await fetchJSON('/api/tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: state.selectedPath, key, value })
      });
      els.tagFeedback.textContent = 'Tag added';
      els.tagFeedback.classList.remove('error');
      els.tagKeyInput.value = '';
      els.tagValueInput.value = '';
      await loadEntry(state.selectedPath);
      await updateSearchResults();
    } catch (err) {
      els.tagFeedback.textContent = `Tag add failed: ${err.message}`;
      els.tagFeedback.classList.add('error');
    }
  }

  function scheduleRefresh() {
    if (refreshTimer) {
      return;
    }
    refreshTimer = setTimeout(async () => {
      refreshTimer = null;
      await fetchTree();
      await updateSearchResults();
      if (state.selectedPath) {
        await loadEntry(state.selectedPath);
      }
    }, 200);
  }

  function connectRealtime() {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${protocol}://${window.location.host}`);

    ws.addEventListener('open', () => {
      setStatus('Connected');
    });

    ws.addEventListener('message', (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleServerEvent(payload);
      } catch (err) {
        console.error('Failed to parse message', err);
      }
    });

    ws.addEventListener('close', () => {
      setStatus('Reconnecting…');
      reconnectTimer = setTimeout(connectRealtime, 2000);
    });

    ws.addEventListener('error', () => {
      ws.close();
    });
  }

  function handleServerEvent(event) {
    if (!event || !event.type) {
      return;
    }
    switch (event.type) {
      case 'entry-added':
      case 'entry-updated':
      case 'entry-removed':
        scheduleRefresh();
        break;
      case 'tag-added':
      case 'tag-removed':
        if (event.path === state.selectedPath) {
          loadEntry(state.selectedPath);
        }
        scheduleRefresh();
        break;
      default:
        break;
    }
  }

  function initialiseEvents() {
    els.searchInput.addEventListener('input', handleSearchInput);
    els.searchInput.addEventListener('keydown', handleSearchKeydown);
    els.searchInput.addEventListener('blur', () => {
      setTimeout(() => hideSuggestions(), 120);
    });

    els.tagForm.addEventListener('submit', handleAddTag);
    els.refreshTags.addEventListener('click', async () => {
      if (state.selectedPath) {
        await loadEntry(state.selectedPath);
      }
    });
  }

  async function bootstrap() {
    initialiseEvents();
    await fetchTree();
    state.selectedPath = '/';
    if (state.tree) {
      await loadEntry('/');
    }
    await updateSearchResults();
    updateSuggestions('');
    connectRealtime();
  }

  bootstrap().catch((err) => {
    console.error('Failed to bootstrap UI', err);
    setStatus('Failed to initialise UI');
  });
})();

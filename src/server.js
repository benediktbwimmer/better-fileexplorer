const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const http = require('http');
const express = require('express');
const chokidar = require('chokidar');
const Database = require('better-sqlite3');
const Fuse = require('fuse.js');
const { WebSocketServer } = require('ws');

const WS_OPEN = 1;

const PORT = parseInt(process.env.PORT, 10) || 4174;
const START_PATH = path.resolve(process.env.START_PATH || process.cwd());
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const OPENAPI_SPEC_PATH = path.join(__dirname, '..', 'openapi.yaml');
const DOCS_PAGE_PATH = path.join(PUBLIC_DIR, 'docs.html');

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.static(PUBLIC_DIR));

app.get('/openapi.yaml', (_req, res, next) => {
  res.type('application/yaml');
  res.sendFile(OPENAPI_SPEC_PATH, (err) => {
    if (err) {
      next(err);
    }
  });
});

app.get('/docs', (_req, res, next) => {
  res.sendFile(DOCS_PAGE_PATH, (err) => {
    if (err) {
      next(err);
    }
  });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const sockets = new Set();

wss.on('connection', (socket) => {
  sockets.add(socket);
  socket.on('close', () => sockets.delete(socket));
});

function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const socket of sockets) {
    if (socket.readyState === WS_OPEN) {
      socket.send(payload);
    }
  }
}

const db = new Database(':memory:');
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE entries (
    path TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_path TEXT,
    type TEXT NOT NULL CHECK(type IN ('file', 'directory')),
    size INTEGER,
    mtime INTEGER,
    extension TEXT,
    depth INTEGER NOT NULL
  );
  CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    entry_path TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE(entry_path, key, value),
    FOREIGN KEY(entry_path) REFERENCES entries(path) ON DELETE CASCADE
  );
`);

const statements = {
  upsertEntry: db.prepare(`
    INSERT INTO entries (path, name, parent_path, type, size, mtime, extension, depth)
    VALUES (@path, @name, @parent_path, @type, @size, @mtime, @extension, @depth)
    ON CONFLICT(path) DO UPDATE SET
      name = excluded.name,
      parent_path = excluded.parent_path,
      type = excluded.type,
      size = excluded.size,
      mtime = excluded.mtime,
      extension = excluded.extension,
      depth = excluded.depth
  `),
  removeEntry: db.prepare('DELETE FROM entries WHERE path = ?'),
  removeEntriesUnder: db.prepare(`DELETE FROM entries WHERE path LIKE ?`),
  allEntries: db.prepare('SELECT * FROM entries ORDER BY depth ASC, name COLLATE NOCASE ASC'),
  singleEntry: db.prepare('SELECT * FROM entries WHERE path = ?'),
  allTags: db.prepare('SELECT entry_path, key, value FROM tags'),
  entryTags: db.prepare('SELECT key, value FROM tags WHERE entry_path = ? ORDER BY key, value'),
  addTag: db.prepare('INSERT OR IGNORE INTO tags (entry_path, key, value) VALUES (?, ?, ?)'),
  deleteTag: db.prepare('DELETE FROM tags WHERE entry_path = ? AND key = ? AND value = ?'),
  pathsForTag: db.prepare('SELECT entry_path FROM tags WHERE key = ? AND value = ?'),
  deleteTagsForEntry: db.prepare('DELETE FROM tags WHERE entry_path = ?'),
  deleteTagsUnder: db.prepare('DELETE FROM tags WHERE entry_path LIKE ?')
};

let entryCache = [];
let entryFuse = new Fuse([], { keys: ['path', 'name'], threshold: 0.3, ignoreLocation: true });
let tagCache = [];
let tagFuse = new Fuse([], { keys: ['key', 'value', 'pair'], threshold: 0.3, ignoreLocation: true });
let tagValuesByKey = new Map();
const UNSUPPORTED_FS_CODES = new Set(['ENOTSUP', 'EOPNOTSUPP', 'EPERM', 'EACCES']);
const unsupportedPaths = new Set();

function markPathUnsupported(absolutePath) {
  if (!absolutePath) {
    return;
  }
  unsupportedPaths.add(absolutePath);
  const relative = normalizeRelative(absolutePath);
  if (relative && relative !== '/' && !relative.startsWith('..')) {
    unsupportedPaths.add(relative);
  }
}

function shouldIgnorePath(absolutePath) {
  if (!absolutePath) {
    return false;
  }
  if (unsupportedPaths.has(absolutePath)) {
    return true;
  }
  const relative = normalizeRelative(absolutePath);
  if (unsupportedPaths.has(relative)) {
    return true;
  }
  if (relative.startsWith('..')) {
    return false;
  }
  const lowerExt = path.extname(relative).toLowerCase();
  if (lowerExt === '.sock') {
    return true;
  }
  return false;
}

function normalizeRelative(absolutePath) {
  const relative = path.relative(START_PATH, absolutePath);
  if (!relative) {
    return '/';
  }
  const normalized = relative.split(path.sep).join('/');
  return normalized;
}

function absoluteFromRelative(relativePath) {
  if (relativePath === '/' || !relativePath) {
    return START_PATH;
  }
  return path.join(START_PATH, ...relativePath.split('/'));
}

function parentOf(relativePath) {
  if (relativePath === '/' || !relativePath) {
    return null;
  }
  const idx = relativePath.lastIndexOf('/');
  if (idx <= 0) {
    return '/';
  }
  return relativePath.slice(0, idx);
}

function depthOf(relativePath) {
  if (relativePath === '/' || !relativePath) {
    return 0;
  }
  return relativePath.split('/').length;
}

function entryFromStats(relativePath, stats) {
  const name = relativePath === '/' ? path.basename(START_PATH) : path.basename(relativePath);
  const type = stats.isDirectory() ? 'directory' : 'file';
  const extension = type === 'file' ? path.extname(relativePath).replace(/^\./, '').toLowerCase() : '';
  return {
    path: relativePath,
    name,
    parent_path: parentOf(relativePath),
    type,
    size: type === 'file' ? stats.size : null,
    mtime: stats.mtimeMs ? Math.round(stats.mtimeMs) : Date.now(),
    extension,
    depth: depthOf(relativePath)
  };
}

async function scanInitialTree() {
  const seen = new Set();
  async function walk(absolutePath) {
    if (shouldIgnorePath(absolutePath)) {
      return;
    }
    let stats;
    try {
      stats = await fsp.stat(absolutePath);
    } catch (err) {
      if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
        markPathUnsupported(absolutePath);
      }
      return;
    }
    const relativePath = normalizeRelative(absolutePath);
    if (seen.has(relativePath)) {
      return;
    }
    seen.add(relativePath);
    const entry = entryFromStats(relativePath, stats);
    statements.upsertEntry.run(entry);
    if (stats.isDirectory()) {
      let children;
      try {
        children = await fsp.readdir(absolutePath);
      } catch (err) {
        return;
      }
      for (const child of children) {
        await walk(path.join(absolutePath, child));
      }
    }
  }

  await walk(START_PATH);
}

function refreshCaches() {
  entryCache = statements.allEntries.all().map((row) => ({
    path: row.path,
    name: row.name,
    parent_path: row.parent_path,
    type: row.type,
    size: row.size,
    mtime: row.mtime,
    extension: row.extension,
    depth: row.depth
  }));
  entryFuse = new Fuse(entryCache, {
    keys: ['path', 'name'],
    threshold: 0.3,
    ignoreLocation: true
  });

  const tagRows = statements.allTags.all();
  tagCache = tagRows.map((row) => ({
    entry_path: row.entry_path,
    key: row.key,
    value: row.value,
    pair: `${row.key}:${row.value}`
  }));
  tagFuse = new Fuse(tagCache, {
    keys: ['pair', 'key', 'value'],
    threshold: 0.2,
    ignoreLocation: true
  });
  tagValuesByKey = tagRows.reduce((map, row) => {
    if (!map.has(row.key)) {
      map.set(row.key, new Set());
    }
    map.get(row.key).add(row.value);
    return map;
  }, new Map());
}

function listTagsForPath(relativePath) {
  return statements.entryTags.all(relativePath);
}

function deleteEntryBranch(relativePath) {
  if (relativePath === '/' || !relativePath) {
    return;
  }
  const pattern = `${relativePath}/%`;
  const deleteBranch = db.transaction(() => {
    statements.deleteTagsForEntry.run(relativePath);
    statements.deleteTagsUnder.run(pattern);
    statements.removeEntry.run(relativePath);
    statements.removeEntriesUnder.run(pattern);
  });
  deleteBranch();
}

async function indexPath(absolutePath) {
  if (shouldIgnorePath(absolutePath)) {
    return false;
  }
  let stats;
  try {
    stats = await fsp.stat(absolutePath);
  } catch (err) {
    if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
      markPathUnsupported(absolutePath);
    }
    return false;
  }
  const relativePath = normalizeRelative(absolutePath);
  const entry = entryFromStats(relativePath, stats);
  statements.upsertEntry.run(entry);
  return true;
}

function removePath(relativePath) {
  const pattern = `${relativePath}/%`;
  const remove = db.transaction(() => {
    statements.deleteTagsForEntry.run(relativePath);
    statements.deleteTagsUnder.run(pattern);
    statements.removeEntry.run(relativePath);
    statements.removeEntriesUnder.run(pattern);
  });
  remove();
}

function buildTree() {
  const entries = statements.allEntries.all();
  const nodes = new Map();
  for (const entry of entries) {
    nodes.set(entry.path, {
      path: entry.path,
      name: entry.path === '/' ? path.basename(START_PATH) : entry.name,
      type: entry.type,
      size: entry.size,
      mtime: entry.mtime,
      extension: entry.extension,
      depth: entry.depth,
      tags: listTagsForPath(entry.path),
      children: []
    });
  }
  let root = nodes.get('/') || null;
  for (const entry of entries) {
    if (!entry.parent_path) {
      continue;
    }
    const parent = nodes.get(entry.parent_path);
    const node = nodes.get(entry.path);
    if (parent && node) {
      parent.children.push(node);
    }
  }
  for (const node of nodes.values()) {
    node.children.sort((a, b) => {
      if (a.type !== b.type) {
        return a.type === 'directory' ? -1 : 1;
      }
      return a.name.localeCompare(b.name, undefined, { sensitivity: 'base' });
    });
  }
  return root;
}

function parseTagFilters(tagParam) {
  if (!tagParam) {
    return [];
  }
  const raw = Array.isArray(tagParam) ? tagParam : String(tagParam).split(',');
  const clean = [];
  for (const piece of raw) {
    const trimmed = piece.trim();
    if (!trimmed) continue;
    const splitIndex = trimmed.indexOf(':');
    if (splitIndex === -1) continue;
    const key = trimmed.slice(0, splitIndex).trim();
    const value = trimmed.slice(splitIndex + 1).trim();
    if (key && value) {
      clean.push({ key, value });
    }
  }
  return clean;
}

function filterPathsByTags(filters) {
  if (!filters.length) {
    return null;
  }
  const sets = filters.map((filter) => {
    const rows = statements.pathsForTag.all(filter.key, filter.value);
    return new Set(rows.map((row) => row.entry_path));
  });
  if (!sets.length) {
    return null;
  }
  return sets.reduce((acc, current) => {
    const next = new Set();
    for (const value of acc) {
      if (current.has(value)) {
        next.add(value);
      }
    }
    return next;
  });
}

function gatherSearchResults({ query, tagFilters }) {
  const targetPathsSet = filterPathsByTags(tagFilters);
  let candidates;
  if (targetPathsSet) {
    candidates = entryCache.filter((entry) => targetPathsSet.has(entry.path));
  } else {
    candidates = entryCache;
  }
  if (query) {
    const fuseResults = entryFuse.search(query, { limit: 50 });
    const seen = new Set();
    const list = [];
    for (const result of fuseResults) {
      const entry = result.item;
      if (targetPathsSet && !targetPathsSet.has(entry.path)) {
        continue;
      }
      if (!seen.has(entry.path)) {
        seen.add(entry.path);
        list.push(entry);
      }
    }
    return list.map((entry) => ({
      ...entry,
      tags: listTagsForPath(entry.path)
    }));
  }
  return candidates.slice(0, 50).map((entry) => ({
    ...entry,
    tags: listTagsForPath(entry.path)
  }));
}

function buildSuggestions(queryRaw) {
  const query = (queryRaw || '').trim();
  if (!query) {
    const topDirs = entryCache.filter((entry) => entry.type === 'directory' && entry.path !== '/')
      .slice(0, 10)
      .map((entry) => ({ type: 'path', value: entry.path }));
    return topDirs;
  }
  const segments = query.split(/\s+/);
  const current = segments[segments.length - 1] || '';
  const colonIndex = current.indexOf(':');
  if (colonIndex > -1) {
    const keyPart = current.slice(0, colonIndex);
    const valuePart = current.slice(colonIndex + 1);
    const values = Array.from(tagValuesByKey.get(keyPart) || []);
    const filtered = values.filter((value) => value.toLowerCase().startsWith(valuePart.toLowerCase()))
      .slice(0, 10)
      .map((value) => ({ type: 'tag', value: `${keyPart}:${value}` }));
    return filtered;
  }
  const pathMatches = entryFuse.search(current, { limit: 5 })
    .map((result) => ({ type: 'path', value: result.item.path }));

  const tagMatches = tagFuse.search(current, { limit: 5 })
    .map((result) => ({ type: 'tag', value: result.item.pair }));

  const keyMatches = Array.from(tagValuesByKey.keys())
    .filter((key) => key.toLowerCase().startsWith(current.toLowerCase()))
    .slice(0, 5)
    .map((key) => ({ type: 'tagKey', value: `${key}:` }));

  const merged = [...pathMatches, ...tagMatches, ...keyMatches];
  const seen = new Set();
  const unique = [];
  for (const suggestion of merged) {
    const token = `${suggestion.type}:${suggestion.value}`;
    if (!seen.has(token)) {
      seen.add(token);
      unique.push(suggestion);
    }
  }
  return unique.slice(0, 10);
}

app.get('/api/tree', (_req, res) => {
  const tree = buildTree();
  res.json({ root: tree, rootName: path.basename(START_PATH), generatedAt: Date.now() });
});

app.get('/api/search', (req, res) => {
  const query = typeof req.query.q === 'string' ? req.query.q.trim() : '';
  const tagFilters = parseTagFilters(req.query.tags);
  const results = gatherSearchResults({ query, tagFilters });
  res.json({ results });
});

app.get('/api/suggestions', (req, res) => {
  const query = typeof req.query.q === 'string' ? req.query.q : '';
  const suggestions = buildSuggestions(query);
  res.json({ suggestions });
});

app.get('/api/entry', (req, res) => {
  const relativePath = typeof req.query.path === 'string' ? req.query.path : '';
  if (!relativePath) {
    res.status(400).json({ error: 'path query parameter required' });
    return;
  }
  const entry = statements.singleEntry.get(relativePath);
  if (!entry) {
    res.status(404).json({ error: 'Not found' });
    return;
  }
  res.json({
    entry: {
      ...entry,
      tags: listTagsForPath(relativePath)
    }
  });
});

app.post('/api/tags', (req, res) => {
  const { path: relativePath, key, value } = req.body || {};
  if (!relativePath || !key || !value) {
    res.status(400).json({ error: 'path, key, and value are required' });
    return;
  }
  const entry = statements.singleEntry.get(relativePath);
  if (!entry) {
    res.status(404).json({ error: 'Entry not found' });
    return;
  }
  statements.addTag.run(relativePath, String(key), String(value));
  refreshCaches();
  broadcast({ type: 'tag-added', path: relativePath, tag: { key: String(key), value: String(value) } });
  res.json({ success: true });
});

app.delete('/api/tags', (req, res) => {
  const { path: relativePath, key, value } = req.body || {};
  if (!relativePath || !key || !value) {
    res.status(400).json({ error: 'path, key, and value are required' });
    return;
  }
  statements.deleteTag.run(relativePath, String(key), String(value));
  refreshCaches();
  broadcast({ type: 'tag-removed', path: relativePath, tag: { key: String(key), value: String(value) } });
  res.json({ success: true });
});

const watcher = new chokidar.FSWatcher({
  persistent: true,
  ignoreInitial: true,
  ignorePermissionErrors: true,
  awaitWriteFinish: {
    stabilityThreshold: 200,
    pollInterval: 100
  },
  ignored: (watchPath) => {
    const absolute = path.isAbsolute(watchPath) ? watchPath : path.join(START_PATH, watchPath);
    return shouldIgnorePath(absolute);
  }
});

watcher.on('error', (err) => {
  if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
    const targetPath = err.path;
    if (targetPath) {
      markPathUnsupported(targetPath);
      watcher.unwatch(targetPath);
    }
    console.warn('Skipping unsupported filesystem entry', targetPath || err.message);
    return;
  }
  console.error('File watcher error', err);
});

watcher.on('add', async (filePath) => {
  if (!await indexPath(filePath)) {
    return;
  }
  refreshCaches();
  broadcast({ type: 'entry-added', path: normalizeRelative(filePath) });
});

watcher.on('change', async (filePath) => {
  if (!await indexPath(filePath)) {
    return;
  }
  refreshCaches();
  broadcast({ type: 'entry-updated', path: normalizeRelative(filePath) });
});

watcher.on('unlink', (filePath) => {
  if (shouldIgnorePath(filePath)) {
    return;
  }
  const relativePath = normalizeRelative(filePath);
  removePath(relativePath);
  refreshCaches();
  broadcast({ type: 'entry-removed', path: relativePath });
});

watcher.on('addDir', async (dirPath) => {
  if (!await indexPath(dirPath)) {
    return;
  }
  refreshCaches();
  broadcast({ type: 'entry-added', path: normalizeRelative(dirPath) });
});

watcher.on('unlinkDir', (dirPath) => {
  if (shouldIgnorePath(dirPath)) {
    return;
  }
  const relativePath = normalizeRelative(dirPath);
  deleteEntryBranch(relativePath);
  refreshCaches();
  broadcast({ type: 'entry-removed', path: relativePath });
});

watcher.add(START_PATH);

async function bootstrap() {
  await scanInitialTree();
  refreshCaches();
  server.listen(PORT, () => {
    console.log(`better-filexplorer running at http://localhost:${PORT}`);
    console.log(`Monitoring ${START_PATH}`);
  });
}

bootstrap().catch((err) => {
  console.error('Failed to bootstrap service', err);
  process.exit(1);
});

let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  watcher.close().catch(() => {});
  server.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

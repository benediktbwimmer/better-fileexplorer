const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const http = require('http');
const { execFile } = require('child_process');
const { promisify } = require('util');
const express = require('express');
const chokidar = require('chokidar');
const Database = require('better-sqlite3');
const Fuse = require('fuse.js');
const { WebSocketServer } = require('ws');

const execFileAsync = promisify(execFile);
const GIT_COMMAND_TIMEOUT = 5_000;
const GIT_MAX_BUFFER = 2 * 1024 * 1024;

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
  CREATE TABLE git_metadata (
    entry_path TEXT PRIMARY KEY,
    detected_at INTEGER NOT NULL,
    current_branch TEXT,
    commit_count INTEGER,
    branch_count INTEGER,
    remote_count INTEGER,
    remotes TEXT,
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
  deleteTagsUnder: db.prepare('DELETE FROM tags WHERE entry_path LIKE ?'),
  upsertGitMetadata: db.prepare(`
    INSERT INTO git_metadata (entry_path, detected_at, current_branch, commit_count, branch_count, remote_count, remotes)
    VALUES (@entry_path, @detected_at, @current_branch, @commit_count, @branch_count, @remote_count, @remotes)
    ON CONFLICT(entry_path) DO UPDATE SET
      detected_at = excluded.detected_at,
      current_branch = excluded.current_branch,
      commit_count = excluded.commit_count,
      branch_count = excluded.branch_count,
      remote_count = excluded.remote_count,
      remotes = excluded.remotes
  `),
  deleteGitMetadata: db.prepare('DELETE FROM git_metadata WHERE entry_path = ?'),
  allGitMetadata: db.prepare('SELECT * FROM git_metadata'),
  singleGitMetadata: db.prepare('SELECT * FROM git_metadata WHERE entry_path = ?')
};

let entryCache = [];
let entryFuse = new Fuse([], { keys: ['path', 'name'], threshold: 0.3, ignoreLocation: true });
let tagCache = [];
let tagFuse = new Fuse([], { keys: ['key', 'value', 'pair'], threshold: 0.3, ignoreLocation: true, includeScore: true });
let tagValuesByKey = new Map();
let gitCacheByPath = new Map();
const pendingGitMetadataUpdates = new Map();
let gitBinaryUnavailable = false;
let gitUnavailableLogged = false;
const UNSUPPORTED_FS_CODES = new Set(['ENOTSUP', 'EOPNOTSUPP', 'EPERM', 'EACCES', 'ENAMETOOLONG']);
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

function isNotGitRepositoryError(err) {
  if (!err) {
    return false;
  }
  const targets = [(err.stderr || ''), (err.stdout || ''), (err.message || '')].join(' ').toLowerCase();
  return targets.includes('not a git repository') || targets.includes('invalid gitfile format');
}

async function runGitCommand(args, cwd) {
  if (gitBinaryUnavailable) {
    const error = new Error('git executable unavailable');
    error.code = 'GIT_UNAVAILABLE';
    throw error;
  }
  try {
    const { stdout } = await execFileAsync('git', args, {
      cwd,
      timeout: GIT_COMMAND_TIMEOUT,
      maxBuffer: GIT_MAX_BUFFER,
      windowsHide: true,
      encoding: 'utf8'
    });
    gitBinaryUnavailable = false;
    return stdout;
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      gitBinaryUnavailable = true;
      if (!gitUnavailableLogged) {
        console.warn('git is not available on PATH; skipping repository metadata.');
        gitUnavailableLogged = true;
      }
      const unavailable = new Error('git executable unavailable');
      unavailable.code = 'GIT_UNAVAILABLE';
      throw unavailable;
    }
    // git exists but the command failed; ensure we only log unexpected issues once per process.
    if (!gitUnavailableLogged && gitBinaryUnavailable) {
      console.warn('git is not available on PATH; skipping repository metadata.');
      gitUnavailableLogged = true;
    }
    throw err;
  }
}

function parseGitRemoteOutput(output) {
  const remotes = new Map();
  if (!output) {
    return [];
  }
  const lines = output.split('\n');
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    const parts = line.split(/\s+/);
    if (parts.length < 2) {
      continue;
    }
    const [name, url, type] = parts;
    if (!remotes.has(name)) {
      remotes.set(name, { name, fetchUrl: null, pushUrl: null });
    }
    const remote = remotes.get(name);
    if (type === '(fetch)') {
      remote.fetchUrl = url;
    } else if (type === '(push)') {
      remote.pushUrl = url;
    } else {
      if (!remote.fetchUrl) {
        remote.fetchUrl = url;
      }
      if (!remote.pushUrl) {
        remote.pushUrl = url;
      }
    }
  }
  return Array.from(remotes.values()).map((remote) => ({
    name: remote.name,
    fetchUrl: remote.fetchUrl || null,
    pushUrl: remote.pushUrl || remote.fetchUrl || null
  }));
}

function repoPathFromGitInternal(relativePath) {
  if (!relativePath) {
    return null;
  }
  if (relativePath === '.git' || relativePath.startsWith('.git/')) {
    return '/';
  }
  const marker = '/.git';
  const idx = relativePath.indexOf(marker);
  if (idx === -1) {
    return null;
  }
  const suffix = relativePath.slice(idx + marker.length);
  if (suffix && suffix[0] !== '/') {
    return null;
  }
  const candidate = relativePath.slice(0, idx);
  return candidate || '/';
}

async function gatherGitMetadata(absolutePath, relativePath) {
  if (!relativePath) {
    return null;
  }
  if (relativePath !== '/' && path.basename(relativePath) === '.git') {
    return null;
  }
  let dirStats;
  try {
    dirStats = await fsp.stat(absolutePath);
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      statements.deleteGitMetadata.run(relativePath);
      return null;
    }
    throw err;
  }
  if (!dirStats.isDirectory()) {
    statements.deleteGitMetadata.run(relativePath);
    return null;
  }

  try {
    await fsp.stat(path.join(absolutePath, '.git'));
  } catch (err) {
    if (err && (err.code === 'ENOENT' || err.code === 'ENOTDIR')) {
      statements.deleteGitMetadata.run(relativePath);
      return null;
    }
    if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
      return null;
    }
    throw err;
  }

  let repoRootAbsolute;
  try {
    repoRootAbsolute = (await runGitCommand(['rev-parse', '--show-toplevel'], absolutePath)).trim();
  } catch (err) {
    if (err && err.code === 'GIT_UNAVAILABLE') {
      throw err;
    }
    if (isNotGitRepositoryError(err)) {
      statements.deleteGitMetadata.run(relativePath);
      return null;
    }
    console.warn(`Failed to resolve git root for ${absolutePath}: ${err.message}`);
    return null;
  }

  const repoRelative = normalizeRelative(repoRootAbsolute);
  if (repoRelative.startsWith('..')) {
    statements.deleteGitMetadata.run(relativePath);
    return null;
  }
  if (repoRelative !== relativePath) {
    // Only store metadata for the actual repository root within the monitored tree.
    statements.deleteGitMetadata.run(relativePath);
    return null;
  }

  let currentBranch = null;
  try {
    currentBranch = (await runGitCommand(['symbolic-ref', '--quiet', '--short', 'HEAD'], absolutePath)).trim();
  } catch (err) {
    if (err && err.code === 'GIT_UNAVAILABLE') {
      throw err;
    }
    if (!isNotGitRepositoryError(err)) {
      try {
        const detached = (await runGitCommand(['rev-parse', '--short', 'HEAD'], absolutePath)).trim();
        currentBranch = detached || null;
      } catch (fallbackErr) {
        if (fallbackErr && fallbackErr.code === 'GIT_UNAVAILABLE') {
          throw fallbackErr;
        }
        currentBranch = null;
      }
    }
  }

  let commitCount = null;
  try {
    const output = await runGitCommand(['rev-list', '--all', '--count'], absolutePath);
    const parsed = parseInt(output.trim(), 10);
    commitCount = Number.isNaN(parsed) ? null : parsed;
  } catch (err) {
    if (err && err.code === 'GIT_UNAVAILABLE') {
      throw err;
    }
    commitCount = null;
  }

  let branchCount = null;
  try {
    const output = await runGitCommand(['branch', '--format=%(refname:short)'], absolutePath);
    branchCount = output
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .length;
  } catch (err) {
    if (err && err.code === 'GIT_UNAVAILABLE') {
      throw err;
    }
    branchCount = null;
  }

  let remotes = [];
  try {
    const remoteOutput = await runGitCommand(['remote', '-v'], absolutePath);
    remotes = parseGitRemoteOutput(remoteOutput);
  } catch (err) {
    if (err && err.code === 'GIT_UNAVAILABLE') {
      throw err;
    }
    remotes = [];
  }

  const detectedAt = Date.now();
  statements.upsertGitMetadata.run({
    entry_path: relativePath,
    detected_at: detectedAt,
    current_branch: currentBranch || null,
    commit_count: commitCount,
    branch_count: branchCount,
    remote_count: remotes.length,
    remotes: JSON.stringify(remotes)
  });

  return {
    isRepo: true,
    detectedAt,
    currentBranch: currentBranch || null,
    commitCount,
    branchCount,
    remoteCount: remotes.length,
    remotes
  };
}

async function updateGitMetadataForDirectory(absolutePath, relativePath) {
  const relPath = relativePath || normalizeRelative(absolutePath);
  if (!relPath) {
    return null;
  }
  if (relPath !== '/' && path.basename(relPath) === '.git') {
    return null;
  }
  const key = relPath;
  if (pendingGitMetadataUpdates.has(key)) {
    return pendingGitMetadataUpdates.get(key);
  }
  const absolute = absolutePath || absoluteFromRelative(relPath);
  const promise = (async () => {
    try {
      return await gatherGitMetadata(absolute, key);
    } catch (err) {
      if (err && err.code === 'GIT_UNAVAILABLE') {
        return null;
      }
      if (err && err.message) {
        console.warn(`Failed to update git metadata for ${key}: ${err.message}`);
      }
      return null;
    } finally {
      pendingGitMetadataUpdates.delete(key);
    }
  })();
  pendingGitMetadataUpdates.set(key, promise);
  return promise;
}

async function refreshGitMetadataForPath(relativePath) {
  const repoPath = repoPathFromGitInternal(relativePath);
  if (!repoPath) {
    return;
  }
  const absolute = absoluteFromRelative(repoPath);
  await updateGitMetadataForDirectory(absolute, repoPath);
}

function parseRemotesJson(raw) {
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((entry) => ({
        name: entry && entry.name ? String(entry.name) : '',
        fetchUrl: entry && entry.fetchUrl ? String(entry.fetchUrl) : null,
        pushUrl: entry && entry.pushUrl ? String(entry.pushUrl) : null
      }))
      .filter((entry) => Boolean(entry.name));
  } catch (_err) {
    return [];
  }
}

function buildGitInfoForEntry(entry) {
  if (!entry || entry.type !== 'directory') {
    return null;
  }
  const cached = gitCacheByPath.get(entry.path);
  if (!cached) {
    return { isRepo: false };
  }
  const remotes = Array.isArray(cached.remotes)
    ? cached.remotes.map((remote) => ({
      name: remote.name,
      fetchUrl: remote.fetchUrl || null,
      pushUrl: remote.pushUrl || null
    }))
    : [];
  const remoteCount = typeof cached.remoteCount === 'number' ? cached.remoteCount : remotes.length;
  return {
    isRepo: true,
    detectedAt: cached.detectedAt || null,
    currentBranch: cached.currentBranch || null,
    commitCount: typeof cached.commitCount === 'number' ? cached.commitCount : null,
    branchCount: typeof cached.branchCount === 'number' ? cached.branchCount : null,
    remoteCount,
    isLocalOnly: remoteCount === 0,
    remotes
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
      await updateGitMetadataForDirectory(absolutePath, relativePath);
    } else {
      await refreshGitMetadataForPath(relativePath);
    }
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
  const gitRows = statements.allGitMetadata.all();
  gitCacheByPath = new Map();
  for (const row of gitRows) {
    gitCacheByPath.set(row.entry_path, {
      detectedAt: row.detected_at || null,
      currentBranch: row.current_branch || null,
      commitCount: typeof row.commit_count === 'number' ? row.commit_count : null,
      branchCount: typeof row.branch_count === 'number' ? row.branch_count : null,
      remoteCount: typeof row.remote_count === 'number' ? row.remote_count : 0,
      remotes: parseRemotesJson(row.remotes)
    });
  }

  entryCache = statements.allEntries.all().map((row) => {
    const entry = {
      path: row.path,
      name: row.name,
      parent_path: row.parent_path,
      type: row.type,
      size: row.size,
      mtime: row.mtime,
      extension: row.extension,
      depth: row.depth
    };
    entry.git = buildGitInfoForEntry(entry);
    return entry;
  });
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
    ignoreLocation: true,
    includeScore: true
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

async function deleteEntryBranch(relativePath) {
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
  await refreshGitMetadataForPath(relativePath);
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
  if (stats.isDirectory()) {
    await updateGitMetadataForDirectory(absolutePath, relativePath);
  } else {
    statements.deleteGitMetadata.run(relativePath);
    await refreshGitMetadataForPath(relativePath);
  }
  return true;
}

async function removePath(relativePath) {
  const pattern = `${relativePath}/%`;
  const remove = db.transaction(() => {
    statements.deleteTagsForEntry.run(relativePath);
    statements.deleteTagsUnder.run(pattern);
    statements.removeEntry.run(relativePath);
    statements.removeEntriesUnder.run(pattern);
  });
  remove();
  await refreshGitMetadataForPath(relativePath);
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
      git: buildGitInfoForEntry(entry),
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
      git: buildGitInfoForEntry(entry),
      tags: listTagsForPath(entry.path)
    }));
  }
  return candidates.slice(0, 50).map((entry) => ({
    ...entry,
    git: buildGitInfoForEntry(entry),
    tags: listTagsForPath(entry.path)
  }));
}

function buildFileSnippet(lineText, query) {
  const text = typeof lineText === 'string' ? lineText : String(lineText || '');
  const trimmedQuery = (query || '').trim();
  const MAX_LENGTH = 240;
  if (!text) {
    return '';
  }
  const normalizedText = text.replace(/\t/g, '    ');
  if (!trimmedQuery) {
    if (normalizedText.length <= MAX_LENGTH) {
      return normalizedText;
    }
    return `${normalizedText.slice(0, MAX_LENGTH - 1)}…`;
  }
  const lowerText = normalizedText.toLowerCase();
  const lowerQuery = trimmedQuery.toLowerCase();
  const index = lowerText.indexOf(lowerQuery);
  if (index === -1) {
    if (normalizedText.length <= MAX_LENGTH) {
      return normalizedText;
    }
    return `${normalizedText.slice(0, MAX_LENGTH - 1)}…`;
  }
  const preContext = 60;
  const postContext = 120;
  const start = Math.max(0, index - preContext);
  const end = Math.min(normalizedText.length, index + lowerQuery.length + postContext);
  let snippet = normalizedText.slice(start, end);
  if (start > 0) {
    snippet = `…${snippet}`;
  }
  if (end < normalizedText.length) {
    snippet = `${snippet}…`;
  }
  return snippet;
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
      tags: listTagsForPath(relativePath),
      git: buildGitInfoForEntry(entry)
    }
  });
});

app.get('/api/file/stream', async (req, res) => {
  const relativePath = typeof req.query.path === 'string' ? req.query.path : '';
  if (!relativePath) {
    res.status(400).json({ error: 'path query parameter required' });
    return;
  }
  const entry = statements.singleEntry.get(relativePath);
  if (!entry) {
    res.status(404).json({ error: 'File not found' });
    return;
  }
  if (entry.type !== 'file') {
    res.status(400).json({ error: 'Requested path is not a file' });
    return;
  }

  const absolute = absoluteFromRelative(relativePath);
  let stats;
  try {
    stats = await fsp.stat(absolute);
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      res.status(404).json({ error: 'File not found' });
      return;
    }
    if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
      res.status(403).json({ error: 'File cannot be read on this filesystem' });
      return;
    }
    console.error(`Failed to stat file for streaming ${absolute}: ${err.message}`);
    res.status(500).json({ error: 'Failed to read file' });
    return;
  }

  if (!stats.isFile()) {
    res.status(400).json({ error: 'Requested path is not a regular file' });
    return;
  }

  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  if (typeof stats.size === 'number') {
    res.setHeader('Content-Length', String(stats.size));
  }
  res.setHeader('X-File-Path', relativePath);
  res.setHeader('X-File-Mtime', String(stats.mtimeMs ? Math.round(stats.mtimeMs) : Date.now()));

  const stream = fs.createReadStream(absolute, { encoding: 'utf8', highWaterMark: 64 * 1024 });
  stream.on('error', (err) => {
    if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
      if (!res.headersSent) {
        res.status(403).json({ error: 'File cannot be read on this filesystem' });
      } else {
        res.destroy(err);
      }
      return;
    }
    console.error(`Stream error while sending ${absolute}: ${err.message}`);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Failed to stream file' });
    } else {
      res.destroy(err);
    }
  });

  stream.pipe(res);
});

app.get('/api/file/search', async (req, res) => {
  const relativePath = typeof req.query.path === 'string' ? req.query.path : '';
  const query = typeof req.query.q === 'string' ? req.query.q.trim() : '';
  if (!relativePath) {
    res.status(400).json({ error: 'path query parameter required' });
    return;
  }
  if (!query) {
    res.status(400).json({ error: 'q query parameter required' });
    return;
  }

  const entry = statements.singleEntry.get(relativePath);
  if (!entry) {
    res.status(404).json({ error: 'File not found' });
    return;
  }
  if (entry.type !== 'file') {
    res.status(400).json({ error: 'Requested path is not a file' });
    return;
  }

  const absolute = absoluteFromRelative(relativePath);
  let content;
  try {
    content = await fsp.readFile(absolute, 'utf8');
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      res.status(404).json({ error: 'File not found' });
      return;
    }
    if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
      res.status(403).json({ error: 'File cannot be read on this filesystem' });
      return;
    }
    console.error(`Failed to read file for search ${absolute}: ${err.message}`);
    res.status(500).json({ error: 'Failed to read file' });
    return;
  }

  const lines = content.split(/\r\n|\n|\r/);
  const items = lines.map((text, index) => ({ line: index + 1, text }));
  const fuse = new Fuse(items, {
    keys: ['text'],
    threshold: 0.4,
    ignoreLocation: true,
    includeScore: true
  });
  const fuseResults = fuse.search(query, { limit: 50 });
  const matches = fuseResults.map((result) => ({
    line: result.item.line,
    score: typeof result.score === 'number' ? result.score : null,
    snippet: buildFileSnippet(result.item.text, query)
  }));

  res.json({ matches });
});

app.get('/api/tags', (req, res) => {
  const relativePath = typeof req.query.path === 'string' ? req.query.path : '';
  if (relativePath) {
    const entry = statements.singleEntry.get(relativePath);
    if (!entry) {
      res.status(404).json({ error: 'Entry not found' });
      return;
    }
    const tags = listTagsForPath(relativePath).map((tag) => ({
      path: relativePath,
      key: tag.key,
      value: tag.value
    }));
    res.json({ tags });
    return;
  }

  const tags = tagCache.map((tag) => ({
    path: tag.entry_path,
    key: tag.key,
    value: tag.value
  }));
  res.json({ tags });
});

app.get('/api/tags/search', (req, res) => {
  const query = typeof req.query.q === 'string' ? req.query.q.trim() : '';
  if (!query) {
    res.status(400).json({ error: 'q query parameter required' });
    return;
  }

  const rawLimit = typeof req.query.limit === 'string' ? Number.parseInt(req.query.limit, 10) : NaN;
  const limit = Number.isFinite(rawLimit) ? Math.min(Math.max(rawLimit, 1), 100) : 20;

  const fuseResults = tagFuse.search(query, { limit });
  const results = fuseResults.map((result) => ({
    path: result.item.entry_path,
    key: result.item.key,
    value: result.item.value,
    pair: result.item.pair,
    score: typeof result.score === 'number' ? result.score : null
  }));

  res.json({ results });
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

function buildWatcherOptions(extra = {}) {
  return {
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
    },
    ...extra
  };
}

let watcher = null;
let watcherMode = 'native';
let watcherSwitchPromise = null;
let watcherErrorDiagnosticsLogged = false;

function registerWatcherEvents(instance) {
  instance.on('error', (err) => {
    handleWatcherError(err, instance);
  });
  instance.on('add', async (filePath) => {
    if (instance !== watcher) {
      return;
    }
    if (!await indexPath(filePath)) {
      return;
    }
    refreshCaches();
    broadcast({ type: 'entry-added', path: normalizeRelative(filePath) });
  });
  instance.on('change', async (filePath) => {
    if (instance !== watcher) {
      return;
    }
    if (!await indexPath(filePath)) {
      return;
    }
    refreshCaches();
    broadcast({ type: 'entry-updated', path: normalizeRelative(filePath) });
  });
  instance.on('unlink', async (filePath) => {
    if (instance !== watcher) {
      return;
    }
    if (shouldIgnorePath(filePath)) {
      return;
    }
    const relativePath = normalizeRelative(filePath);
    await removePath(relativePath);
    refreshCaches();
    broadcast({ type: 'entry-removed', path: relativePath });
  });
  instance.on('addDir', async (dirPath) => {
    if (instance !== watcher) {
      return;
    }
    if (!await indexPath(dirPath)) {
      return;
    }
    refreshCaches();
    broadcast({ type: 'entry-added', path: normalizeRelative(dirPath) });
  });
  instance.on('unlinkDir', async (dirPath) => {
    if (instance !== watcher) {
      return;
    }
    if (shouldIgnorePath(dirPath)) {
      return;
    }
    const relativePath = normalizeRelative(dirPath);
    await deleteEntryBranch(relativePath);
    refreshCaches();
    broadcast({ type: 'entry-removed', path: relativePath });
  });
}

function isWatcherLimitError(err) {
  if (!err) {
    return false;
  }
  const code = typeof err.code === 'string' ? err.code.toUpperCase() : '';
  if (code === 'EMFILE' || code === 'ENOSPC') {
    return true;
  }
  const message = typeof err.message === 'string' ? err.message.toUpperCase() : '';
  return message.includes('EMFILE') || message.includes('ENOSPC');
}

function handleWatcherError(err, instance) {
  if (instance !== watcher) {
    return;
  }
  if (!watcherErrorDiagnosticsLogged) {
    console.warn('File watcher encountered an error', {
      code: err && err.code,
      message: err && err.message
    });
    watcherErrorDiagnosticsLogged = true;
  }
  if (err && UNSUPPORTED_FS_CODES.has(err.code)) {
    const targetPath = err.path;
    if (targetPath) {
      markPathUnsupported(targetPath);
      instance.unwatch(targetPath);
    }
    console.warn('Skipping unsupported filesystem entry', targetPath || err.message);
    return;
  }
  if (isWatcherLimitError(err)) {
    if (watcherMode !== 'polling') {
      console.warn('File watcher limit reached; switching to polling mode.');
      switchWatcherMode('polling').catch((switchErr) => {
        console.error('Failed to switch watcher mode', switchErr);
      });
    } else {
      console.warn('File watcher limit reached even in polling mode. Consider increasing the file descriptor limit.');
    }
    return;
  }
  console.error('File watcher error', err);
}

async function switchWatcherMode(mode) {
  if (mode === watcherMode && watcher) {
    return;
  }
  if (watcherSwitchPromise) {
    return watcherSwitchPromise;
  }
  watcherSwitchPromise = (async () => {
    const oldWatcher = watcher;
    watcher = null;
    if (oldWatcher) {
      oldWatcher.removeAllListeners();
      try {
        await oldWatcher.close();
      } catch (closeErr) {
        console.warn('Failed to close previous watcher', closeErr);
      }
    }
    const extraOptions = mode === 'polling'
      ? { usePolling: true, interval: 5_000, binaryInterval: 7_500 }
      : {};
    const newWatcher = new chokidar.FSWatcher(buildWatcherOptions(extraOptions));
    registerWatcherEvents(newWatcher);
    watcher = newWatcher;
    watcherMode = mode;
    try {
      await newWatcher.add(START_PATH);
    } catch (addErr) {
      console.error('Failed to initialize file watcher', addErr);
      throw addErr;
    }
    if (mode === 'polling') {
      console.warn('Watcher running in polling mode (5s interval). Consider raising the file descriptor limit for better performance.');
    }
  })().finally(() => {
    watcherSwitchPromise = null;
  });
  return watcherSwitchPromise;
}

async function bootstrap() {
  await scanInitialTree();
  refreshCaches();
  await switchWatcherMode('native');
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
  const currentWatcher = watcher;
  watcher = null;
  if (currentWatcher) {
    currentWatcher.close().catch(() => {});
  }
  server.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

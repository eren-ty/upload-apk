const http = require("http");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const REMOTE_PATH = process.env.REMOTE_PATH || "minio:app-pkg/downloads/apks";
const RCLONE_CONFIG = process.env.RCLONE_CONFIG || "/root/.config/rclone/rsync_oss.conf";
const ACCESS_TOKEN = process.env.ACCESS_TOKEN || "";
const MAX_ACTIVE_JOBS = Number(process.env.MAX_ACTIVE_JOBS || 2);
const SYNC_INTERVAL_MINUTES = Number(process.env.SYNC_INTERVAL_MINUTES || 360);
const AUTO_SYNC_ON_CHANGE = process.env.AUTO_SYNC_ON_CHANGE !== "false";
const SYNC_ON_START = process.env.SYNC_ON_START === "true";
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "data", "urls.json");
const ALLOWED_EXTENSIONS = (process.env.ALLOWED_EXTENSIONS || ".apk")
  .split(",")
  .map((item) => item.trim().toLowerCase())
  .filter(Boolean);

const publicDir = path.join(__dirname, "public");
const jobs = new Map();
let activeJobs = 0;
const queue = [];
let urlRecords = [];

loadUrlRecords();

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload)
  });
  res.end(payload);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        req.destroy();
        reject(new Error("request body too large"));
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function hasAccess(req) {
  if (!ACCESS_TOKEN) return true;
  const header = req.headers.authorization || "";
  return header === `Bearer ${ACCESS_TOKEN}`;
}

function requireAccess(req, res) {
  if (hasAccess(req)) return true;
  sendJson(res, 401, { error: "unauthorized" });
  return false;
}

function parseRequestUrl(req) {
  return new URL(req.url, `http://${req.headers.host || "localhost"}`);
}

function loadUrlRecords() {
  try {
    const content = fs.readFileSync(DATA_FILE, "utf8");
    const data = JSON.parse(content);
    urlRecords = Array.isArray(data.urls) ? data.urls : [];
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.error(`Failed to load ${DATA_FILE}:`, error.message);
    }
    urlRecords = [];
  }
}

async function saveUrlRecords() {
  await fs.promises.mkdir(path.dirname(DATA_FILE), { recursive: true });
  const payload = JSON.stringify({ urls: urlRecords }, null, 2);
  await fs.promises.writeFile(DATA_FILE, `${payload}\n`);
}

function validateUrl(rawUrl) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error("请输入合法的 URL");
  }

  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("只支持 http 或 https 地址");
  }

  const filename = path.basename(decodeURIComponent(parsed.pathname || ""));
  if (!filename || filename === "." || filename === "..") {
    throw new Error("URL 中没有可用的文件名");
  }

  const extension = path.extname(filename).toLowerCase();
  if (ALLOWED_EXTENSIONS.length > 0 && !ALLOWED_EXTENSIONS.includes(extension)) {
    throw new Error(`只允许上传这些后缀: ${ALLOWED_EXTENSIONS.join(", ")}`);
  }

  return { parsed, filename: sanitizeFilename(filename) };
}

function sanitizeFilename(filename) {
  const cleaned = filename.replace(/[^\w.\-()+@]/g, "_");
  return cleaned.slice(0, 180) || `download-${Date.now()}`;
}

function normalizeName(name, filename) {
  const normalized = String(name || "").trim();
  return normalized || filename;
}

function appendLog(job, message) {
  const text = String(message || "").trim();
  if (!text) return;
  const line = `[${new Date().toISOString()}] ${text}`;
  job.logs.push(line);
  if (job.logs.length > 200) job.logs.shift();
}

function runCommand(command, args, job) {
  return new Promise((resolve, reject) => {
    appendLog(job, `$ ${command} ${args.map((arg) => JSON.stringify(arg)).join(" ")}`);
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });

    child.stdout.on("data", (chunk) => appendLog(job, chunk.toString()));
    child.stderr.on("data", (chunk) => appendLog(job, chunk.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with code ${code}`));
    });
  });
}

function createJob({ url, filename, sourceId = null, sourceName = "" }) {
  const id = crypto.randomUUID();
  const job = {
    id,
    sourceId,
    sourceName,
    url,
    filename,
    status: "queued",
    logs: [],
    createdAt: new Date().toISOString(),
    finishedAt: null,
    error: null
  };

  jobs.set(id, job);
  appendLog(job, "已加入队列");
  enqueueJob(job);
  return job;
}

function enqueueJob(job) {
  queue.push(job);
  drainQueue();
}

function drainQueue() {
  while (activeJobs < MAX_ACTIVE_JOBS && queue.length > 0) {
    const job = queue.shift();
    activeJobs += 1;
    processJob(job).finally(() => {
      activeJobs -= 1;
      drainQueue();
    });
  }
}

async function processJob(job) {
  const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "url-minio-"));
  const tmpFile = path.join(tmpDir, job.filename);

  try {
    updateSourceFromJob(job, { lastStatus: "running", lastJobId: job.id, lastError: null });

    job.status = "downloading";
    appendLog(job, `开始下载: ${job.url}`);
    await runCommand("curl", ["-fSL", "--retry", "2", "--connect-timeout", "15", "-o", tmpFile, job.url], job);

    job.status = "uploading";
    appendLog(job, `开始上传到 MinIO: ${REMOTE_PATH}`);
    await runCommand("rclone", ["copy", tmpFile, REMOTE_PATH, "--config", RCLONE_CONFIG], job);

    job.status = "done";
    job.finishedAt = new Date().toISOString();
    appendLog(job, "完成");
    updateSourceFromJob(job, {
      lastStatus: "done",
      lastJobId: job.id,
      lastSyncedAt: job.finishedAt,
      lastError: null
    });
  } catch (error) {
    job.status = "failed";
    job.error = error.message;
    job.finishedAt = new Date().toISOString();
    appendLog(job, `失败: ${error.message}`);
    updateSourceFromJob(job, {
      lastStatus: "failed",
      lastJobId: job.id,
      lastError: error.message
    });
  } finally {
    await fs.promises.rm(tmpDir, { recursive: true, force: true });
  }
}

function updateSourceFromJob(job, patch) {
  if (!job.sourceId) return;
  const record = urlRecords.find((item) => item.id === job.sourceId);
  if (!record) return;
  Object.assign(record, patch);
  saveUrlRecords().catch((error) => console.error("Failed to update source record:", error.message));
}

function createRecordFromBody(body) {
  const url = String(body.url || "").trim();
  const { filename } = validateUrl(url);
  const exists = urlRecords.some((item) => item.url === url);
  if (exists) throw new Error("这个 URL 已存在");

  return {
    id: crypto.randomUUID(),
    name: normalizeName(body.name, filename),
    url,
    filename,
    enabled: body.enabled !== false,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastSyncedAt: null,
    lastStatus: "never",
    lastJobId: null,
    lastError: null
  };
}

function createSyncJobForRecord(record, reason) {
  if (!record.enabled) return null;
  const job = createJob({
    url: record.url,
    filename: record.filename,
    sourceId: record.id,
    sourceName: record.name
  });
  appendLog(job, `触发原因: ${reason}`);
  record.lastStatus = "queued";
  record.lastJobId = job.id;
  record.lastError = null;
  saveUrlRecords().catch((error) => console.error("Failed to save queued status:", error.message));
  return job;
}

function syncAllEnabled(reason) {
  return urlRecords
    .filter((record) => record.enabled)
    .map((record) => createSyncJobForRecord(record, reason))
    .filter(Boolean);
}

function serveStatic(req, res) {
  const requestUrl = parseRequestUrl(req);
  const requested = requestUrl.pathname === "/" ? "/index.html" : requestUrl.pathname;
  const safePath = path.normalize(decodeURIComponent(requested)).replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(publicDir, safePath);

  if (!filePath.startsWith(publicDir)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (error, content) => {
    if (error) {
      res.writeHead(404);
      res.end("Not found");
      return;
    }

    const ext = path.extname(filePath);
    const contentTypes = {
      ".css": "text/css; charset=utf-8",
      ".js": "application/javascript; charset=utf-8",
      ".html": "text/html; charset=utf-8"
    };
    res.writeHead(200, { "content-type": contentTypes[ext] || "application/octet-stream" });
    res.end(content);
  });
}

async function handleCreateJob(req, res) {
  if (!requireAccess(req, res)) return;

  try {
    const body = JSON.parse(await readBody(req));
    const url = String(body.url || "").trim();
    const { filename } = validateUrl(url);
    const job = createJob({ url, filename });
    sendJson(res, 202, { id: job.id, job });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

function handleGetJob(req, res, id) {
  if (!requireAccess(req, res)) return;

  const job = jobs.get(id);
  if (!job) {
    sendJson(res, 404, { error: "job not found" });
    return;
  }

  sendJson(res, 200, { job });
}

function handleListJobs(req, res) {
  if (!requireAccess(req, res)) return;
  const items = Array.from(jobs.values()).slice(-50).reverse();
  sendJson(res, 200, { jobs: items });
}

async function handleListUrls(req, res) {
  if (!requireAccess(req, res)) return;
  sendJson(res, 200, { urls: urlRecords });
}

async function handleCreateUrl(req, res) {
  if (!requireAccess(req, res)) return;

  try {
    const body = JSON.parse(await readBody(req));
    const record = createRecordFromBody(body);
    urlRecords.unshift(record);
    await saveUrlRecords();

    let job = null;
    if (AUTO_SYNC_ON_CHANGE && record.enabled) {
      job = createSyncJobForRecord(record, "URL 新增后自动同步");
    }

    sendJson(res, 201, { url: record, job });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

async function handleUpdateUrl(req, res, id) {
  if (!requireAccess(req, res)) return;

  const record = urlRecords.find((item) => item.id === id);
  if (!record) {
    sendJson(res, 404, { error: "url not found" });
    return;
  }

  try {
    const body = JSON.parse(await readBody(req));
    let urlChanged = false;
    let enabledChanged = false;

    if (body.url !== undefined) {
      const nextUrl = String(body.url || "").trim();
      const { filename } = validateUrl(nextUrl);
      const exists = urlRecords.some((item) => item.id !== id && item.url === nextUrl);
      if (exists) throw new Error("这个 URL 已存在");
      urlChanged = record.url !== nextUrl;
      record.url = nextUrl;
      record.filename = filename;
    }

    if (body.name !== undefined) {
      record.name = normalizeName(body.name, record.filename);
    }

    if (body.enabled !== undefined) {
      enabledChanged = record.enabled !== Boolean(body.enabled);
      record.enabled = Boolean(body.enabled);
    }

    record.updatedAt = new Date().toISOString();
    await saveUrlRecords();

    let job = null;
    if (AUTO_SYNC_ON_CHANGE && record.enabled && (urlChanged || enabledChanged)) {
      job = createSyncJobForRecord(record, "URL 修改后自动同步");
    }

    sendJson(res, 200, { url: record, job });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

async function handleDeleteUrl(req, res, id) {
  if (!requireAccess(req, res)) return;

  const previousLength = urlRecords.length;
  urlRecords = urlRecords.filter((item) => item.id !== id);
  if (urlRecords.length === previousLength) {
    sendJson(res, 404, { error: "url not found" });
    return;
  }

  await saveUrlRecords();
  sendJson(res, 200, { ok: true });
}

function handleSyncUrl(req, res, id) {
  if (!requireAccess(req, res)) return;

  const record = urlRecords.find((item) => item.id === id);
  if (!record) {
    sendJson(res, 404, { error: "url not found" });
    return;
  }

  const job = createSyncJobForRecord(record, "手动同步单个 URL");
  if (!job) {
    sendJson(res, 400, { error: "URL 已禁用，不能同步" });
    return;
  }

  sendJson(res, 202, { id: job.id, job });
}

function handleSyncAll(req, res) {
  if (!requireAccess(req, res)) return;
  const created = syncAllEnabled("手动同步全部 URL");
  sendJson(res, 202, { count: created.length, jobs: created });
}

const server = http.createServer(async (req, res) => {
  const requestUrl = parseRequestUrl(req);
  const pathname = requestUrl.pathname;

  if (req.method === "GET" && pathname === "/api/config") {
    if (!requireAccess(req, res)) return;
    sendJson(res, 200, {
      remotePath: REMOTE_PATH,
      syncIntervalMinutes: SYNC_INTERVAL_MINUTES,
      autoSyncOnChange: AUTO_SYNC_ON_CHANGE,
      allowedExtensions: ALLOWED_EXTENSIONS
    });
    return;
  }

  if (req.method === "GET" && pathname === "/api/urls") {
    await handleListUrls(req, res);
    return;
  }

  if (req.method === "POST" && pathname === "/api/urls") {
    await handleCreateUrl(req, res);
    return;
  }

  const urlMatch = pathname.match(/^\/api\/urls\/([^/]+)$/);
  if (urlMatch && req.method === "PUT") {
    await handleUpdateUrl(req, res, decodeURIComponent(urlMatch[1]));
    return;
  }

  if (urlMatch && req.method === "DELETE") {
    await handleDeleteUrl(req, res, decodeURIComponent(urlMatch[1]));
    return;
  }

  const syncUrlMatch = pathname.match(/^\/api\/urls\/([^/]+)\/sync$/);
  if (syncUrlMatch && req.method === "POST") {
    handleSyncUrl(req, res, decodeURIComponent(syncUrlMatch[1]));
    return;
  }

  if (req.method === "POST" && pathname === "/api/sync-all") {
    handleSyncAll(req, res);
    return;
  }

  if (req.method === "GET" && pathname === "/api/jobs") {
    handleListJobs(req, res);
    return;
  }

  if (req.method === "POST" && pathname === "/api/jobs") {
    await handleCreateJob(req, res);
    return;
  }

  const jobMatch = pathname.match(/^\/api\/jobs\/([^/]+)$/);
  if (jobMatch && req.method === "GET") {
    handleGetJob(req, res, decodeURIComponent(jobMatch[1]));
    return;
  }

  if (req.method === "GET") {
    serveStatic(req, res);
    return;
  }

  res.writeHead(405);
  res.end("Method not allowed");
});

server.listen(PORT, HOST, () => {
  console.log(`URL to MinIO uploader listening on http://${HOST}:${PORT}`);
  console.log(`REMOTE_PATH=${REMOTE_PATH}`);
  console.log(`RCLONE_CONFIG=${RCLONE_CONFIG}`);
  console.log(`DATA_FILE=${DATA_FILE}`);
  console.log(`SYNC_INTERVAL_MINUTES=${SYNC_INTERVAL_MINUTES}`);
  if (SYNC_ON_START) syncAllEnabled("服务启动后自动同步");
});

if (SYNC_INTERVAL_MINUTES > 0) {
  setInterval(() => {
    syncAllEnabled("定时同步");
  }, SYNC_INTERVAL_MINUTES * 60 * 1000);
}

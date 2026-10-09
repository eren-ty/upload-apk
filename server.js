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
const RCLONE_PROGRESS = process.env.RCLONE_PROGRESS !== "false";
const RCLONE_TIMEOUT = process.env.RCLONE_TIMEOUT || "60s";
const RCLONE_CONNECT_TIMEOUT = process.env.RCLONE_CONNECT_TIMEOUT || "10s";
const RCLONE_RETRIES = process.env.RCLONE_RETRIES || "2";
const RCLONE_LOW_LEVEL_RETRIES = process.env.RCLONE_LOW_LEVEL_RETRIES || "2";
const UPLOAD_MAX_SECONDS = Number(process.env.UPLOAD_MAX_SECONDS || 120);
const UPLOAD_VIA_TEMP_OBJECT = process.env.UPLOAD_VIA_TEMP_OBJECT !== "false";
const ACCESS_TOKEN = process.env.ACCESS_TOKEN || "";
const SESSION_COOKIE = "upload_apk_token";
const MAX_ACTIVE_JOBS = Number(process.env.MAX_ACTIVE_JOBS || 2);
const SYNC_INTERVAL_MINUTES = Number(process.env.SYNC_INTERVAL_MINUTES || 360);
const CHECK_INTERVAL_MINUTES = Number(process.env.CHECK_INTERVAL_MINUTES || 10);
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
let checkingUpdates = false;

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
  const cookieToken = parseCookies(req.headers.cookie || "")[SESSION_COOKIE] || "";
  return header === `Bearer ${ACCESS_TOKEN}` || cookieToken === ACCESS_TOKEN;
}

function requireAccess(req, res) {
  if (hasAccess(req)) return true;
  sendJson(res, 401, { error: "访问令牌不正确，请填写服务端 ACCESS_TOKEN 的值" });
  return false;
}

function parseRequestUrl(req) {
  return new URL(req.url, `http://${req.headers.host || "localhost"}`);
}

function parseCookies(cookieHeader) {
  return cookieHeader.split(";").reduce((cookies, pair) => {
    const index = pair.indexOf("=");
    if (index === -1) return cookies;
    const key = pair.slice(0, index).trim();
    const value = pair.slice(index + 1).trim();
    if (key) cookies[key] = decodeURIComponent(value);
    return cookies;
  }, {});
}

function cookieOptions(maxAge) {
  const parts = [
    `${SESSION_COOKIE}=${encodeURIComponent(ACCESS_TOKEN)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax"
  ];
  if (maxAge !== undefined) parts.push(`Max-Age=${maxAge}`);
  return parts.join("; ");
}

function loadUrlRecords() {
  try {
    const content = fs.readFileSync(DATA_FILE, "utf8");
    const data = JSON.parse(content);
    urlRecords = Array.isArray(data.urls) ? data.urls : [];
    resetStaleRecordStatuses();
  } catch (error) {
    if (error.code !== "ENOENT") {
      console.error(`Failed to load ${DATA_FILE}:`, error.message);
    }
    urlRecords = [];
  }
}

function resetStaleRecordStatuses() {
  const staleStatuses = new Set(["queued", "running"]);
  let changed = false;

  for (const record of urlRecords) {
    if (!staleStatuses.has(record.lastStatus)) continue;
    record.lastStatus = record.lastSyncedAt ? "interrupted" : "never";
    record.lastError = "上次任务因服务重启或进程退出中断，请重新同步";
    changed = true;
  }

  if (changed) {
    saveUrlRecords().catch((error) => console.error("Failed to reset stale URL statuses:", error.message));
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

function runCommand(command, args, job, options = {}) {
  return new Promise((resolve, reject) => {
    appendLog(job, `$ ${command} ${args.map((arg) => JSON.stringify(arg)).join(" ")}`);
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let closed = false;
    const timeoutMs = Number(options.timeoutMs || 0);
    const timer = timeoutMs > 0 ? setTimeout(() => {
      timedOut = true;
      appendLog(job, `${command} 超过 ${Math.round(timeoutMs / 1000)} 秒未结束，强制终止`);
      child.kill("SIGTERM");
      setTimeout(() => {
        if (!closed) child.kill("SIGKILL");
      }, 5000);
    }, timeoutMs) : null;

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      appendLog(job, chunk.toString());
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      appendLog(job, chunk.toString());
    });
    child.on("error", (error) => {
      if (timer) clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      closed = true;
      if (timer) clearTimeout(timer);
      if (code === 0) resolve({ stdout, stderr });
      else {
        const error = new Error(timedOut ? `${command} timed out after ${Math.round(timeoutMs / 1000)} seconds` : `${command} exited with code ${code}`);
        error.timedOut = timedOut;
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
      }
    });
  });
}

function remoteObjectPath(filename) {
  return `${REMOTE_PATH.replace(/\/+$/, "")}/${filename}`;
}

function appendRcloneTransferOptions(args) {
  args.push(
    "--timeout",
    RCLONE_TIMEOUT,
    "--contimeout",
    RCLONE_CONNECT_TIMEOUT,
    "--retries",
    RCLONE_RETRIES,
    "--low-level-retries",
    RCLONE_LOW_LEVEL_RETRIES
  );

  if (RCLONE_PROGRESS) {
    args.push("--progress", "--stats", "1s", "--stats-one-line");
  }

  return args;
}

function buildRcloneArgs(file) {
  const args = [
    "copy",
    file,
    REMOTE_PATH,
    "--config",
    RCLONE_CONFIG
  ];

  return appendRcloneTransferOptions(args);
}

function buildRcloneCopyToArgs(source, destination) {
  return appendRcloneTransferOptions([
    "copyto",
    source,
    destination,
    "--config",
    RCLONE_CONFIG
  ]);
}

function buildRcloneDeleteFileArgs(filename) {
  return [
    "deletefile",
    remoteObjectPath(filename),
    "--config",
    RCLONE_CONFIG,
    "--timeout",
    RCLONE_TIMEOUT,
    "--contimeout",
    RCLONE_CONNECT_TIMEOUT
  ];
}

function buildRcloneLsjsonArgs(filename) {
  return [
    "lsjson",
    remoteObjectPath(filename),
    "--config",
    RCLONE_CONFIG,
    "--stats",
    "0"
  ];
}

function parseJsonArrayOutput(output) {
  const text = String(output || "").trim();
  if (!text) return [];

  const start = text.indexOf("[");
  const end = text.lastIndexOf("]");
  if (start === -1 || end === -1 || end < start) {
    throw new Error("rclone lsjson 没有返回有效 JSON");
  }

  return JSON.parse(text.slice(start, end + 1));
}

async function getRemoteObjectInfo(filename, job) {
  const result = await runCommand("rclone", buildRcloneLsjsonArgs(filename), job);
  const items = parseJsonArrayOutput(result.stdout);
  if (!Array.isArray(items) || items.length === 0) return null;
  return items[0];
}

async function verifyUploadedObject(file, filename, job, options = {}) {
  const localStat = await fs.promises.stat(file);
  const remoteInfo = await getRemoteObjectInfo(filename, job);
  const remoteSize = remoteInfo ? Number(remoteInfo.Size) : null;
  if (remoteSize === localStat.size) {
    if (options.minModTime) {
      const remoteModTime = Date.parse(remoteInfo.ModTime || "");
      const minModTime = options.minModTime.getTime() - 5000;
      if (!Number.isFinite(remoteModTime) || remoteModTime < minModTime) {
        appendLog(job, `MinIO 目标文件大小一致但更新时间未刷新: remote=${remoteInfo.ModTime || "unknown"}`);
        return false;
      }
    }

    appendLog(job, `MinIO 目标文件已存在且大小一致: ${remoteSize} bytes`);
    return true;
  }

  appendLog(job, `MinIO 目标文件大小不一致: local=${localStat.size}, remote=${remoteSize}`);
  return false;
}

async function deleteRemoteObject(filename, job) {
  try {
    await runCommand("rclone", buildRcloneDeleteFileArgs(filename), job, { timeoutMs: 60 * 1000 });
    appendLog(job, `已清理临时对象: ${filename}`);
  } catch (error) {
    appendLog(job, `清理临时对象失败: ${filename}, ${error.message}`);
  }
}

async function uploadDirectly(file, job) {
  appendLog(job, `开始上传到 MinIO: ${REMOTE_PATH}`);
  try {
    await runCommand("rclone", buildRcloneArgs(file), job, { timeoutMs: UPLOAD_MAX_SECONDS * 1000 });
  } catch (error) {
    appendLog(job, error.timedOut ? "上传命令超时，开始检查 MinIO 目标文件" : "上传命令失败，开始检查 MinIO 目标文件");
    const uploaded = await verifyUploadedObject(file, job.filename, job);
    if (!uploaded) throw error;
    appendLog(job, "上传命令异常但目标文件校验通过，按成功处理");
  }
}

async function uploadViaTempObject(file, job) {
  const tempFilename = `${job.filename}.uploading-${Date.now()}-${job.id.slice(0, 8)}`;
  let tempUploaded = false;

  try {
    appendLog(job, `开始上传到 MinIO 临时对象: ${remoteObjectPath(tempFilename)}`);
    try {
      await runCommand("rclone", buildRcloneCopyToArgs(file, remoteObjectPath(tempFilename)), job, {
        timeoutMs: UPLOAD_MAX_SECONDS * 1000
      });
    } catch (error) {
      appendLog(job, error.timedOut ? "临时对象上传超时，开始检查临时对象" : "临时对象上传异常，开始检查临时对象");
      const uploaded = await verifyUploadedObject(file, tempFilename, job);
      if (!uploaded) throw error;
      appendLog(job, "临时对象校验通过，继续覆盖正式对象");
    }

    tempUploaded = true;
    const tempReady = await verifyUploadedObject(file, tempFilename, job);
    if (!tempReady) throw new Error("临时对象上传后校验失败");

    const finalCopyStartedAt = new Date();
    appendLog(job, `临时对象校验通过，开始覆盖正式对象: ${remoteObjectPath(job.filename)}`);
    try {
      await runCommand("rclone", buildRcloneCopyToArgs(remoteObjectPath(tempFilename), remoteObjectPath(job.filename)), job, {
        timeoutMs: UPLOAD_MAX_SECONDS * 1000
      });
    } catch (error) {
      appendLog(job, error.timedOut ? "正式对象覆盖超时，开始检查正式对象" : "正式对象覆盖异常，开始检查正式对象");
      const uploaded = await verifyUploadedObject(file, job.filename, job, { minModTime: finalCopyStartedAt });
      if (!uploaded) throw error;
      appendLog(job, "正式对象校验通过，按成功处理");
    }

    const finalReady = await verifyUploadedObject(file, job.filename, job, { minModTime: finalCopyStartedAt });
    if (!finalReady) throw new Error("正式对象覆盖后校验失败");
  } finally {
    if (tempUploaded) await deleteRemoteObject(tempFilename, job);
  }
}

function hasPendingJobForRecord(record) {
  if (queue.some((job) => job.sourceId === record.id)) return true;
  return Array.from(jobs.values()).some((job) => {
    return job.sourceId === record.id && ["queued", "downloading", "uploading"].includes(job.status);
  });
}

async function fetchRemoteFingerprint(url) {
  const head = await fetchWithTimeout(url, { method: "HEAD" });
  if (head.ok) return fingerprintFromResponse(head);

  const partial = await fetchWithTimeout(url, {
    method: "GET",
    headers: { Range: "bytes=0-0" }
  });
  if (!partial.ok && partial.status !== 206) {
    throw new Error(`远程检测失败: HTTP ${partial.status}`);
  }
  return fingerprintFromResponse(partial);
}

async function fetchWithTimeout(url, options) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000);
  try {
    return await fetch(url, { ...options, redirect: "follow", signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function fingerprintFromResponse(response) {
  const etag = response.headers.get("etag") || "";
  const lastModified = response.headers.get("last-modified") || "";
  const contentLength = response.headers.get("content-length") || "";
  const contentRange = response.headers.get("content-range") || "";
  const fingerprint = [etag, lastModified, contentLength, contentRange].filter(Boolean).join("|");

  return {
    fingerprint: fingerprint || `status:${response.status}`,
    etag,
    lastModified,
    contentLength,
    contentRange,
    checkedAt: new Date().toISOString()
  };
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
    if (UPLOAD_VIA_TEMP_OBJECT) {
      await uploadViaTempObject(tmpFile, job);
    } else {
      await uploadDirectly(tmpFile, job);
    }

    job.status = "done";
    job.finishedAt = new Date().toISOString();
    appendLog(job, "完成");
    let remoteMeta = null;
    if (job.sourceId) {
      try {
        remoteMeta = await fetchRemoteFingerprint(job.url);
      } catch (error) {
        appendLog(job, `同步后检测远程版本失败: ${error.message}`);
      }
    }
    updateSourceFromJob(job, {
      lastStatus: "done",
      lastJobId: job.id,
      lastSyncedAt: job.finishedAt,
      lastCheckedAt: remoteMeta ? remoteMeta.checkedAt : undefined,
      remoteFingerprint: remoteMeta ? remoteMeta.fingerprint : undefined,
      remoteEtag: remoteMeta ? remoteMeta.etag : undefined,
      remoteLastModified: remoteMeta ? remoteMeta.lastModified : undefined,
      remoteContentLength: remoteMeta ? remoteMeta.contentLength : undefined,
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
  Object.keys(patch).forEach((key) => {
    if (patch[key] === undefined) delete patch[key];
  });
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
    lastCheckedAt: null,
    lastStatus: "never",
    lastJobId: null,
    lastError: null,
    remoteFingerprint: null,
    remoteEtag: null,
    remoteLastModified: null,
    remoteContentLength: null
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

async function checkUpdatedUrls(reason) {
  if (checkingUpdates) return [];
  checkingUpdates = true;
  const createdJobs = [];

  try {
    for (const record of urlRecords) {
      if (!record.enabled || hasPendingJobForRecord(record)) continue;

      try {
        const remoteMeta = await fetchRemoteFingerprint(record.url);
        const changed = record.remoteFingerprint && record.remoteFingerprint !== remoteMeta.fingerprint;
        record.lastCheckedAt = remoteMeta.checkedAt;
        record.remoteEtag = remoteMeta.etag;
        record.remoteLastModified = remoteMeta.lastModified;
        record.remoteContentLength = remoteMeta.contentLength;

        if (!record.remoteFingerprint) {
          record.remoteFingerprint = remoteMeta.fingerprint;
          record.lastStatus = record.lastStatus === "never" ? "checked" : record.lastStatus;
          record.lastError = null;
          continue;
        }

        if (changed) {
          record.remoteFingerprint = remoteMeta.fingerprint;
          record.lastChangeAt = remoteMeta.checkedAt;
          const job = createSyncJobForRecord(record, reason);
          if (job) createdJobs.push(job);
        } else {
          record.lastStatus = record.lastStatus === "never" ? "checked" : record.lastStatus;
          record.lastError = null;
        }
      } catch (error) {
        record.lastCheckedAt = new Date().toISOString();
        record.lastStatus = "check_failed";
        record.lastError = error.message;
      }
    }

    await saveUrlRecords();
  } finally {
    checkingUpdates = false;
  }

  return createdJobs;
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

async function handleLogin(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    const token = String(body.token || "");
    if (ACCESS_TOKEN && token !== ACCESS_TOKEN) {
      sendJson(res, 401, { error: "登录密码不正确" });
      return;
    }

    const payload = JSON.stringify({ ok: true });
    res.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
      "content-length": Buffer.byteLength(payload),
      "set-cookie": cookieOptions(7 * 24 * 60 * 60)
    });
    res.end(payload);
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

function handleLogout(req, res) {
  const payload = JSON.stringify({ ok: true });
  res.writeHead(200, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
    "set-cookie": `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
  });
  res.end(payload);
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

async function handleCheckUpdates(req, res) {
  if (!requireAccess(req, res)) return;
  const created = await checkUpdatedUrls("检测到远程文件更新后自动同步");
  sendJson(res, 202, { count: created.length, jobs: created });
}

const server = http.createServer(async (req, res) => {
  const requestUrl = parseRequestUrl(req);
  const pathname = requestUrl.pathname;

  if (req.method === "POST" && pathname === "/api/login") {
    await handleLogin(req, res);
    return;
  }

  if (req.method === "POST" && pathname === "/api/logout") {
    handleLogout(req, res);
    return;
  }

  if (req.method === "GET" && pathname === "/api/config") {
    if (!requireAccess(req, res)) return;
    sendJson(res, 200, {
      remotePath: REMOTE_PATH,
      syncIntervalMinutes: SYNC_INTERVAL_MINUTES,
      checkIntervalMinutes: CHECK_INTERVAL_MINUTES,
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

  if (req.method === "POST" && pathname === "/api/check-updates") {
    await handleCheckUpdates(req, res);
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
  console.log(`RCLONE_PROGRESS=${RCLONE_PROGRESS}`);
  console.log(`RCLONE_TIMEOUT=${RCLONE_TIMEOUT}`);
  console.log(`RCLONE_CONNECT_TIMEOUT=${RCLONE_CONNECT_TIMEOUT}`);
  console.log(`RCLONE_RETRIES=${RCLONE_RETRIES}`);
  console.log(`RCLONE_LOW_LEVEL_RETRIES=${RCLONE_LOW_LEVEL_RETRIES}`);
  console.log(`UPLOAD_MAX_SECONDS=${UPLOAD_MAX_SECONDS}`);
  console.log(`UPLOAD_VIA_TEMP_OBJECT=${UPLOAD_VIA_TEMP_OBJECT}`);
  console.log(`DATA_FILE=${DATA_FILE}`);
  console.log(`SYNC_INTERVAL_MINUTES=${SYNC_INTERVAL_MINUTES}`);
  console.log(`CHECK_INTERVAL_MINUTES=${CHECK_INTERVAL_MINUTES}`);
  if (SYNC_ON_START) syncAllEnabled("服务启动后自动同步");
});

if (SYNC_INTERVAL_MINUTES > 0) {
  setInterval(() => {
    syncAllEnabled("定时同步");
  }, SYNC_INTERVAL_MINUTES * 60 * 1000);
}

if (CHECK_INTERVAL_MINUTES > 0) {
  setInterval(() => {
    checkUpdatedUrls("检测到远程文件更新后自动同步").catch((error) => {
      console.error("Failed to check URL updates:", error.message);
    });
  }, CHECK_INTERVAL_MINUTES * 60 * 1000);
}

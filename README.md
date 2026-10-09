# MinIO URL 同步后台

这个服务替代原来的定时脚本维护方式：用户在页面里新增、修改、删除下载 URL，服务端负责定时下载并同步到自建 MinIO。

## 功能

- 页面维护 URL 列表，不需要再改 shell 脚本
- 新增或修改 URL 后可自动触发同步
- 支持手动同步单个 URL
- 支持手动同步全部 URL
- 支持按固定间隔定时同步全部启用的 URL
- 支持定时检测远程文件是否更新，更新后才自动同步
- URL 列表持久化保存到本地 JSON 文件
- 支持后台登录认证，避免页面暴露后被随意操作
- 后端仍然使用 `curl` 下载，使用 `rclone` 上传到 MinIO
- 默认先上传到临时对象，校验成功后删除旧正式对象，再创建新正式对象，减少直接覆盖已有文件时卡住的问题

## 运行要求

- Node.js 18+
- 服务器已安装 `curl`
- 服务器已安装 `rclone`
- `rclone` 已配置好 MinIO remote，例如 `minio`

## 启动

```bash
PORT=3000 \
REMOTE_PATH="minio:app-pkg/downloads/apks" \
RCLONE_CONFIG="/root/.config/rclone/rsync_oss.conf" \
ACCESS_TOKEN="your-secret-token" \
SYNC_INTERVAL_MINUTES=360 \
CHECK_INTERVAL_MINUTES=10 \
RCLONE_PROGRESS=true \
RCLONE_TIMEOUT=60s \
RCLONE_CONNECT_TIMEOUT=10s \
UPLOAD_MAX_SECONDS=120 \
DELETE_TIMEOUT_SECONDS=180 \
DELETE_RETRIES=3 \
DELETE_RETRY_DELAY_MS=3000 \
UPLOAD_VIA_TEMP_OBJECT=true \
node server.js
```

打开：

```text
http://服务器IP:3000
```

打开页面后使用 `ACCESS_TOKEN` 的值登录。如果服务端没有设置 `ACCESS_TOKEN`，页面登录时可输入任意值，但不建议公网这样部署。

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | Web 服务端口 |
| `HOST` | `0.0.0.0` | Web 服务监听地址 |
| `REMOTE_PATH` | `minio:app-pkg/downloads/apks` | rclone 上传目标 |
| `RCLONE_CONFIG` | `/root/.config/rclone/rsync_oss.conf` | rclone 配置文件路径 |
| `RCLONE_PROGRESS` | `true` | 上传时是否输出 rclone 进度和速度 |
| `RCLONE_TIMEOUT` | `60s` | rclone 上传无响应超时时间 |
| `RCLONE_CONNECT_TIMEOUT` | `10s` | rclone 连接 MinIO 超时时间 |
| `RCLONE_RETRIES` | `2` | rclone 失败重试次数 |
| `RCLONE_LOW_LEVEL_RETRIES` | `2` | rclone 底层请求重试次数 |
| `UPLOAD_MAX_SECONDS` | `120` | 上传命令硬超时；上传异常后会检查 MinIO 目标文件大小，一致则按成功处理 |
| `DELETE_TIMEOUT_SECONDS` | `180` | 删除旧正式对象单次等待秒数；部分 MinIO 删除会慢于上传 |
| `DELETE_RETRIES` | `3` | 删除旧正式对象失败或超时后的重试次数 |
| `DELETE_RETRY_DELAY_MS` | `3000` | 删除旧正式对象失败后的重试等待时间，单位毫秒 |
| `UPLOAD_VIA_TEMP_OBJECT` | `true` | 先上传到临时对象，校验成功后删除旧正式对象，再创建新正式对象；适合已有同名文件覆盖容易卡住的 MinIO 环境 |
| `ACCESS_TOKEN` | 空 | 后台登录密码 |
| `DATA_FILE` | `./data/urls.json` | URL 列表保存位置 |
| `MAX_ACTIVE_JOBS` | `2` | 同时下载上传的任务数 |
| `SYNC_INTERVAL_MINUTES` | `360` | 定时同步间隔；设为 `0` 表示关闭定时同步 |
| `CHECK_INTERVAL_MINUTES` | `10` | 检测远程文件是否更新的间隔；设为 `0` 表示关闭智能检测 |
| `AUTO_SYNC_ON_CHANGE` | `true` | 新增或修改 URL 后是否自动同步 |
| `SYNC_ON_START` | `false` | 服务启动后是否立即同步全部启用 URL |
| `ALLOWED_EXTENSIONS` | `.apk` | 允许的文件后缀，多个用英文逗号分隔 |

## systemd 示例

```ini
[Unit]
Description=MinIO URL Sync Admin
After=network.target

[Service]
WorkingDirectory=/opt/url-to-minio-uploader
ExecStart=/usr/bin/node server.js
Restart=always
Environment=PORT=3000
Environment=REMOTE_PATH=minio:app-pkg/downloads/apks
Environment=RCLONE_CONFIG=/root/.config/rclone/rsync_oss.conf
Environment=RCLONE_PROGRESS=true
Environment=RCLONE_TIMEOUT=60s
Environment=RCLONE_CONNECT_TIMEOUT=10s
Environment=RCLONE_RETRIES=2
Environment=RCLONE_LOW_LEVEL_RETRIES=2
Environment=UPLOAD_MAX_SECONDS=120
Environment=DELETE_TIMEOUT_SECONDS=180
Environment=DELETE_RETRIES=3
Environment=DELETE_RETRY_DELAY_MS=3000
Environment=UPLOAD_VIA_TEMP_OBJECT=true
Environment=ACCESS_TOKEN=your-secret-token
Environment=SYNC_INTERVAL_MINUTES=360
Environment=CHECK_INTERVAL_MINUTES=10
Environment=AUTO_SYNC_ON_CHANGE=true
Environment=DATA_FILE=/opt/url-to-minio-uploader/data/urls.json

[Install]
WantedBy=multi-user.target
```

## 从旧脚本迁移

把旧脚本里的 URL 通过页面逐条新增即可。新增后默认会立即同步一次，之后服务会按 `CHECK_INTERVAL_MINUTES` 检测远程文件是否变化，检测到变化才重新同步；也可以按 `SYNC_INTERVAL_MINUTES` 做兜底的全量定时同步。

如果你想直接导入，也可以编辑 `DATA_FILE` 指向的 JSON 文件，但更建议通过页面操作，避免格式写错。

## 注意

- 不建议公网直接暴露未设置 `ACCESS_TOKEN` 的服务。
- 当前默认只允许 `.apk` 后缀，避免被当成任意文件下载器滥用。
- 定时同步是“固定间隔”模式，不是 cron 表达式；如果必须精确到每天某个时间，可以用 systemd timer 或外部 cron 调用 `/api/sync-all`。
- 如果需要 HTTPS，建议前面放 Nginx 或 Caddy 反向代理。

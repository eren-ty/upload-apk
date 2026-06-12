# MinIO URL 同步后台

这个服务替代原来的定时脚本维护方式：用户在页面里新增、修改、删除下载 URL，服务端负责定时下载并同步到自建 MinIO。

## 功能

- 页面维护 URL 列表，不需要再改 shell 脚本
- 新增或修改 URL 后可自动触发同步
- 支持手动同步单个 URL
- 支持手动同步全部 URL
- 支持按固定间隔定时同步全部启用的 URL
- URL 列表持久化保存到本地 JSON 文件
- 后端仍然使用 `curl` 下载，使用 `rclone copy` 上传到 MinIO

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
node server.js
```

打开：

```text
http://服务器IP:3000
```

页面里的“访问令牌”填写 `ACCESS_TOKEN` 的值。如果服务端没有设置 `ACCESS_TOKEN`，页面可以留空，但不建议公网这样部署。

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | Web 服务端口 |
| `HOST` | `0.0.0.0` | Web 服务监听地址 |
| `REMOTE_PATH` | `minio:app-pkg/downloads/apks` | rclone 上传目标 |
| `RCLONE_CONFIG` | `/root/.config/rclone/rsync_oss.conf` | rclone 配置文件路径 |
| `ACCESS_TOKEN` | 空 | 设置后，页面请求必须填写同一个令牌 |
| `DATA_FILE` | `./data/urls.json` | URL 列表保存位置 |
| `MAX_ACTIVE_JOBS` | `2` | 同时下载上传的任务数 |
| `SYNC_INTERVAL_MINUTES` | `360` | 定时同步间隔；设为 `0` 表示关闭定时同步 |
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
Environment=ACCESS_TOKEN=your-secret-token
Environment=SYNC_INTERVAL_MINUTES=360
Environment=AUTO_SYNC_ON_CHANGE=true
Environment=DATA_FILE=/opt/url-to-minio-uploader/data/urls.json

[Install]
WantedBy=multi-user.target
```

## 从旧脚本迁移

把旧脚本里的 URL 通过页面逐条新增即可。新增后默认会立即同步一次，之后由服务按 `SYNC_INTERVAL_MINUTES` 定时同步。

如果你想直接导入，也可以编辑 `DATA_FILE` 指向的 JSON 文件，但更建议通过页面操作，避免格式写错。

## 注意

- 不建议公网直接暴露未设置 `ACCESS_TOKEN` 的服务。
- 当前默认只允许 `.apk` 后缀，避免被当成任意文件下载器滥用。
- 定时同步是“固定间隔”模式，不是 cron 表达式；如果必须精确到每天某个时间，可以用 systemd timer 或外部 cron 调用 `/api/sync-all`。
- 如果需要 HTTPS，建议前面放 Nginx 或 Caddy 反向代理。

# rclone baidunetdisk 后端（baidu-upload-mod 分支）

本分支在官方 rclone 基础上，增加了一个专门面向百度网盘的后端 `baidunetdisk`。  
目标是：在 rclone 中 1:1 复刻 OpenList / alist 魔改版的百度网盘上传下载行为，用于大文件批量同步。

> 警告：这是非官方后端，接口策略完全受百度控制。建议自用，避免高风险场景。

---

## 功能概览

- 新增存储后端：`baidunetdisk`
  - 支持普通文件/目录的上传、下载、列目录。
  - 支持服务器端的 `Copy` / `Move` / `DirMove` / `Purge` 等高级操作。
- 上传特性
  - 优先尝试秒传（rapid upload）。
  - 否则使用 `precreate + superfile2` 分片上传，支持断点续传。
  - 每个分片有独立重试机制（指数退避）。
  - 上传进度按 `content-md5 + access_token` 落盘，可跨进程恢复。
- 下载特性
  - 使用官方 `xpan/multimedia?method=filemetas` 流程获取下载链接。
  - 强制带 `User-Agent: pan.baidu.com`，兼容官方客户端链路。
- 与百度特性对齐的限制
  - **不提供可靠 MD5 校验和**（百度的 MD5 非真正内容 MD5）。
  - **不提供可靠 mtime**（百度返回的是上传时刻，而非文件原始修改时间）。
  - rclone 在此后端上等价于 **“按文件名 + 大小判断是否一致”**。

---

## 配置方式

通过 `rclone config` 创建一个 `baidunetdisk` 远端，例如：

```bash
rclone-bd config
  > n) New remote
  > name> baidu-main
  > Storage> baidunetdisk
```

### 核心配置项

这些选项在 `rclone config` 中会以“高级选项”的形式出现：

- `refresh_token`（必填）
  - 百度网盘 OAuth 的 refresh token。
  - 通常从 OpenList / 其它工具导出，或者走一次手动 OAuth。

- `client_id` / `client_secret`（可选，高级）
  - 当 `use_online_api = false` 时，用于直接调用百度官方 OAuth 刷新 token。

- `use_online_api`（默认 `true`，高级）
  - `true`：通过在线 API 刷新 token（默认使用 OpenList API：`https://api.oplist.org/baiduyun/renewapi`）。
  - `false`：直接调用百度官方 `oauth/2.0/token` 刷新。

- `api_url_address`（高级）
  - 在线 API 地址，默认 `https://api.oplist.org/baiduyun/renewapi`。

- `upload_thread`（高级）
  - 并发上传分片数量，范围 `1–64`，默认 `32`。
  - 实际并发 = 此值；无需再额外加 Semaphore。

- `upload_timeout`（高级）
  - 单个分片上传超时（秒），默认 `60` 秒。超时会触发该分片重试。

- `upload_api`（高级）
  - 固定上传域名，默认 `https://d.pcs.baidu.com`。
  - 当未启用动态上传域名或 `locateupload` 调用失败时，会回退使用该值。

- `use_dynamic_upload_api` / `dynamic_upload_api_rotate`（高级）
  - `use_dynamic_upload_api` 默认 `true`：会调用官方 `locateupload` 接口为每个上传会话解析推荐的上传域名；设为 `false` 时，仅使用 `upload_api`。
  - `dynamic_upload_api_rotate` 控制分片上传过程中多久重新解析一次上传域名（默认 `256`，单位：分片序号；设为 `0` 可禁用轮换，仅首次解析一次）。

- `dynamic_upload_api_random_pick`（高级）
  - 配合 `use_dynamic_upload_api=true` 使用，默认 `true`。
  - 为 `true` 时，从 `locateupload` 返回的 https `servers` 列表中随机选择一个上传域名；若该列表为空，则从 `bak_servers` 的 https 域名中随机选择。
  - 为 `false` 时，始终选择 https servers 列表中的第一个（若有），行为更稳定可预测。

- `dynamic_upload_api_slice_random_pick`（高级）
  - 配合 `use_dynamic_upload_api=true` 使用。
  - 为 `true` 时，会在每个分片上传和重试时，从最近一次 `locateupload` 缓存的 https 域名列表中随机选择上传主机；多个分片会均匀打散到不同 host 上。
  - 为 `false`（默认）时，同一轮 locate 解析出的 host 会在该分片的所有重试中保持不变，仅在 `dynamic_upload_api_rotate` 触发时才更换主机。

- `custom_upload_part_size`（高级）
  - 自定义分片大小（字节）。  
  - 受会员等级限制：普通用户固定 4 MiB，VIP / SVIP 有更大上限。

- `low_bandwith_upload_mode`（高级）
  - 是否启用“低带宽模式”，默认 `true`：从 4 MiB 开始逐步增大分片大小，保证总分片数 ≤ 2048。

- `upload_retry_count`（高级）
  - 每个分片的最大重试次数。默认 `10`。

- `upload_retry_initial_wait` / `upload_retry_max_wait`（高级）
  - 分片重试的初始退避时间 / 最大退避时间（默认为 1s / 5s）。
  - 退避策略为指数退避，封顶于 `upload_retry_max_wait`。

- `serverside_md5_override`（高级）
  - 默认 `false`。
  - `true`：precreate 仍按本地计算的 `block_list` 提交，但真正执行最终 `create` 时，会完全使用每个分片 `superfile2` 响应中**服务端返回的 md5 列表**来构造 `block_list`；如果某个分片响应缺失 md5，则该分片会被视为失败并重试，最终若仍有分片缺 md5，则整次上传失败（不会静默回退到本地 md5）。
  - `false`：`create` 仅使用本地计算的分片 md5 列表，`superfile2` 的 md5 仅用于日志/排错，不参与 `block_list` 纠偏。无论该选项如何设置，本后端对外都不声明 HashMD5 支持。

- `order_by` / `order_direction`（高级）
  - 列目录排序字段：`name|time|size`，默认 `name`。
  - 排序方向：`asc|desc`，默认 `asc`。

> 说明：`access_token` 字段在配置中不需要手动填写。后端会在刷新 token 成功后将新 `access_token` 持久化回 `rclone.conf`。

---

## 上传原理

### 1. 秒传（rapid upload）

1. 打开输入流，将数据写入一个可随机访问的临时文件（必要时落盘）。  
2. 根据文件大小和用户 VIP 等级，计算分片大小 `sliceSize`，保证总分片数 ≤ 2048。  
3. 按 `sliceSize` 计算：
   - 整体 `content-md5`；
   - 前 256 KiB 的 `slice-md5`；
   - 每个分片的 MD5 列表 `block_list`。
4. 构造 `block_list = [content-md5]` 调用 `xpan/file?method=create`，尝试 rapid 上传：
   - 命中则直接返回新对象（并修正 mtime 到源文件时间）。
   - 未命中则进入分片上传流程。

### 2. 分片上传（precreate + superfile2）

1. 首次上传 / 无进度缓存：
   - 调用 `xpan/file?method=precreate`：
     - `path`, `size`, `isdir=0`, `autoinit=1`, `rtype=3`, `block_list`；
     - 首次会附带 `content-md5`/`slice-md5` 以便服务器做秒传判定。
   - 若 `ReturnType=2`，说明服务端已有相同内容，直接当作秒传成功。
2. 如果 `ReturnType=1`：
   - 记录 `uploadid` 和 `block_list`（分片序号列表）。
   - 计算每个分片的 `(offset, size)`，并发调用 `pcs/superfile2?method=upload`。
   - 每个分片：
     - 使用 multipart/form-data 构造请求体，避免 chunked 传输；
     - 单片内有 `upload_retry_count` 次重试，带指数退避，受 `upload_timeout` 控制。
3. 断点续传：
   - 进度会持久化到缓存目录：`~/.cache/rclone/baidunetdisk/<remoteName>/`。  
   - key 为 `content-md5 + "_" + access_token`，内容是 `PrecreateResp`，包含 `uploadid` 和未完成的 `block_list`。
   - 下次上传同一文件时，会先尝试加载该进度，只重传未完成的分片。
4. `uploadid` 过期：
   - 后端在分片响应中检测 uploadid 失效错误，返回内部 `errUploadIDExpired`。  
   - 外层会重新 `precreate`（不再带 content/slice md5），丢弃旧进度，从头重新上传剩余分片。

### 3. 最终合并（create）

所有分片成功后，调用：

- `xpan/file?method=create`：
  - `path`, `size`, `isdir=0`, `rtype=3`, `uploadid`, `block_list`；
  - 同时设置 `local_mtime/local_ctime`（虽然百度不会按原意返回这些时间）。

返回的 File 信息中，百度的 `server_mtime` 仍然是当前时间；后端会在内存中将对象的 `modTime/ctime` 强制覆盖为源文件时间，以便当前命令内的日志/对象信息一致。但**下次 `list` 时，仍然只能拿到百度返回的时间，因此我们在同步判断上直接放弃使用 mtime**（见下一节）。

---

## 下载原理

- 只使用官方 API：
  - `xpan/multimedia?method=filemetas&fsids=[...]&dlink=1` 获取 `dlink`；
  - 拼接 `dlink + access_token` 后先发 `HEAD` 请求，禁止自动跟随重定向，从 `Location` 拿到真实下载 URL；
  - 对真实 URL 发 `GET` 请求，携带 `User-Agent: pan.baidu.com`，始终走官方下载链路（不使用 crack/crack_video 等非官方接口）。
- Range / 多线程支持（对齐 rclone 官方语义）：
  - `Object.Open` 支持 rclone 的 `RangeOption`，通过 `Range` 头把“只下载某一段 bytes”的需求下沉到 Baidu 官方 HTTP 链路；
  - 配合 `--multi-thread-streams` / `--multi-thread-cutoff` 等参数，由 rclone 主程序负责把一个对象拆成多个 Range，并发调用多次 `Open`，本后端每次仍是一条官方 GET；
  - 后端自身不做“本地再分片”或额外的多线程调度，只提供稳定的 Range 支持。
- 下载错误处理与重试：
  - 对 `filemetas`、HEAD、GET 的错误响应体尝试解析 Baidu 的 JSON 错误（`errno` / `error_code` / `errmsg` / `request_id`）；
  - 遇到 `31045`（access_token 失效）时，会自动调用配置好的在线 API 或官方 OAuth 刷新 token，然后重新执行一次完整的 `filemetas + HEAD + GET`；
  - 遇到 `31360`（dlink 过期）或 `31362`（签名错误）时，会重新调用 `filemetas` 获取新的 dlink，再重试下载；
  - 遇到 `31326`（防盗链）时不会盲目重试，而是直接返回错误，保留 errno / status / request_id 方便排查。
- 不实现 / 不迁移以下 OpenList 特性：
  - 各种 crack / crack_video 等非官方下载接口；
  - only_list_video_file 等特定业务行为。

---

## 一致性与校验策略

百度网盘的 API 限制决定了这个后端在校验上只能做到：

- **按文件名 + 文件大小判断是否一致**，不做真正的内容校验。

具体实现：

- `Hashes()` 返回 `hash.None`，`Object.Hash()` 始终返回 `hash.ErrUnsupported`：
  - rclone 不会使用百度返回的 MD5 做传输后校验；
  - 避免“上传成功但 MD5 不同 → 错误判为 corrupted on transfer”。
- `Precision()` 返回 `fs.ModTimeNotSupported`：
  - 向 rclone 声明：该后端不提供可靠的 mtime；
  - 同步/复制时不会再用 mtime 参与判等，等价于自动启用“size-only”语义。

这意味着：

- 第二轮 `copy` / `sync` 不会因为 mtime 或 Baidu 的伪 MD5 而触发重复上传；
- 如果你需要更强的校验，只能通过额外手段（例如在另一端生成校验和文件、自己比对），rclone 这里不会替你做严格验证。

---

## 高级 FS 操作

`baidunetdisk` 后端实现了部分类似 OpenList 的服务器端操作：

- `Copy` / `Move`：
  - 使用 `xpan/file?method=filemanager&opera=copy|move`；
  - 参数包括 `path`, `dest`, `newname`。

- `DirMove`：
  - 在同一远端内使用 `filemanager&opera=move` 将整个目录树移动到新位置。

- `Purge`：
  - 使用 `filemanager&opera=delete` 删除指定目录及其内容。

这些操作的行为与 OpenList 中的百度网盘驱动保持一致，但同样受百度官方接口限制。

---

## 使用示例

### 配置远端

```bash
rclone-bd config
  name> baidu-main
  Storage> baidunetdisk
  # 按提示填写 refresh_token 等参数
```

### 基本复制

```bash
# 本地目录 -> 百度网盘目录
rclone-bd copy 2025-11-28 baidu-main:test1 -P -vv
```

### 大文件上传建议

- 为大文件（多 GB）场景调整：
  - `upload_thread`：例如 32–64；
  - `upload_timeout`：根据网络情况酌情提高；
  - `upload_retry_count` + `upload_retry_max_wait`：适当增大，改善长链路稳定性。
- 当遇到错误 `errno=-9 / errno=10` 时：
  - `-9` 多为路径/空目录场景，后端已做兼容处理；
  - `10` 通常是百度认为分片状态/参数有问题，本分支已经按 OpenList 的逻辑做了对齐，如果仍遇到，建议降低并发或重试。

---

## 差异小结（相对于官方 rclone）

- 多了一个 **实验性** 后端：`baidunetdisk`，专门用于百度网盘。
- 上传逻辑 1:1 参考 OpenList / alist 魔改版：
  - 优先秒传；
  - 分片上传 + 断点续传 + uploadid 过期重试；
  - 可调的并发、分片大小和重试策略。
- 故意关闭：
  - Baidu MD5 的校验使用（只展示、不参与一致性判断）；
  - mtime 用于 sync 判等（Precision=ModTimeNotSupported）。

如果你只关心“文件名 + 大小一致即可”，这个后端已经可以投入日常使用；  
如果需要严格比对内容完整性，则需要在 rclone 之外额外设计校验策略。

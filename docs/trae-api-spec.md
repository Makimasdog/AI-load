# Trae 反代渠道 · 上游协议规格（gpt-load fork）

> 版本：2026-11-12 · 结论来源：GitHub 上多个独立实现源码 + 本机 Trae 客户端（CN 3.4.18）取证 + 线上端点可达性实测。
> 本文不含任何真实 token / cookie / 私钥，字段只描述结构与用途。

---

## 0. 结论速览

| 项目 | 结论 |
| --- | --- |
| 能否反代 | **可以**。Trae 的 AI 网关是普通 HTTPS + SSE，可用纯 HTTP 调用，不需要启动 Trae 客户端。 |
| 渠道拆分 | 按用户要求做**两个独立渠道**：`trae`（国内版）与 `trae_intl`（国际版），各自独立 ID、base URL、鉴权域，共享同一份代码，差异收敛在 realm 常量表。 |
| 聊天端点 | `POST {core}/api/ide/v2/llm_raw_chat`（CN，回退 v1）；`POST {core}/api/ide/v1/llm_raw_chat`（INTL） |
| 鉴权 | 仅 token / refreshToken，无 cookie。请求头 `Authorization: Cloud-IDE-JWT <token>`（INTL 部分实现用 `Bearer`）、`x-ide-token`、`X-Cloudide-Token` 三处同值。 |
| 响应形态 | **不是原生 OpenAI chunk**：服务端恒回 SSE，事件 `metadata / output / token_usage / done / error`，需要网关翻译。 |
| 硬门槛 | 两区都走同一套客户端续期协议（refreshToken + ECDSA-P256 设备证明）；INTL 还需地区一致的出口 IP、`model_list` 不可用需本地快照。 |
| 主要风险 | UA 会被当爬虫识别（报 4011「限流」）、设备头/出口变动触发 4019 风控、refreshToken 轮换（旧值立即失效，必须原子落盘）。 |
| 网页登录 | **可用**。`/authorization` + PKCE + loopback `/authorize` 回调，`ExchangeToken` 不带 DeviceProof；设备密钥对由服务端生成并随登录注册（详见 §2.5）。 |

---

## 1. 区域与端点表

### 1.1 国内版（渠道 `trae`）

| 用途 | 主机 | 路径 |
| --- | --- | --- |
| AI 核心（聊天/模型） | `https://trae-api-cn.mchost.guru` | `/api/ide/v2/llm_raw_chat`、`/api/ide/v1/llm_raw_chat`、`/api/ide/v1/model_list`、`/api/v1/commercial/get_session_usage` |
| OAuth / 身份 | `https://api.trae.com.cn` | `/cloudide/api/v3/trae/oauth/ExchangeToken`、`/cloudide/api/v3/trae/GetUserInfo`、`/cloudide/api/v3/trae/GenerateTempToken` |
| 付费 / 额度 | `https://api.trae.cn`（兼容 `api.trae.com.cn`） | `/trae/api/v2/pay/ide_user_ent_usage`、`/trae/api/v2/pay/ide_user_pay_status` |
| 签到 | `https://api.trae.cn` | `/trae/api/v2/ug/checkin_credits/status`、`/claim` |
| 控制台 | `https://www.trae.cn` | 登录页 / 授权页 |

本机取证佐证：`D:\Trae CN\resources\app\product.json` 的 `bootConfig` 中
`iCube.normal = https://api.trae.com.cn`、`agent/remote/ckg/cue/hub.trae.normal = https://trae-api-cn.mchost.guru`、
`account.trae.normal = https://api.trae.cn`、`ug.trae.normal = https://api.trae.cn`、
`ws.trae.normal = wss://trae-ws-cn.mchost.guru/custom_model`（自定义模型通道，非本次范围）。

线上实测（无凭据）：

| 请求 | 结果 |
| --- | --- |
| `GET https://trae-api-cn.mchost.guru/api/ide/v1/ping` | `200 {"message":"pong"}` |
| `GET https://trae-api-cn.mchost.guru/api/ide/v1/model_list` | `401`（端点存在，需鉴权） |
| `GET https://api.trae.com.cn/api/ide/v1/ping` | `404`（印证 `api/ide/*` 不在该主机） |
| `POST https://api.trae.cn/trae/api/v2/pay/ide_user_pay_status` | `401`（需鉴权） |

### 1.2 国际版（渠道 `trae_intl`）

| 用途 | 主机 | 路径 |
| --- | --- | --- |
| AI 核心 | `https://core-normal.traeapi.us`（回退 `https://core-normal.trae.ai`、`https://coresg-normal.trae.ai`、`https://coreva-normal.trae.ai`） | `/api/ide/v1/llm_raw_chat`、`/api/v1/commercial/get_session_usage` |
| OAuth / 身份 | `https://ug-normal.trae.ai`（回退 `https://ug-normal.traeapi.us`、`https://grow-normal.trae.ai`） | `/trae/api/v3/oauth/ExchangeToken`、`/cloudide/api/v3/trae/GetUserInfo`（`/cloudide/.../oauth/ExchangeToken` 同机可用） |
| 付费 / 额度 | `https://grow-normal.traeapi.us`（回退 `https://grow-normal.trae.ai`） | `/trae/api/v1/pay/ide_user_ent_usage`、`/trae/api/v1/pay/ide_user_pay_status` |
| 控制台 | `https://www.trae.ai` | 登录页 / 授权页 |

线上实测：`https://core-normal.trae.ai/api/ide/v1/ping` → `200 pong`；`/model_list` → `401`；
`https://api.trae.ai/cloudide/api/v3/trae/GetUserInfo` → `401`（存在）；`api.trae.ai` 上的 `/trae/api/v2/pay/*` → `404`。

> 差异说明：INTL 的 `model_list` 在多数账号上返回 `403 no schema found`，因此国际版渠道**内置模型快照**兜底，动态拉取失败不算账号异常。

---

## 2. 鉴权与凭证

### 2.1 凭证来源

| 方式 | 说明 | 本次实现 |
| --- | --- | --- |
| 粘贴 refreshToken + 设备私钥 | 用户从国际版客户端的 `storage.json` 或社区工具导出，连同 deviceId/machineId 一起粘贴 | ✅ `ImportCredential`（JSON 导入） |
| 直接粘贴国际版 `storage.json` 明文片段 | 国际版的 `iCubeAuthInfo://icube.cloudide` 与 `iCubeAuthInfo://icube-dc:*` 是**明文 JSON**，整份粘贴即可 | ✅ 自动识别并提取 |
| 国内版 `storage.json` | 同名键是 `tc` 密文信封（AES-128-CBC + 内置盐表），需要工具先解密 | ⛔ 明确报错 `ErrEncryptedStorage`，不做假成功 |
| 浏览器 OAuth（PKCE + loopback 回调） | 点「登录账号」→ 打开官方授权页 → 回调本地端口自动换 token；设备密钥对由服务端生成并随登录注册 | ✅ 已实现（CN/INTL 各自独立回调端口） |

本机取证（只记录结构，不含任何凭据值）：

- 已安装并运行的是**国内版** `Trae CN 3.4.18`（`D:\Trae CN`，进程名 `Trae CN`）；`D:\Trae` 只有残留 `debug.log`。
- 同时存在 `%APPDATA%\Trae CN`、`%APPDATA%\Trae`（国际版目录，仅少量缓存）、`%APPDATA%\TRAE SOLO CN`。
- `%APPDATA%\Trae CN\User\globalStorage\storage.json` 中的登录态键（国际版同名键结构一致，但值可能是明文 JSON）：
  - `iCubeAuthInfo://icube.cloudide`（access/refresh token、userId、expiredAt）
  - `iCubeAuthInfo://icube-dc:<deviceId>`（EC P-256 密钥对：`privateKeyPEM` / `publicKeyPEM`）
  - `telemetry.machineId`（64 位 hex 机器码）
  - `iCubeServerData://icube.cloudide`（权益/付费状态缓存：`entitlementInfo`、`serverTimeInfo`、`commercialActivityInfo`、`originPayStatusData`）
- CN 的值是 `tc` 密文（采集样本首 6 字节为 `0x64 0x63 0x05 0x10 0x00 0x00` 一类信封头），国际版可能是明文 JSON；本实现只支持后者。

### 2.2 刷新流程（ExchangeToken，轮换式）

**已用本机客户端源码（`D:\Trae CN\resources\app\out\main.js`）逐行确认**，不再依赖第三方实现：

```js
// exchangeTokenByRefreshToken()
url     = `${apiHost}/trae/api/v3/oauth/ExchangeToken`
headers = { "Content-Type": "application/json", "x-cloudide-token": <token 或空串> }
body    = { ClientID: "ono9krqynydwx5", ClientSecret: "",
            RefreshToken, UserID, IDEVersion,
            DeviceInfo: {...}, DeviceProof: {...} }

// pFe() —— 设备证明签名
timestamp = Math.floor(Date.now() / 1000)            // 秒
nonce     = crypto.randomBytes(16).toString("hex")   // 32 字符
canonical = [ "POST", path, ClientID, RefreshToken, String(timestamp), nonce ].join("\n")
signature = crypto.sign("sha256", Buffer.from(canonical), privateKeyPEM).toString("base64")
```

| 项 | 唯一正确形式 | 说明 |
| --- | --- | --- |
| 拼接符 | `"\n"` | hub / magpie 的「空格拼接」被客户端源码证伪 |
| 时间戳单位 | **unix 秒**（整数） | hub 的「毫秒」证伪；`Timestamp` 在 body 里是数字，不是字符串 |
| nonce | 16 字节随机数的 hex，共 **32 字符** | |
| 签名算法 | ECDSA-P256-SHA256，输出 **ASN.1 DER**，再 base64 | Node `crypto.sign(pem)` 默认 DER；不是 ieee-p1363 |
| 设备私钥 | `iCubeAuthInfo://icube-dc:<deviceId>` → `privateKeyPEM`（PKCS#8 或 SEC1） | |
| DeviceInfo.PlatformCode | `IDE_PC`（SOLO 为 `SOLO_PC`） | 不是 `Win` |
| DeviceInfo 字段名 | 全部 PascalCase | 小写会被当成「未提供证明」→ 20405 |
| ClientSecret | **空串 `""`** | 旧资料里的 `"-"` 属于另一条 `getJwtToken` 流程 |
| 请求头 `x-cloudide-token` | 刷新时为空串 | 带旧 token 会 20405 |

续期主机与路径（客户端当前形态）：

| 区域 | 首选 | 回退 |
| --- | --- | --- |
| CN | `https://api.trae.cn` + `/trae/api/v3/oauth/ExchangeToken` | `https://api.trae.com.cn` 同路径 → `https://api.trae.com.cn/cloudide/api/v3/trae/oauth/ExchangeToken` |
| INTL | `https://ug-normal.trae.ai` + `/trae/api/v3/oauth/ExchangeToken` | `https://ug-normal.traeapi.us` → `https://grow-normal.trae.ai` → 同机 `/cloudide/api/v3/trae/oauth/ExchangeToken` → `https://api-us-east.trae.ai/cloudide/...` |

> 标注「需实测校准」：
> 1. 国际版权威 `apiHost` 已由 CN 客户端 `product.json` 的 `bootConfig.account.trae.normal` 与国际站前端同款常量对齐为 `ug-normal.trae.ai`（详见 §2.5.5），刷新与登录共用；
> 2. 国际版是否也要求 DeviceProof：客户端代码对两个区域走同一函数，因此**推断需要**，但国际版服务端是否对旧版本放宽，未实测；
> 3. 国际版 `IDEVersion` 最低要求（社区反馈 CN 后端曾要求 ≥3.5.20），本实现默认 `3.6.2` 且允许凭证覆盖。
>
> 实现策略：按候选主机/路径依次尝试，只对 404/405 换下一个候选；签名原文会随实际路径重新计算，不存在“用旧路径的签名打新路径”的错配。

- **轮换**：每次刷新返回新的 `RefreshToken`，旧值立即失效 → 网关必须串行化刷新并原子落盘，否则账号作废。
- 响应字段：`Result.{Token, RefreshToken, TokenExpireAt(ms), RefreshExpireAt(ms), UserJwt, ClientID}`；错误在 `ResponseMetadata.Error.{Code,Message}`。
- **设备证明缺失时**：刷新直接返回 `ErrIntlMissingDeviceKey`（导入阶段拦截）/ `20405`（服务端），不会伪装成网络错误。

### 2.3 身份查询

```
POST {account_host}/cloudide/api/v3/trae/GetUserInfo
x-cloudide-token: <token>
{ "ReqSource": "IDE", "IDEVersion": "3.4.18" }   // SOLO 用 "Lite"
→ { "Result": { "UserId"/"UID", "UserName", "Email", "StoreRegion", ... } }
```

### 2.4 国际版从零到调通的凭证清单

| 字段 | 是否必需 | 从哪来 | 说明 |
| --- | --- | --- | --- |
| `refresh_token` | **必需**（长跑必备） | `storage.json` → `iCubeAuthInfo://icube.cloudide` 的 `refreshToken` | 轮换式，导入后每次刷新都会变 |
| `access_token` | 可选 | 同上的 `token` | 有它可立即调用；过期后靠 refresh 换新 |
| `uid` | 强烈建议 | 同上的 `userId` | 用于设备号派生、展示名与刷新结果校验 |
| `device_private_key` | 国际版刷新**必需** | `iCubeAuthInfo://icube-dc:<deviceId>` 的 `privateKeyPEM` | P-256；缺失则无法生成 DeviceProof（20405） |
| `device_public_key` | 建议 | 同上的 `publicKeyPEM` | 与服务端注册时的公钥一致 |
| `device_id` | 建议 | 键名后缀 `iCubeAuthInfo://icube-dc:<deviceId>` | 缺省时网关按 uid 稳定派生 |
| `machine_id` | 建议 | `telemetry.machineId` | 缺省时网关按 uid 稳定派生 |
| `os_version` / `device_brand` / `device_cpu` | 可选 | 本机真实信息 | 留空也能刷新（服务端不严格比对硬件字段） |
| `app_version` / `version_code` | 可选 | 客户端版本 | 默认 3.6.2 / 20260924，可覆盖 |

**获取路径（推荐顺序）**

1. **安装/登录国际版客户端一次（必须）**：安装 TRAE 国际版并用目标账号登录，让它完成设备注册（`iCube-dc` 设备密钥对就是登录时生成的）。
2. 关闭客户端后读取 `%APPDATA%\Trae\User\globalStorage\storage.json`。
3. 把整份 `storage.json` 直接粘贴到 Trae 渠道的凭证导入框（网关会自动识别 `iCubeAuthInfo://*` 键）；若该值是明文 JSON 即可直接导入。若显示 `ErrEncryptedStorage`，说明拿到的是 CN 的 `tc` 密文，需要换成国际版客户端的数据或先用社区工具导出明文。
4. 若只有 refreshToken、没有设备私钥，刷新必然失败；此时只能导入**短期 access_token** 做临时验证。

> 只在浏览器登录网页版、不安装客户端，**拿不到客户端已有的**设备私钥；但服务端可以在自己的登录流程里生成新的 P-256 设备密钥对，并在 `ExchangeToken` 时把公钥登记到账号上（见 §2.5）。因此浏览器登录不依赖本机客户端，也不依赖 `storage.json`。

### 2.5 网页登录（browser OAuth）实现说明

**结论：可行，已实现并接入 channel 的 `browser_oauth` 授权方式。**

#### 2.5.1 客户端证据（`D:\Trae CN\resources\app\out\main.js` + `product.json`）

| 环节 | 证据 | 结论 |
| --- | --- | --- |
| 授权页构造 | `loginUrlBuilder.js` 的 `Nhe.buildLoginUrl` | 授权页为 `{loginHost}/authorization?...`，`loginHost = consoleHost`（CN `https://www.trae.cn`，INTL `https://www.trae.ai`） |
| 回调地址 | `http://127.0.0.1:${port}/authorize` | 固定 path 为 `/authorize`，端口由本地 server 随机分配 |
| PKCE | `uFe()`：48 字节随机 → base64url verifier；`code_challenge = SHA256(verifier)` base64url | S256，服务端可完全复刻 |
| 回调参数 | `loginService` 把整段原始 query 交给 `loginCredentialCallback`；后者读取 `authCodeInfo`（JSON 字符串，内含 `AuthCode`） | **不是** 标准 `code` 参数，也**不保证**回传 `state` |
| 换 token | `exchangeTokenByAuthCode` → `POST {apiHost}/trae/api/v3/oauth/ExchangeToken`，body `{ClientID, AuthCode, CodeVerifier, DeviceInfo, IDEVersion}` | **不带 DeviceProof**；DeviceInfo 里放 `DevicePublicKey` |
| 设备密钥 | `hFe()` 生成 EC P-256 密钥对；公钥进 DeviceInfo | 设备密钥是客户端生成的，但登录协议只要求“提交一个公钥”，没有硬件证明 |

#### 2.5.2 本实现的流程

1. `BeginAuthorization`：生成 PKCE verifier、P-256 密钥对、19 位 deviceId、64 位 hex machineId、32 位 hex state；所有机密写入加密 Stage 载荷（`DriverState`）。
2. 授权 URL 完全对齐客户端参数：`login_version=1`、`auth_from=trae`、`login_channel=native_ide`、`plugin_version=<tronBuildVersion>`（CN 3.4.18 = `2.3.102387`，不是 `local`）、`auth_type=local`、`client_id=ono9krqynydwx5`、`redirect=0`、`login_trace_id=<v4 UUID>`、`machine_id`、`device_id`、`x_*` 指纹、`code_challenge(S256)`，`auth_callback_url` **必须**是裸地址 `http://127.0.0.1:<port>/authorize`。
3. loopback 回调：`CallbackQueryParser` 优先用渠道驱动的解析器读取 `authCodeInfo`；`state` 缺失时按“该渠道最近一个 pending 阶段”关联。授权码仍绑定 PKCE verifier，错误的码换不到 token。
4. `CompleteAuthorization`：`POST {account_host}/trae/api/v3/oauth/ExchangeToken`（无 DeviceProof），成功后调用 `GetUserInfo` 补全 uid/email，落成标准 Trae 凭证（含服务端生成的 `device_private_key`/`device_public_key`/`device_id`/`machine_id`）。
5. 后续刷新复用 §2.2 的 DeviceProof，签名私钥就是登录时生成的那把。

#### 2.5.3 回调端口与区域隔离

| 渠道 | 授权域 | loopback 回调 |
| --- | --- | --- |
| `trae` | `https://www.trae.cn` | `http://127.0.0.1:54546/authorize` |
| `trae_intl` | `https://www.trae.ai` | `http://127.0.0.1:54547/authorize` |

两个渠道共用一个 `CallbackQueryParser` 实现，但端口、授权域、账号域、交换主机完全独立。

#### 2.5.4 控制台侧的硬校验（已实测定位）

授权页组件（`static/js/async/authorization/page.*.js`）在加载时执行：

```js
const Z = decodeURIComponent(url.get("auth_callback_url") || "");
if (!$ || !Z || !/^http:\/\/127\.0\.0\.1:(\d+)\/authorize$/.test(Z)) {
  setPhase(3); // 登录失败 → 默认文案「网络错误，请刷新页面重试。」
  return;
}
```

- 回调地址**不能带任何 query**；带 `?state=...` 会被直接判为非法，这正是首版报错的根因。
- 回调页实际回传的参数是 `isRedirect`、`scope`、`authCodeInfo`、`loginTraceID`、`host`、`userRegion`、`userInfo`（`loginTraceID` 为驼峰），解析器已同时兼容。
- 页面只有 `client_id` 非空校验，不校验 `plugin_version` / `machine_id` / `device_id` 的格式；`plugin_version` 仅用于埋点，但仍按真实 `tronBuildVersion` 发送以保持一致。
- 点击「登录并使用 TRAE」时页面调用 `TraeGetPCAuthCode({ClientID, CodeChallenge, CodeChallengeMethod, DeviceID, PlatformCode:"IDE_PC"})`，再跳回回调，因此 PKCE 与设备 ID 必须由同一次登录会话提供。

#### 2.5.5 交换阶段的真实返回与取证

真实 `trae.4` 构建上的三次登录（`work\test-instance\instance.log`，11:21–11:22）只打出同一条记录：

```
endpoint=https://grow-normal.trae.ai/trae/api/v3/oauth/ExchangeToken status=400 code=10101
message="Invalid param: {__Message.field}."
body={"ResponseMetadata":{"Error":{"Code":"10101","Data":{},"Message":"Invalid param: {__Message.field}.",
      "StandardCode":"040004"},...}}
code_len=43 verifier_len=64 device_id_len=19 machine_id_len=64 public_key_len=177
```

请求确实发出去了（有 HTTP 记录，不是本地 state 校验失败）。随后用无效/空/缺失 AuthCode 做差分探测，结论如下：

| 探测 | 结果 |
| --- | --- |
| `ug-normal.trae.ai` × `/trae/api/v3/...`（43 位假码 / 空码 / 无 AuthCode 字段） | 全部 `400 10101`，body 逐字相同 |
| `grow-normal.trae.ai` × 同一组 body | 全部 `400 10101`，与 ug 主机完全相同 |
| 同主机同路径发**刷新** body（`RefreshToken` 无效） | `401 20101 Token invalid: refresh token is invalid.` |
| 去掉 `DeviceInfo` / 加 `ClientSecret:""` / 补齐客户端 `x-*` 头 | 仍为 `400 10101`，无变化 |
| `api.trae.ai` 的 V3 路径 | `404` nginx HTML（不是 API） |
| `api-us-east.trae.ai` | 只有 `/cloudide/...` 路径可用（V3 `404`） |

由此可得两条硬结论：

1. **`10101` 是网关级参数校验的兜底码，不区分“字段缺失 / 值非法 / 授权码无效”**，因为缺字段、空码、假码、补齐客户端全部字段都返回同一段 body；而同一主机上的刷新请求能进入业务分支（`20101`），说明我们的请求体与头本身是合法的。**它不能用来判断 AuthCode 是否被消费。**
2. 客户端对 Trae 的 `apiHost` 取自 `product.json` 的 `bootConfig.account.trae.*`，**不用回调里的 `host` 参数**（`loginCredentialCallback` 里写死走 `account.trae.normal/USTTP/SG`）。CN 的该值是 `https://api.trae.cn`；国际站的同款常量（`ug-normal.trae.ai`）也正是它 `/oauth/ExchangeToken` 的默认 base。`grow-normal.*` 是另一个服务（对应付费/权益域），`trae.4` 把授权码打到它上面属于**主机选错**。

修复（`trae.5`）：

- `IntlProfile.AccountURL` 改为 `https://ug-normal.trae.ai`，回退 `ug-normal.traeapi.us` → `grow-normal.trae.ai`（身份/续期接口一并受益）。
- 授权码交换候选按 `ug-normal V3 → ug-normal.traeapi.us V3 → grow-normal V3 → ug-normal/… cloudide → api-us-east cloudide` 顺序尝试，任一成功即停。
- 回调解析新增取证日志：`host`、`userRegion`、`userTag`、是否带 `userInfo`、`authCodeInfo` 长度；交换成功时记录最终 `endpoint`。
- 每次失败仍写 `realm/endpoint/status/code/message/脱敏 body`，因此下一轮可直接看出 `10101` 是否消失、最终落在哪台主机。

`3010` 测试实例已重启为 `v2.0.0-rc.63-trae.5`，日志同址落盘。

首轮「服务商拒绝了授权交换」的根因不在上游：修复前的回调地址带 `?state=`，授权页用 `` `${callbackURL}?${params}` `` 追加参数，实际得到 `state=abc?isRedirect=true`，state 校验失败被归类为“交换被拒绝”，**并未发出 ExchangeToken 请求**。现在改为无 query 回调，并优先用 `loginTraceID` 关联；即使旧 URL 再次出现也能正确取到 state。

#### 2.5.5.1 真实账号验证结果（2026-11-13，`trae.5`）

| 区域 | 回调 `host` / `userRegion` | 交换结果 |
| --- | --- | --- |
| INTL | `https://ug-normal.us.trae.ai` / `us` | 首台 `ug-normal.trae.ai` 返回 `10101`，第二台 **`ug-normal.traeapi.us/trae/api/v3/oauth/ExchangeToken` 成功**（`has_refresh_token=true`） |
| CN | `https://api.trae.com.cn` / `cn` | 首台 **`api.trae.cn/trae/api/v3/oauth/ExchangeToken` 成功** |

- 两条渠道各自落库 1 个凭证，`auth_state=ready`，模型目录（`/api/ide/v1/model_list`）可用：INTL 返回 8 个（快照），CN 返回 28/30 个实时条目。
- 兑换成功的候选顺序说明「`ug-normal.trae.ai` → `ug-normal.traeapi.us`」的二段回退是必要的：该账号的授权码只被 `.traeapi.us` 接受。
- 配额观测（`ide_user_ent_usage` / `pay_status`）同样跑通，两个测试账号均判定为 `free` 计划。

#### 2.5.6 待实测校准

1. 控制台是否把 `login_trace_id` 原样回传。实现同时支持 `loginTraceID`、`state`、`login_trace_id` 与“最近 pending 阶段”四种关联方式；即使全部不成立也能完成登录，只是失去按 state 的精确定位。
2. INTL 的 `apiHost` 已按客户端 `bootConfig.account.trae.normal`（`ug-normal.trae.ai`）修正，`grow-normal.*` 降为回退；真实账号已验证登录/刷新/目录/配额，**但两者都还没有一次成功的对话**。
3. 服务端自生成设备是否会被更严格的风控拒绝（当前协议只校验公钥与签名，**推断**接受；需真实账号回归一次刷新）。
4. **对话链路未验证**：两个测试账号都是 `free` 计划（`quota_windows` 的 `limit=0`、`credits.balance=0`），任意模型经 `/api/ide/v*/llm_raw_chat` 都被上游以 `400 / 4023 model_unknown`（“上游不支持或已下线该模型”）拒绝；CN 的 `model_list` 只返回账号自建的 BYOK 条目且 `status=false`。因此“真实凭证跑通一次对话”这一验收项**尚未达成**，需要在有额度的账号上复测。参考实现里 `llm_raw_chat` 的完整请求体还包含 `config_name`/`conversation_id`/`raw_chat_function`/`config_source`，本实现目前只发 `model_name`；这部分在未拿到可对话账号前**没有合入**，避免推送未验证改动。
5. 54000 段端口被占用时，登录会返回 `AUTHORIZATION_UNAVAILABLE`，需要手工释放端口。

---

## 3. 请求头契约（聊天/模型/额度共用）

| Header | CN | INTL | 说明 |
| --- | --- | --- | --- |
| `Authorization` | `Cloud-IDE-JWT <token>` | `Bearer <token>` | INTL 用 `Cloud-IDE-JWT` 会 4015/4023 |
| `x-ide-token` | `<token>` | `<token>` | 必填 |
| `X-Cloudide-Token` | `<token>` | `<token>` | 必填 |
| `X-App-Id` | `6eefa01c-1036-4c7e-9ca5-d891f63bfcd8` | 同 | 客户端常量 |
| `x-uid` | `<uid>` | `<uid>` | 账号维度 |
| `X-Device-Id` | 15~19 位数字（官方复用 userId） | 同 | **15 位以下/16 位部分接口会被拒** |
| `X-Machine-Id` | 64 位 hex | 64 位 hex | 与 deviceId 同源派生 |
| `X-Device-Type` | `windows` | `windows` | |
| `X-Device-Brand` / `X-Device-Cpu` / `X-Os-Version` | 非空 | 非空 | 空值会增大风控概率 |
| `X-Ide-Version` / `x-ide-version-code` | `3.4.3` / 纯数字 `20260212`（CN 客户端上报 20250325） | `3.6.2` / `20260924` | code 必须纯数字 |
| `X-Ide-Version-Type` | `stable` | `stable` | |
| `X-Request-Id` | 每次唯一 | 每次唯一 | 缺失会返回误导性 4011 |
| `Request-Traffic-Type` | `prod` | `prod` | |
| `User-Agent` | `Trae CN/3.4.3` | `TraeClient/TTNet` | **绝不能用 Go/curl 默认 UA**（会被判爬虫，返回 200 + 业务码 4011） |
| `store-country-code` | — | `us` | INTL 专用 |
| `Accept` | `text/event-stream` | 同 | |

---

## 4. 请求体：OpenAI → Trae

```
POST {core}/api/ide/{v2|v1}/llm_raw_chat
{
  "model_name": "<model>",
  "messages": [ { "role": "system|user|assistant",
                  "content": [ { "type": "text", "text": "..." } ] } ],
  "stream": true,
  "temperature"?: 0.2, "top_p"?: 0.9, "max_tokens"?: 1024,
  "stop"?: [...], "presence_penalty"?: 0, "frequency_penalty"?: 0,
  "session_id"?: "..."
}
```

要点（多实现一致）：

1. **`content` 必须是 parts 数组**；直接传字符串 → `400 / 4001`。
2. 模型字段名是 `model_name`（不是 `model`）。
3. 参数白名单只有 `temperature/top_p/max_tokens/stop/presence_penalty/frequency_penalty`；其他字段不要透传（4001 风险）。
4. 服务端**恒回 SSE**，`stream:false` 也一样；网关需要自己聚合。
5. 工具调用：`tools/tool_choice` 在部分实现中可用，但 CN 各账号套餐差异大；首版默认透传 `tools`，失败按请求错误处理，不做静默降级。
6. 会话：可传 `session_id` 便于上游缓存；网关按 credential + 会话键生成稳定值。

---

## 5. 响应：Trae SSE → OpenAI

上游帧形如 `event: <name>` + `data: <json>`：

| event | data 关键字段 | 网关映射 |
| --- | --- | --- |
| `metadata` | `conversation_id`、`message_id` 等 | 记录 id / 忽略 |
| `output` | `response`（正文增量）、`reasoning_content`（思维链）、`tool_calls`、`phase` | `choices[0].delta.content` / `delta.reasoning_content` / `delta.tool_calls` |
| `token_usage` | `prompt_tokens`、`completion_tokens`、`total_tokens`、`cached_tokens`、`reasoning_tokens`、`credits` | 合并到最终 chunk 的 `usage` |
| `done` | `finish_reason` | `choices[0].finish_reason` |
| `error` | `code`、`message` | 归类为上游错误（见 §6） |

其他：`extra_info`（完整思维链快照）必须丢弃；`phase` 仅用于思考面板状态，不进入正文；`queue_*` 类事件不产生 content。

---

## 6. 错误码与分类

| 码 | 含义 | 分类（gpt-load） |
| --- | --- | --- |
| `1001` / `20101` | 鉴权失败 / 未登录 | 凭证失效 → 触发刷新；刷新失败则标记账号需重新授权 |
| `4001` | 参数非法（content 非 parts / model 名错） | 请求错误，不换号重试 |
| `4015` | 上游拒绝（模型权限 / 国际版风控） | 非请求错误、不惩罚账号：按 403 返回并冷却，不重放 |
| `4019` | 风控命中（设备头 / 出口区域） | 账号维度冷却，禁止换号风暴式重试 |
| `4011` | 伪装成「超出限流」的爬虫识别（多为 UA / 缺必填头） | 尽量在网关侧消除；命中则按主机错误处理，不扣账号 |
| `4023` | 模型未知 | 模型维度不可用 |
| `3003` | prompt pipeline 失败（仅 `/chat`） | 请求错误 |
| `20403` / `20405` | 设备不匹配 / 缺设备证明（INTL） | 凭证维度需修复（导入正确设备信息） |
| `403 no schema found` | INTL `model_list` 不可用 | 模型发现降级到内置快照，不算错误 |
| HTTP 401 / 429 / 5xx | 标准语义 | 401→刷新凭证；429→限流；5xx→主机错误 |

红线（沿用本项目既有约定）：

- **`11102` 类「无此模型」不得标记账号不可用**（Trae 对应 `4023`）。
- **`4019` 风控不得换号重试**，应冷却并返回明确文案。

---

## 7. gpt-load 代码落点

新增 `internal/subscription/providers/trae/`（共享实现，按 realm 参数化）：

| 文件 | 职责 |
| --- | --- |
| `trae.go` | 包说明、Realm 常量表、两个 Driver（`trae` / `trae-intl`）、`Implementations()` |
| `credential.go` | `Credential` 结构、JSON 导入解析、脱敏值、过期判定 |
| `auth.go` | ExchangeToken 刷新（含 INTL DeviceProof）、GetUserInfo、TokenEndpointError |
| `models.go` | `model_list` 动态发现 + 各 realm 内置快照兜底 |
| `quota.go` | `ide_user_ent_usage` / `ide_user_pay_status` → `observation.Snapshot` |
| `executor.go` | OpenAI 请求 → `llm_raw_chat`，SSE 翻译为 OpenAI chunk / 非流式聚合，错误分类 |
| `trae_test.go` | 凭证解析、请求体归一化、SSE 翻译、错误分类单测 |

注册点（与 WorkBuddy 完全对齐）：

1. `internal/channel/spec/definition.go`：`Trae` / `TraeIntl` 渠道 ID、`ProviderTrae` / `ProviderTraeIntl`，加入 `allProviders`
2. `internal/channel/channel.go`：别名常量
3. `internal/channel/modules/trae.go`、`trae_intl.go`：模块定义（订阅型、OAuth 文件导入、路由）
4. `internal/channel/builtins.go`：注册模块
5. `internal/container/container.go`：provider → adapter 绑定
6. `internal/execution/cpa/adapter.go` + `trae_provider.go`：CPA 桥接
7. `internal/subscription/providers/providers.go`：注册 `trae.Implementations()`
8. `internal/channel/registry_test.go`、`final_provider_contract_test.go`、`internal/subscription/providers/providers_test.go`：断言列表更新
9. `webui` i18n / 图标 / 渠道图标映射：新增 `trae` 词条

---

## 8. 需要用户拍板的问题

1. **国际版是否现在就要可跑？**
   INTL 需要设备私钥（`iCubeAuthInfo://icube-dc:*`）做 DeviceProof，且 model_list 不可用、出口 IP 必须与账号区域一致。建议：先交付「代码齐全 + 内置模型快照」，INTL 凭证导入要求用户提供 refreshToken + 设备信息，实测可用性由用户验证。

2. **凭证导入方式**
   首版实现「粘贴 JSON（refreshToken + 可选 deviceId/machineId/device 信息）」。是否需要额外实现 `storage.json`（`tc` 封装）自动解密导入？该解密依赖未公开的 pepper 常量，属于逆向增强，工作量与稳定性风险都更高。

3. **是否要 Anthropic / Gemini 转换路由**
   Trae 上游只有自己的 SSE 形态，网关侧已统一转 OpenAI chunk。若需要 `/v1/messages`（Anthropic）与 `/v1beta`（Gemini）客户端协议，需要额外写转换层，建议后续单独排期。

4. **模型命名策略**
   CN 的 `model_name` 必须是 `model_list` 里的内部名（如 `glm-5.2`、`Doubao-*`），与客户端展示名不一致。是否需要维护「展示名 → 内部名」别名表（工作量大且随上游变动）？首版建议：直接暴露上游内部名，由用户在客户端选择。

5. **额度展示口径**
   `ide_user_ent_usage` 返回 `user_entitlement_pack_list[]`（`quota/usage/display_desc`），是「权益包」不是单一余额。是否按权益包逐条展示（与 WorkBuddy 一致），还是只展示总量？

---

## 9. 参考仓库（均已读源码）

| 仓库 | 语言 | 最后更新 | 区域 | 采用点 |
| --- | --- | --- | --- | --- |
| `shuishuipingan/trae2api-hub` | Python | 2026-11-11 | CN+INTL | 最完整的双区常量表、SSE 事件模型、刷新轮换 |
| `autumnsentiment/Trae2api-cn` | Python | 2026-11-05 | CN | `llm_raw_chat` v2 端点、风控头清单 |
| `connectedGraph/trae2api-web` | Go | 2026-10-11 | CN SOLO | 常量/请求头最小实现（Go 参考） |
| `misakano7545/traework2api-panel` | Go | 2026-11-10 | 双区 | 域名调度、UA 风控证据 |
| `XCool-603/trae2api` | Go | 2026-11-09 | 双区 | ClientID 归属规则、签名原文 |
| `bluechonk/trae-credential-reverse-engineering` | TS | 2026-10-23 | 双区 | 本地凭证封装结构、鉴权头分族、错误码 |
| `A-23187/trae-api` | Python | 2026-06-22 | INTL | 浏览器脚本取 refreshToken 链路（旧） |
| `magpie-community/plugins` | JS | 活跃 | 双区 | 端点地图、登录 URL 参数 |
| `ZedeX/trae-local-api`、`laojichao/trae-local-api` | JS | 2026-07 | INTL/CN | 本地 storage 解密思路 |
| `arch3rPro/Trae-Proxy`、`zhiquanchi/Trae-proxy-go`、`mtfly/trae-switch` | — | — | — | **方向相反**（让 Trae 调别人的 API），仅用于排除 |

---

## 10. 已实现范围（v1）

分支 `feat/trae-channel`，独立 worktree `2026-11-12\trae-channel\work\gpt-load-trae`，基于 `workbuddy` HEAD 拉出，未合并、未推送。

### 10.1 代码清单

```
internal/subscription/providers/trae/
  trae.go        # 两个 Driver + Profile（cn/intl）常量表
  credential.go  # 凭证解析/脱敏/身份派生
  auth.go        # ExchangeToken 刷新、GetUserInfo、DeviceProof(P-256)
  models.go      # model_list 动态发现 + 内置快照兜底
  quota.go       # 权益包 / 付费状态 → observation.Snapshot
  executor.go    # OpenAI 请求归一 + SSE → OpenAI chunk 翻译 + 错误分类
  trae_test.go   # 7 组单测
internal/channel/modules/trae.go      # trae / trae_intl 两个渠道定义
internal/execution/cpa/trae_provider.go  # CPA 桥接（同一份代码按 profile 实例化两次）
```

注册点已全部接齐：`spec/definition.go`、`channel.go`、`builtins.go`、`container.go`、`cpa/adapter.go`、`providers/providers.go`，以及 `registry_test.go`、`final_provider_contract_test.go`、`route_golden_test.go`、`provideradapter/registry_test.go`、`providers/providers_test.go` 的断言清单。

### 10.2 凭证导入格式

渠道为 `subscription` + `oauth_file` 导入，粘贴 JSON（字段可省略，`refresh_token` 与 `access_token` 至少一个）：

```json
{
  "realm": "cn",
  "access_token": "<可选>",
  "refresh_token": "<必填/推荐>",
  "uid": "<账号 uid>",
  "base_url": "<可选，覆盖核心网关>",
  "device_id": "<可选，19 位数字>",
  "machine_id": "<可选，64 位 hex>",
  "device_private_key": "<仅国际版刷新需要，P-256 PEM>",
  "device_public_key": "<可选>",
  "app_version": "3.4.18",
  "version_code": "20250325"
}
```

说明：

- 未提供 `uid` 时用刷新令牌哈希派生稳定身份（`rt-` 前缀），保证刷新结果能对上号。
- 未提供设备指纹时由 uid 稳定派生（`X-Device-Id` 19 位数字、`X-Machine-Id` 64 位 hex），同一账号重导入不会漂移。
- 刷新令牌是轮换式的：网关每次刷新后立即写回加密存储，旧值随之作废。

### 10.3 运行行为

| 场景 | 行为 |
| --- | --- |
| 动态模型目录失败 | 回落内置快照，不把网络抖动当账号异常；国际版始终走快照 |
| 4019 风控 | 账号级冷却 10 分钟，返回明确文案，**禁止换号重试** |
| 4015 / 4011 / 403 | 不惩罚账号，按主机错误处理 |
| 4023 模型未知 | 仅标记模型不可用，不标记账号不可用 |
| 1001 / 401 | 触发凭证刷新；刷新失败才要求重新授权 |
| reasoning 逐词下发 | 网关侧合并到 ≥24 字符再转发，避免思考面板一词一行 |

### 10.4 验证方式

```powershell
# 单测
& "$env:TEMP\goroot-1.27.3\go\bin\go.exe" test ./internal/channel/... ./internal/subscription/... ./internal/execution/cpa/... ./internal/provideradapter/...
# 前端（必须先构建，否则 exe 只会嵌入占位页，界面提示“前端资源尚未构建”）
corepack pnpm --dir web install --frozen-lockfile
corepack pnpm --dir web exec vite build      # 输出到 internal/webui/dist
# 构建（产物留在 worktree，不覆盖现役 exe）
& "$env:TEMP\goroot-1.27.3\go\bin\go.exe" build `
  -ldflags "-X gpt-load/internal/platform/version.Version=v2.0.0-rc.63-trae.4" `
  -o gpt-load-trae.exe .
```

结果：上述相关包全部通过；仓库原有的 `internal/container` 环境代理测试、`internal/catalog` 快照摘要、`internal/gateway` SSE 夹具、`internal/platform/authkey` 日志路径测试在基线 `workbuddy` 分支上同样失败，与本次改动无关。

冒烟验证（`3010` 实例，`v2.0.0-rc.63-trae.4`）：

- `GET /api/channels`：
  ```
  trae      | Trae        | subscription | browser_oauth,oauth_file | openai-completions: chat_completion/probe (native)
  trae_intl | Trae (Intl) | subscription | browser_oauth,oauth_file | openai-completions: chat_completion/probe (native)
  ```
- `POST /api/credential-stages/authorizations {"channel_id":"trae"}` → `pending_authorization`，`redirect_uri=http://127.0.0.1:54546/authorize`，授权页 `https://www.trae.cn/authorization?...`（`client_id` / `auth_from` / `auth_callback_url` / `code_challenge_method=S256` 均已校验）。
- `POST ... {"channel_id":"trae_intl"}` → `http://127.0.0.1:54547/authorize`，授权页 `https://www.trae.ai/authorization?...`。
- 回调链路：`GET http://127.0.0.1:54546/authorize` → “无法识别这次授权”；`?error_code=access_denied`（无 state）→ “授权未完成”，对应 Stage 变成 `failed / authorization_denied`，证明无 state 的渠道关联路径生效。
- 授权 URL 自检（`v2.0.0-rc.63-trae.3`）：`auth_callback_url=http://127.0.0.1:54546/authorize`（通过控制台正则）、`plugin_version=2.3.102387`、`login_trace_id` 为 v4 UUID、`client_id=ono9krqynydwx5`、`x_app_type=stable`、`x_app_version=3.4.18`。
- 真实账号登录与登录后首次刷新仍需用户配合回归（会走真实 `ExchangeToken`）。

聊天请求链路只做过本地 SSE 夹具单测（事件解析、chunk 翻译、usage 合并、reasoning 合并），尚未用真实 Trae 凭证打上游。

隔离测试实例（与现役 3001 完全分离）：

| 项 | 值 |
| --- | --- |
| 访问地址 | `http://127.0.0.1:3010` |
| 版本 | `v2.0.0-rc.63-trae.4`（`/api/system/info` 可查） |
| 运行日志 | `2026-11-12\trae-channel\work\test-instance\instance.log` |
| 进程 | worktree 内 `gpt-load-trae.exe` |
| 数据目录 | `2026-11-12\trae-channel\work\test-instance\data` |
| 环境变量 | `HOST=127.0.0.1 PORT=3010 DATA_DIR=<上表> AUTH_KEY=<见对话>` |
| 影响范围 | 不读写 `d:\Projects\APIprovider\gpt-load-deploy\data`，不占用 3001 |

重启命令（在 worktree 目录执行）：

```powershell
$env:HOST="127.0.0.1"; $env:PORT="3010"
$env:DATA_DIR="D:\Projects\APIprovider\gpt-load-deploy\2026-11-12\trae-channel\work\test-instance\data"
$env:AUTH_KEY="<导入界面用的登录密钥>"
& "D:\Projects\APIprovider\gpt-load-deploy\2026-11-12\trae-channel\work\gpt-load-trae\gpt-load-trae.exe"
```

### 10.5 尚未覆盖（需用户拍板）

1. 国际版的权威 `apiHost`、最低 `IDEVersion`、以及服务端是否对 DeviceProof 放宽，均**需实测校准**（代码按候选顺序尝试并逐项标注，不假装确定）。
2. CN `storage.json`（`tc` 加密封装）自动解密导入未实现。
3. Anthropic / Gemini 客户端协议转换路由未实现（当前只有 OpenAI Completions 原生路由）。
4. 展示名 → 内部 `model_name` 的别名表未实现（首版直接暴露上游内部名）。
5. 网页登录的“授权页是否回传 state / 服务端自生成设备能否长期刷新”需要一次真实账号回归；失败时的表现已分别落到 `authorization_denied`、`authorization_exchange_failed` 与 `20405 → 重新授权`，不会静默失败。

## 11. 2026-11-13 实测补充（4001 / 1005 / tools / UA / UID）

- **模型标识**：`llm_raw_chat` 只认 `batch_get_detail_param`（`chat_v3` 组）里的 `config_name` + `model_detail_list[].model_name`。用 `model_list` 的展示名（如 `seed_m8`、`DeepSeek-V4-Flash`）会得到 4001/4023。配置名键必须映射到真实 `model_name`（如 `xxx__dev`），直接发配置名会被判 4023。
- **展示名冲突**：`DeepSeek-V4-Flash` 与 `DeepSeek-V4-Flash-Official` 共用同一展示名。精确键必须优先于展示名别名，否则会命中套餐不可用的 `-Official` 配置并返回 1005。
- **1005**：HTTP 200 + `{"code":1005,"extra":"{\"plan\":3}"}`，表示当前账号套餐不支持该模型配置；网关映射 403，文案保留 code 与 extra 原文。
- **tools**：`llm_raw_chat` 接受原生 `tools`，但 `function.parameters` 必须是 JSON **字符串**（传对象得到 HTTP 400 空响应）；对象形态的 `tool_choice` 需降级为函数名字符串。上游下发的调用形态是 `{"function_call":{"name","arguments"}}`，需翻译成 OpenAI 的 `function` 形态；assistant 历史里的 `tool_calls` 回放时也要反向翻译。
- **会话 id**：`conversation_id` / `session_id` 必须是 UUID。
- **UID**：真实值取 JWT payload 的 `data.id`；凭据里 `rt-` 前缀是占位值。请求头 `x-uid` 要用小写原始键（Go 的 `Header.Set` 会规范成 `X-Uid`）。
- **INTL UA**：用 `TraeClient/TTNet` 会被上游按另一套模型注册表处理并返回 4023（model unknown）；改 `Trae/3.6.2` 才能拿到真实业务码。
- **套餐**：`user_pay_identity_str`（INTL=`Free`，CN=`Lite`）。到期时间只取权益包 `entitlement_base_info.end_time`，对应配额里的 pack 窗口 `reset_at_ms`。
- **INTL 边界**：当前账号 Free、额度 0，任意模型均返回 `4015`（无权限 / 需要与账号区域一致的出口），无法完成对话验证；网关原样透出该 code 与文案。
- **可暴露模型**：只暴露 `config_source=1`、非 `is_custom_model`、且 `model_capability` 非空的条目（本账号 22 个）。`source=3` 的账号自建或模板条目经 `llm_raw_chat` 一律 4001（原文 `the param is invalid`），`custom_model_*` 是占位模板（2001 `invalid prompt`），`summary`/`title_generation`/`fast_apply` 等是无 capability 的内部工具，均不应出现在对话模型列表里。
- **自建模型（config_source=3）**：仍走 `/api/ide/v2/llm_raw_chat`，但必须同时带 `is_custom_model:true`、`provider`（取自 `model_detail_list[].provider`，如 `aliyuncs`）、`custom_model_id`（取自 `custom_config.custom_model_id`）。缺任意一个都是 4001；带齐后上游代理到模型服务商。`custom_config` 原始结构：`{ak(加密), auth_type, base_url, client_connect, custom_model_id, is_custom_model_id, status, thinking_enable}`，其中 `custom_model_id` 是唯一定位键。本账号 `deepseek-v4.1-flash`、`qwen3.8-flash` 带齐后均返回 4028（Aliyun `invalid_api_key`，HTTP 401），说明是账号侧服务商密钥失效，需在 Trae 客户端重新填写。
- **实测分类**：`1005`=当前套餐需更高 plan（`extra={"plan":3}`）；`4015`=账号无权/区域不一致；`4023`=模型不存在（`the model is unknown`）；`4001`=参数或模型标识不被接受（`the param is invalid`）。
- **关 key 后的自建条目**：在 Trae 客户端关闭自建模型的 key 不会让条目从 `batch_get_detail_param` / `model_list` 消失，只会把 `config_switch` 与 `model_detail_list[].custom_config.status` 置为 `false`（本账号 24 条自建配置全部如此，且 `provider`/`custom_model_id` 等字段仍在）。这类条目不应发布到分组模型列表：它仍能通过参数校验，但请求会被上游代理到模型服务商并由服务商密钥决定成败，实测返回 `4028`（Aliyun `invalid_api_key`，HTTP 401）；不带 `is_custom_model/provider/custom_model_id` 则直接 `4001`。正确做法是重新在客户端启用并填入有效密钥后重拉目录再同步，无需改代码。
- **自填回环 `base_url`（已知风险）**：自建条目的 `base_url` 由用户自填。`deepseek-v4.1-flash-sg` 实测为 `base_url=http://127.0.0.1:3001/v1/chat/completions`、`client_connect=true`，等于让 Trae 服务端回调本机网关（自环/递归转发），且把本机网关的可用性暴露给上游。此类条目不应发布；同步分组时应把 `client_connect=true` 且 `base_url` 指向回环/内网地址的条目单独识别，不能只按 `config_switch` 判断。


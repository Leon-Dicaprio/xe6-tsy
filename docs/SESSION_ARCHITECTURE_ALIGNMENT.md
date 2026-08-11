# Session 模块架构确认汇报

> 汇报目标：只对齐 Session 模块的架构拆分、接口设计、依赖关系和一致性方案，不重复讲产品功能。
>
> 建议展示方式：在 GitHub 中打开本文，按“现场展示顺序”逐项点击代码链接。链接已定位到对应代码行。

## 1. 先给结论

Session 模块是 `services/api` 控制面的业务会话模块，负责 `Session` 的创建、启动、结束、查询、鉴权边界、幂等和持久化一致性；它不处理音频、不维护 WebRTC 连接，也不实现 ASR / 翻译 / TTS。

这里的 **Session 是从创建、启动、持续运行到最终结束的一整场实时语音沟通过程，不是一轮翻译**。一场 Session 中可以产生很多个 Turn；一个 Turn 才代表一次发言及其翻译结果。Session 只提供这场会话的 ID 和生命周期上下文，不保存每个 Turn 的原文与译文。

```text
一个 Session
┌─────────────────────────────────────────────┐
│ Turn 1：用户 A 发言 → 识别 → 翻译            │
│ Turn 2：用户 B 发言 → 识别 → 翻译            │
│ Turn 3：用户 A 发言 → 识别 → 翻译            │
│ ……                                          │
└─────────────────────────────────────────────┘
created                  active             ended
```

| 概念 | 粒度 | 负责模块 | 代码证据 |
| --- | --- | --- | --- |
| Session | 一整场语音沟通 | `services/api/sessions` | [VoiceSession 业务实体](../services/api/sessions/model.go#L121) |
| Turn | 会话中的一次发言和翻译结果 | Realtime 产生，Records 持久化 | [VoiceTurn 契约](../packages/contracts/records/v1/records.go#L91)、[FinalTurnEvent 契约](../packages/contracts/records/v1/records.go#L160) |

建议开场直接说：

> 我负责的是 API Service 内部的 Session 模块。这里的 Session 不是一轮翻译，而是从创建、启动、运行到结束的一整场实时语音翻译会话。一场 Session 里面可以包含很多轮发言和翻译，但每轮翻译内容不由 Session 模块保存。

最关键的架构决策有四个：

1. **三类状态分开管理**：Session 只持久化业务状态；运行状态归 Realtime；连接状态归 WebRTC。
2. **调用方定义接口**：Session 在自己的 `ports.go` 中定义最小依赖接口，再由 Adapter 显式转换其他模块的模型和错误。
3. **数据库是跨实例一致性的权威**：进程内锁只减少同一实例的重复工作；StartOperation、EndIntent 和数据库事务才负责幂等、并发互斥与恢复。
4. **先确认媒体资源，再提交业务终态**：Start 必须确认当前 Operation 拥有运行中的 Runtime 才写 `active`；End 必须确认 Runtime 已 `stopped` 才写 `ended`。

代码入口：

- [业务状态、运行状态、连接状态的分离模型](../services/api/sessions/model.go#L10)
- [Session 对外依赖 Ports](../services/api/sessions/ports.go#L184)
- [Service 的依赖注入与构造约束](../services/api/sessions/service.go#L20)
- [生产环境依赖装配](../services/api/session_runtime.go#L61)

## 2. 现场展示顺序（建议 8～10 分钟）

| 时间 | 展示内容 | 直接打开的代码 | 要讲的结论 |
| --- | --- | --- | --- |
| 1 分钟 | Session 与 Turn | [Session 模型](../services/api/sessions/model.go#L121) / [Turn 模型](../packages/contracts/records/v1/records.go#L91) | Session 是整场会话；Turn 才是一轮发言和翻译 |
| 1 分钟 | 模块边界 | [三类状态定义](../services/api/sessions/model.go#L10) | Session 只拥有业务生命周期，不复制媒体面状态机 |
| 1 分钟 | 对外 HTTP 契约 | [OpenAPI 路由](../packages/contracts/openapi.yaml#L59) / [路由注册](../services/api/sessions/http.go#L69) | 契约先行，Handler 只做协议转换 |
| 1 分钟 | 模块接口 | [Repository 与 RealtimeLifecycle](../services/api/sessions/ports.go#L184) | Session 通过最小 Port 依赖其他模块 |
| 2 分钟 | Start 主链路 | [Start 编排入口](../services/api/sessions/service_start.go#L10) / [Runtime 确认](../services/api/sessions/service_start_runtime.go#L9) | 用持久化 Operation 处理幂等、跨实例并发与补偿 |
| 1.5 分钟 | End 主链路 | [End 编排入口](../services/api/sessions/service_end.go#L10) / [后台恢复](../services/api/sessions/end_recovery.go#L73) | 先保存 EndIntent；Stop 未确认时绝不提前写 `ended` |
| 1 分钟 | 数据一致性 | [核心表结构](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L63) | Session、StartOperation、EndIntent 分别承担不同职责 |
| 1 分钟 | 模块适配与装配 | [显式 Adapter](../services/api/realtimeaccess/adapters.go#L20) / [生产装配](../services/api/session_runtime.go#L61) | 不让 provider 类型和错误泄漏进业务层 |
| 1 分钟 | 测试证据 | [生产控制面集成测试](../services/api/main_integration_test.go#L199) | 代码已覆盖 Create → Readiness → Start → Query → End 与幂等重放 |

如果现场时间不足，只展示 `model.go`、`ports.go`、`service_start.go`、`service_end.go` 和一条集成测试。

## 3. 模块拆分与边界

```text
Client
  │  REST + Bearer Token + Idempotency-Key
  ▼
sessions.Handler                 HTTP 协议、认证上下文、请求哈希、错误映射
  ▼
sessions.Service                 业务编排、状态规则、补偿与恢复
  ├── Repository                PostgreSQL：Session + 操作一致性
  ├── LanguageConfigReader      languages 模块适配器
  ├── WebRTCConnectionReader    realtime control-plane 适配器
  └── RealtimeLifecycle         realtime Start / Stop / State 适配器
```

Session 当前请求和运行链路直接协作四类边界：

| 边界 | 提供什么 | 代码落点 |
| --- | --- | --- |
| Auth | 认证中间件写入的可信 `AccountID` | [HTTP 只接受中间件身份](../services/api/sessions/http.go#L20)、[requireAccount](../services/api/sessions/http.go#L314) |
| Language | 当前双向语言配置是否 ready | [LanguageConfigReader](../services/api/sessions/ports.go#L267)、[Start readiness 校验](../services/api/sessions/service_start.go#L163) |
| PostgreSQL | 业务状态、Create 请求、StartOperation、EndIntent 和恢复租约 | [PostgresRepository](../services/api/sessions/postgres.go#L27) |
| Realtime Service | WebRTC 连接状态、Runtime Start / Stop / State | [Realtime Ports](../services/api/sessions/ports.go#L232)、[Realtime Adapter](../services/api/realtimeaccess/adapters.go#L90) |

**Records、Usage、Delivery 不是 Session 当前的直接依赖。** 它们通过 `SessionID` 与整场会话关联：Records 保存属于该 Session 的 Turn，Usage 按 Session 聚合用量，Delivery 读取已保存的 Turn 生成消息。代码可见 [VoiceTurn.SessionID](../packages/contracts/records/v1/records.go#L91)、[Usage.SessionID](../services/api/internal/usage/model.go#L20) 和 [Delivery FinalTurnSnapshot.SessionID](../services/api/internal/delivery/model.go#L68)。

### 3.1 Session 拥有什么

| 内容 | 权威位置 | 代码证据 |
| --- | --- | --- |
| 业务状态 `created / active / ended / failed` | `services/api/sessions` + PostgreSQL | [Status 定义](../services/api/sessions/model.go#L10)、[voice_sessions 约束](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L63) |
| Create / Start / End 幂等 | Session Repository | [Repository 接口](../services/api/sessions/ports.go#L184) |
| 会话归属与账户范围读取 | Session Repository | [actor 经 lineage 授权、保留 owner](../services/api/sessions/postgres.go#L333) |
| Detail / State 的聚合结果 | Session Service | [查询聚合](../services/api/sessions/service_query.go#L19) |
| Runtime 清理后业务失败落库 | Session Service | [RuntimeFailureConsumer](../services/api/sessions/service_failure.go#L8) |

### 3.2 Session 明确不拥有什么

| 内容 | 实际 Owner | Session 如何使用 |
| --- | --- | --- |
| `listening / translating / playing` 等运行态 | `services/realtime-audio` | 通过 [RealtimeLifecycle](../services/api/sessions/ports.go#L232) 查询或控制 |
| WebRTC 连接状态与 PeerConnection | Realtime WebRTC connection manager | Start 前通过 [WebRTCConnectionReader](../services/api/sessions/ports.go#L287) 检查 `connected` |
| 双语配置及版本 | `services/api/languages` | Start 前通过 [LanguageConfigReader](../services/api/sessions/ports.go#L267) 检查 active 双向配置 |
| SDP / ICE / 音频流 / VAD / ASR / 翻译 / TTS | `services/realtime-audio` | Session 不直接依赖这些包或 provider |

这里最值得现场强调的是 [`VoiceSession`](../services/api/sessions/model.go#L121) 不包含 `runtime_state` 或 `connection_state`。只有用于查询聚合的 [`VoiceSessionDetail`](../services/api/sessions/model.go#L200) 才临时组合持久化业务状态和实时快照。

### 3.3 Session 进入 active 后还做什么

不要用“中间交给核心模块干活”概括。更准确的说法是：

> Session 进入 `active` 后，实时音频主链路由 Realtime Service 负责，包括 WebRTC、VAD、ASR、翻译、TTS 和播放。Session 不参与每一帧音频处理，也不保存每一轮翻译内容；它继续维护整场会话的业务生命周期，并在详情或状态查询时组合 PostgreSQL 业务状态和 Realtime 运行状态。

必须区分三类状态：

| 状态类型 | 示例 | Owner | Session 中的代码 |
| --- | --- | --- | --- |
| Session business status | `created / active / ended / failed` | Session + PostgreSQL | [Status](../services/api/sessions/model.go#L10) |
| Runtime state | `listening / translating / playing / stopping` | Realtime Service | [RuntimeState 投影](../services/api/sessions/model.go#L43) |
| Connection state | `connected / disconnected / failed` | Realtime 的 WebRTC 模块 | [ConnectionState 投影](../services/api/sessions/model.go#L58) |

Session 查询不复制另外两套状态机：Detail / State 只在读取时组合一次 Runtime 快照，List 则完全不查询 Realtime，见 [查询实现](../services/api/sessions/service_query.go#L19)。

## 4. 接口设计

### 4.1 REST API

HTTP 真源是 [`packages/contracts/openapi.yaml`](../packages/contracts/openapi.yaml#L59)，具体挂载由 [`Handler.Register`](../services/api/sessions/http.go#L69) 完成。

| 方法 | 路径 | 作用 | 实现入口 |
| --- | --- | --- | --- |
| `POST` | `/api/v1/voice-sessions` | 创建业务会话 | [Handler.create](../services/api/sessions/http.go#L103) → [Service.Create](../services/api/sessions/service_create.go#L10) |
| `POST` | `/api/v1/voice-sessions/{id}/start` | 启动媒体运行时并提交 `active` | [Handler.start](../services/api/sessions/http.go#L136) → [Service.Start](../services/api/sessions/service_start.go#L13) |
| `POST` | `/api/v1/voice-sessions/{id}/end` | 清理媒体资源并提交 `ended` | [Handler.end](../services/api/sessions/http.go#L172) → [Service.End](../services/api/sessions/service_end.go#L12) |
| `GET` | `/api/v1/voice-sessions/{id}` | 业务实体 + 一次 Runtime 快照 | [Handler.detail](../services/api/sessions/http.go#L211) → [Service.GetDetail](../services/api/sessions/service_query.go#L19) |
| `GET` | `/api/v1/voice-sessions/{id}/state` | 高频轮询的精简状态 | [Handler.state](../services/api/sessions/http.go#L233) → [Service.GetState](../services/api/sessions/service_query.go#L37) |
| `GET` | `/api/v1/voice-sessions` | 仅持久化数据的游标分页 | [Handler.list](../services/api/sessions/http.go#L280) → [Service.List](../services/api/sessions/service_query.go#L56) |
| `POST` | `/api/v1/voice-sessions/{id}/realtime-ticket` | 为会话 Owner 签发短期 Realtime Ticket | [Handler.mintRealtimeTicket](../services/api/sessions/http.go#L256) |

接口层的三个设计点：

- `account_id` 只从可信认证中间件上下文读取，客户端 Body/Header 不是授权依据，见 [Handler 构造约束](../services/api/sessions/http.go#L51)。
- Create / Start / End 使用 `Idempotency-Key`，Handler 对规范化请求生成 SHA-256 指纹，见 [canonicalHash](../services/api/sessions/http.go#L390)。
- 业务错误统一映射成稳定 HTTP 状态和错误码，见 [statusCodeForError](../services/api/sessions/http.go#L437)。

### 4.2 Service Port

Session 采用“consumer-owned interface”：接口定义在使用方 `sessions` 包内，其他模块通过 Adapter 满足它，而不是让 Session 直接依赖对方实现。

| Port | 最小职责 | 为什么这样拆 |
| --- | --- | --- |
| [`Repository`](../services/api/sessions/ports.go#L184) | Session 持久化、幂等、原子状态迁移、恢复租约 | 数据库事务是跨实例一致性的权威 |
| [`RealtimeLifecycle`](../services/api/sessions/ports.go#L232) | `Start`、`Stop`、`GetRuntimeState` | 不暴露 Realtime 内部 Pipeline / WebRTC 实现 |
| [`LanguageConfigReader`](../services/api/sessions/ports.go#L267) | 读取当前配置的最小 readiness 投影 | Session 只关心“是否可启动”，不管理配置版本历史 |
| [`WebRTCConnectionReader`](../services/api/sessions/ports.go#L287) | 读取连接 readiness | 连接态不能代替运行态 |
| [`SessionReader`](../services/api/sessions/ports.go#L292) | 向可信内部模块提供不可变 Session 快照 | 与外部账户授权读取明确分离 |
| [`RuntimeFailureConsumer`](../services/api/sessions/ports.go#L297) | 接收“资源已清理”的不可恢复失败 | 只有清理确认后才允许 `active -> failed` |

Adapter 会显式映射状态、字段和错误。例如 Realtime 的 `RuntimeSnapshot.StartOperationID` 被明确映射到 Session 的消费者模型，provider 的 `runtime not found` 被转换为 Session 内部哨兵错误，见 [mapRuntimeSnapshot](../services/api/realtimeaccess/adapters.go#L266) 和 [mapRuntimeError](../services/api/realtimeaccess/adapters.go#L238)。

## 5. 生命周期与关键实现

### 5.1 业务状态机

```text
created ── Start 成功且 Runtime ownership 已确认 ──▶ active
   │                                                ├── End：Stop 确认 stopped ──▶ ended
   │                                                └── cleaned runtime failure ─▶ failed
   └── End（没有 Runtime，无需 Stop）───────────────────────▶ ended
```

允许的迁移由 Service 判断，并由数据库事务再次使用 `Expected` 状态做并发保护：

- `created -> active`：[Service 提交激活](../services/api/sessions/service_start_runtime.go#L138) / [Repository 原子提交 Session + Operation](../services/api/sessions/postgres_end.go#L285)
- `created -> ended`：[created 快捷结束](../services/api/sessions/service_end.go#L151)
- `active -> ended`：[Stop 后提交结束](../services/api/sessions/service_end.go#L211)
- `active -> failed`：[已清理 Runtime 的失败通知](../services/api/sessions/service_failure.go#L8)

### 5.2 Create：只创建业务会话

Create 主链路很窄：校验认证、幂等键、终端能力和音频配置，生成 ID 与时间，然后调用 Repository；它不会读取或创建 Runtime。

```text
Handler 生成 request hash
  -> Service 校验配置
  -> Repository 在一个事务中写 voice_sessions + create_requests
  -> 相同 key + 相同 hash 重放原 Session
  -> 相同 key + 不同 hash 返回 idempotency_key_conflict
```

代码：

- [Create 业务编排](../services/api/sessions/service_create.go#L10)
- [Create 事务与并发冲突处理](../services/api/sessions/postgres.go#L48)
- [Create 幂等表](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L103)

### 5.3 Start：持久化 Operation + Runtime ownership + 补偿

Start 是 Session 模块最核心的架构实现。

```text
GetOwned
  -> 查找/恢复同一个 StartOperation
  -> 校验语言配置 active 且有两个方向
  -> 校验 WebRTC connection = connected
  -> BeginStartOperation(status=pending)
  -> Realtime.Start(session_id, operation_id)
  -> 校验 Runtime.StartOperationID == 当前 OperationID
  -> Runtime 已运行：事务提交 Session=active + Operation=completed
  -> 结果不确定：用独立超时再读取一次 Runtime 做 reconciliation
  -> 已启动但业务提交失败：先 Claim compensation，再允许 Stop
```

对应代码：

1. [按 Session 串行化并根据业务状态分流](../services/api/sessions/service_start.go#L13)
2. [先恢复已有 Operation，再执行 readiness](../services/api/sessions/service_start.go#L40)
3. [语言配置和 WebRTC 启动前置条件](../services/api/sessions/service_start.go#L163)
4. [在跨 Realtime 边界前持久化 pending Operation](../services/api/sessions/service_start.go#L195)
5. [调用 Realtime.Start 并处理不确定结果](../services/api/sessions/service_start_runtime.go#L12)
6. [校验 Runtime 归属于当前 Operation](../services/api/sessions/service_start_runtime.go#L166)
7. [原子提交 `created -> active` 与 `pending -> completed`](../services/api/sessions/postgres_end.go#L285)
8. [Repository 授权补偿后才允许 Stop](../services/api/sessions/service_start.go#L355)
9. [数据库 Compensation Claim 的排他规则](../services/api/sessions/postgres_start.go#L245)

关键回答：为什么不能直接“Realtime.Start 成功后 UPDATE status=active”？因为调用超时不等于启动失败，且多个 API 实例可能同时处理同一请求。OperationID 同时绑定持久化请求和 Runtime ownership，使超时后的查询、幂等重放与补偿都有可证明的归属。

**OperationID 不是普通随机字段。** 它标识一次具体的 Start 操作，用于三个目的：

1. 跨服务幂等：相同 SessionID + OperationID 的重试仍属于同一次启动。
2. 并发冲突判断：另一个 OperationID 不能占用已经存在的 Runtime。
3. Runtime 所有权校验：Realtime 返回的 `StartOperationID` 必须等于数据库中的 OperationID，Session 才能写 `active`。

导师追问“Realtime Start 超时怎么办”时，最小回答是：**不能直接认定失败**。Session 会用一个新的有界 Context 调用 `GetRuntimeState` 对账；如果匹配本次 OperationID 的 Runtime 已进入运行态，就继续提交 `active`，否则保持 Operation 可重试。实现见 [reconcileUncertainStart](../services/api/sessions/service_start_runtime.go#L35)。

### 5.4 End：持久化 Intent + cleanup-confirmed commit + Worker 恢复

```text
GetOwned
  -> SaveEndIntent（保存 key/hash/reason/trace + 执行租约）
  -> created：直接 TransitionToEnded
  -> active：Realtime.Stop
  -> 只接受匹配 Session 且 runtime_state=stopped 的快照
  -> TransitionToEnded
  -> CompleteEndIntent
  -> 任一步失败：保留原业务状态与未完成 Intent，交给重放或 Worker
```

对应代码：

1. [请求路径先保存 EndIntent](../services/api/sessions/service_end.go#L12)
2. [active Session 必须先 Stop 再写 ended](../services/api/sessions/service_end.go#L191)
3. [Stop 快照必须明确为 stopped](../services/api/sessions/service_end.go#L247)
4. [Worker 领取并恢复一个未完成 Intent](../services/api/sessions/end_recovery.go#L73)
5. [PostgreSQL 使用 `FOR UPDATE SKIP LOCKED` 和租约防止多实例重复执行](../services/api/sessions/postgres_end_recovery.go#L10)

关键不变量：Stop 失败、超时或返回非法快照时，`active` 和 `ended_at` 保持不变。业务层不会伪装成已结束；相同幂等请求或后台 Worker 可以安全恢复。

**EndIntent 的作用是先把“必须结束这场会话”变成持久化事实。** 这样即使 Stop 超时、客户端请求中断或 API 进程重启，结束任务也不会丢失。Recovery Worker 可以重新领取未完成 Intent，继续执行幂等 Stop 和业务状态提交。

### 5.5 Query：Detail/State 聚合，List 保持纯持久化

- Detail 和 State：先做账户范围 Session 读取，再读取一次 Runtime 快照，见 [readOwnedWithRuntime](../services/api/sessions/service_query.go#L87)。
- `created`、`ended`、`failed` 在 Realtime 明确返回“无记录”时可以合成 `stopped`；`active` 无 Runtime 则返回 `runtime_state_unavailable`，见 [synthesizeMissingRuntime](../services/api/sessions/service_query.go#L135)。
- List：只查 PostgreSQL，不对每一行调用 Realtime，避免跨服务 N+1，见 [Service.List](../services/api/sessions/service_query.go#L56) 和 [游标分页 SQL](../services/api/sessions/postgres.go#L214)。
- OpenAPI 测试明确禁止 ListItem 携带 Runtime 字段，见 [契约边界测试](../packages/contracts/openapi_voice_sessions_test.go#L33)。

## 6. 数据与一致性设计

| 表 | 负责什么 | 核心约束 / 代码 |
| --- | --- | --- |
| `voice_sessions` | 业务实体和终态时间 | [状态与时间戳 CHECK](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L63)、[failed 终态时间迁移](../services/api/recordstore/migrations/000013_session_failed_terminal_timestamp.up.sql#L1) |
| `voice_session_create_requests` | Create 的 key → hash → session 结果 | [表与复合外键](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L103) |
| `voice_session_start_operations` | Start 幂等、Runtime ownership、补偿状态 | [表与状态约束](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L118)、[兼容迁移](../services/api/recordstore/migrations/000010_session_start_operation_compatibility.up.sql#L1) |
| `voice_session_end_intents` | End 幂等、恢复进度和执行租约 | [基础表](../services/api/recordstore/migrations/000002_member5_control_plane.up.sql#L168)、[恢复字段和索引](../services/api/recordstore/migrations/000014_end_intent_recovery.up.sql#L1) |

三类幂等对象没有合并成一个通用表，因为它们的原子结果不同：

- Create：原子结果是“请求身份 + 新 Session”。
- Start：原子结果是“请求身份 + `created -> active` + Operation completed”，还需要补偿所有权。
- End：原子结果是“结束意图 + cleanup-confirmed 终态”，失败后必须可恢复。

并发边界分两层：

- [`keyedLocker`](../services/api/sessions/locker.go#L8) 只在单进程内按 Session ID 串行化，减少重复外部调用。
- PostgreSQL 行锁、唯一索引、Expected 状态和持久化 Operation/Intent 才是跨实例权威；例如 [BeginStartOperation 锁 Session 并检查 End 互锁](../services/api/sessions/postgres_start.go#L72)。

## 7. 依赖与授权关系

### 7.1 生产装配

[`newSessionHTTPDependencies`](../services/api/session_runtime.go#L61) 负责组装：

```text
PostgresRepository
  + LanguageConfigAdapter
  + WebRTCConnectionAdapter
  + RealtimeLifecycleAdapter
  + ULIDGenerator
  + UTC Clock
  -> sessions.Service
  -> sessions.Handler + EndRecoveryWorker
```

所有必选依赖都会在构造阶段 fail-fast，见 [`sessions.NewService`](../services/api/sessions/service.go#L43)。这样不会生成“部分可用”的 Service，把缺失依赖拖到业务请求时才暴露。

### 7.2 账户 Actor 与不可变 Owner

外部请求必须走 `GetOwned` / 带 `AccountID` 的 List。Repository 用 `lingow_account_lineage(actor_account_id)` 做授权，但保留 `voice_sessions.account_id` 作为历史 Session 的不可变 Owner，见 [getSessionForActor](../services/api/sessions/postgres.go#L333)。

这解决了匿名账户绑定手机号后的场景：新账户可以访问原会话，但 StartOperation、EndIntent 和历史归属无需批量改写。内部 [`SessionReader.GetSession`](../services/api/sessions/ports.go#L292) 是可信模块接口，不得用于外部授权。

## 8. 错误与失败语义

稳定错误码定义在 [`errors.go`](../services/api/sessions/errors.go#L5)，HTTP 映射集中在 [`statusCodeForError`](../services/api/sessions/http.go#L437)。汇报时建议强调以下区别：

| 情况 | 结果 |
| --- | --- |
| 相同幂等键、不同请求内容 | `409 idempotency_key_conflict` |
| 另一个未完成 Start 占用 Session | `409 session_start_in_progress` |
| 语言或 WebRTC 尚未就绪 | `409 language_config_not_ready / webrtc_not_ready` |
| Runtime 服务不可用 | `503 runtime_state_unavailable`，不伪造 `stopped` |
| Start 调用结果不确定 | 读取一次 Runtime reconciliation；无法确认则保持 Operation pending |
| Start 后业务提交失败 | 取得 DB compensation claim 后 Stop；无 claim 严禁 Stop |
| End Stop 未确认 | 保持原业务状态，EndIntent 留待重放 / Worker 恢复 |
| Realtime 不可恢复失败 | 只有媒体资源已清理后，才通过内部接口写 `active -> failed` |

## 9. 测试与验收证据

### 9.1 最适合现场展示的一条测试

[`TestSessionProductionCompositionRunsControlPlaneFlow`](../services/api/main_integration_test.go#L199) 覆盖真实生产装配下的完整控制面：

1. Create 与 Create 幂等冲突：[对应断言](../services/api/main_integration_test.go#L268)
2. 缺语言配置拒绝 Start：[对应断言](../services/api/main_integration_test.go#L299)
3. WebRTC 未连接拒绝 Start：[对应断言](../services/api/main_integration_test.go#L322)
4. Start 成功且相同请求不重复调用 Realtime：[对应断言](../services/api/main_integration_test.go#L335)
5. 外部账户不可读取、合并后账户可读取：[对应断言](../services/api/main_integration_test.go#L393)
6. List 不调用 Realtime：[对应断言](../services/api/main_integration_test.go#L404)
7. End 成功且相同请求不重复 Stop：[对应断言](../services/api/main_integration_test.go#L413)

### 9.2 关键单元 / 集成测试入口

| 风险 | 测试证据 |
| --- | --- |
| HTTP 身份、规范化哈希、路由和错误码 | [http_test.go](../services/api/sessions/http_test.go#L14) |
| Start 前置条件与主流程 | [TestServiceStartRunsPrerequisitesBeforeTransition](../services/api/sessions/service_start_test.go#L10) |
| Start 超时后确认已运行 Runtime | [TestServiceStartReconcilesRunningRuntimeAfterStartTimeout](../services/api/sessions/service_start_runtime_test.go#L78) |
| 跨实例竞争时 loser 不误停 winner 的 Runtime | [Start recovery tests](../services/api/sessions/service_start_recovery_test.go#L681) |
| End 必须 Stop 后再迁移 | [TestServiceEndActiveStopsBeforeTransition](../services/api/sessions/service_end_test.go#L497) |
| End Stop 失败保持业务状态 | [TestServiceEndActiveStopErrorPreservesSession](../services/api/sessions/service_end_test.go#L581) |
| End Worker 租约互斥 | [TestEndRecoveryLeaseExcludesAnotherWorker](../services/api/sessions/end_recovery_test.go#L185) |
| PostgreSQL 并发权威与 Start/End 互锁 | [Repository integration tests](../services/api/sessions/postgres_integration_test.go#L759) |
| OpenAPI 路由和 List/Detail 状态边界 | [OpenAPI contract test](../packages/contracts/openapi_voice_sessions_test.go#L10) |

本地验证命令：

```bash
go test ./services/api/sessions ./services/api/realtimeaccess ./packages/contracts
```

需要 PostgreSQL 的集成验证使用 `integration` tag，并配置专用的 `_test` 数据库：

```bash
go test -count=1 -tags=integration ./services/api/sessions ./services/api
```

## 10. 汇报前必须掌握与可以暂缓的内容

### 10.1 四个必须掌握的概念

| 概念 | 最小准确答案 |
| --- | --- |
| Session 与 Turn | Session 是整场会话；Turn 是其中一次发言和翻译。Session 不保存 Turn 的翻译内容 |
| OperationID | 标识一次 Start，解决跨服务幂等、并发冲突和 Runtime 所有权校验 |
| EndIntent | 在 Stop 前持久化结束请求，使超时、中断或重启后的结束流程可以恢复 |
| 三类状态 Owner | 业务状态归 Session；Runtime 状态归 Realtime；连接状态归 Realtime 的 WebRTC 模块 |

### 10.2 今晚不需要深挖到代码细节的部分

这些内容现场可以回答“具体实现封装在 Repository / Adapter 中”，但至少要知道它们解决什么问题：

| 实现细节 | 解决的问题 |
| --- | --- |
| PostgreSQL 行锁、唯一约束 | 多 API 实例并发时只能有一个权威结果 |
| `FOR UPDATE SKIP LOCKED`、Lease、Recovery Worker | 多 Worker 互斥领取和恢复未完成 EndIntent |
| HMAC Realtime Ticket | 浏览器以短期、会话范围凭证安全连接 Realtime |
| Adapter 的穷举映射 | 隔离 Session 与 Language / Realtime 的类型和错误 |
| keyed locker | 减少单实例内同一 Session 的重复工作，不承担跨实例权威 |
| Context 超时、指数退避、完整错误码表 | 控制失败预算和重试节奏；不是本次架构汇报主线 |

## 11. 可辅助展示的 PR

以下 PR 来自当前仓库 Git 历史，可用于按实现切片展示评审过程：

- [PR #103：Session Create](https://github.com/1024XEngineer/xe6-tsy/pull/103)
- [PR #106：Session Query](https://github.com/1024XEngineer/xe6-tsy/pull/106)
- [PR #107：Start 编排与补偿](https://github.com/1024XEngineer/xe6-tsy/pull/107)
- [PR #115：持久化 StartOperation 一致性契约](https://github.com/1024XEngineer/xe6-tsy/pull/115)
- [PR #120：Session End](https://github.com/1024XEngineer/xe6-tsy/pull/120)
- [PR #132：PostgreSQL Repository](https://github.com/1024XEngineer/xe6-tsy/pull/132)
- [PR #149：账户 lineage 与不可变 Owner](https://github.com/1024XEngineer/xe6-tsy/pull/149)
- [PR #154：HTTP + OpenAPI](https://github.com/1024XEngineer/xe6-tsy/pull/154)
- [PR #155：Realtime / Language / WebRTC Adapter](https://github.com/1024XEngineer/xe6-tsy/pull/155)

## 12. 导师可能追问的问题

### Q1：Session 和 Turn 有什么区别？

Session 是从创建到结束的一整场语音沟通；Turn 是其中一次发言及其识别、翻译结果。一场 Session 可以包含多个 Turn，二者通过 `session_id` 关联。

### Q2：为什么 Session 状态和 Runtime 状态要分开？

Session 状态是需要长期持久化的业务事实；Runtime 状态是媒体面的瞬时状态。把两者混在一张表会造成双写、状态漂移和高频更新污染业务数据。代码上通过 [`VoiceSession`](../services/api/sessions/model.go#L121) 与 [`RuntimeSnapshot`](../services/api/sessions/model.go#L167) 两个模型强制分离。

### Q3：为什么进程内锁不够？

它只能保护单个 API 实例，无法处理多副本、进程退出或网络超时。跨实例权威由 PostgreSQL 的 StartOperation / EndIntent、行锁和唯一约束提供；代码注释也明确指出这一点，见 [Start 入口](../services/api/sessions/service_start.go#L10)。

### Q4：Start 超时了，怎么判断到底启动成功没有？

使用同一个 OperationID 查询 Runtime。只有 `RuntimeSnapshot.StartOperationID` 与当前持久化 Operation 匹配，且 Runtime 处于有效运行态，才提交 `active`；见 [reconcileUncertainStart](../services/api/sessions/service_start_runtime.go#L35)。

### Q5：为什么 End 失败时不直接把 Session 标为 ended？

因为可能遗留 Pipeline、PeerConnection 或音频资源。必须拿到匹配 Session 的 `stopped` 快照才提交业务终态；否则保留 `active` 和未完成 EndIntent，由重放或 Worker 恢复。

### Q6：Session 处于 active 期间做什么？

保持整场会话的业务状态为 `active`，并提供 Detail / State 查询。实时音频、识别、翻译、TTS 和播放由 Realtime 负责。

### Q7：翻译记录由谁保存？

Records 模块保存。Realtime 产生 FinalTurn，Records 按 `session_id` 持久化；Session 只管理整场会话的生命周期。

### Q8：为什么 List 不返回 Runtime？

List 是历史会话的持久化投影。若每行查询 Realtime，会形成跨服务 N+1，而且历史终态不需要实时状态。需要实时信息时使用 Detail 或 State。

### Q9：Session 如何避免依赖其他模块的内部实现？

接口由 Session 作为消费者定义，Adapter 对状态、时间和错误做穷举映射；Service 不 import provider 实现，也不比较错误字符串，见 [`realtimeaccess/adapters.go`](../services/api/realtimeaccess/adapters.go#L20)。

## 13. 可直接照读的主讲稿

> 我负责的是 API Service 内部的 Session 模块。
>
> 这里的 Session 不是一轮翻译，而是从创建、启动、运行到结束的一整场实时语音翻译会话。一场 Session 里面可以包含很多轮发言和翻译，但每轮翻译内容不由 Session 模块保存，而是由 Realtime 产生、Records 持久化。
>
> Session 当前直接协作四类边界。Auth 提供可信的 AccountID；Language 提供当前语言配置是否已经 ready；PostgreSQL 保存会话业务状态以及 StartOperation 和 EndIntent；Realtime Service 提供 WebRTC 连接状态、Runtime 启动、状态查询和停止能力。Records、Usage 和 Delivery 不是 Session 的直接依赖，它们通过 SessionID 与这场会话关联。
>
> 会话开始时，Session 先读取 Session 并验证用户是否有权操作，然后检查 Language 配置是否 ready，再检查 WebRTC 是否已经 connected。对于新的启动请求，条件满足后 Session 不会直接把状态改成 active，而是先在数据库中保存 StartOperation 并得到 OperationID，再调用 Realtime.Start。
>
> Realtime 返回 RuntimeSnapshot 后，Session 会校验返回的 SessionID 和 StartOperationID 是否属于本次启动。只有确认 Runtime 已进入可运行状态，Session 才把业务状态从 created 修改为 active。OperationID 用于标识这一次 Start，解决重复请求、并发启动和 Runtime 所有权校验。如果 Realtime.Start 超时，Session 不会直接认定失败，而是调用 GetRuntimeState 对账。
>
> 进入 active 后，WebRTC、VAD、ASR、翻译、TTS 和播放由 Realtime Service 负责。Session 不处理实时音频，也不保存每一轮翻译内容。它继续维护整场会话的业务生命周期，并在详情或状态查询时组合 PostgreSQL 中的业务状态与 Realtime 中的运行状态。
>
> 会话结束时，Session 先在数据库中保存 EndIntent，再调用 Realtime.Stop。只有 Realtime 确认资源已经清理并返回 RuntimeStopped，Session 才把业务状态从 active 修改为 ended。
>
> 如果 Stop 失败、超时，或者 API 服务重启，Session 不会提前写 ended，而是保留 active 和未完成的 EndIntent，由 Recovery Worker 继续恢复结束流程。
>
> 因此，Session 模块的主要作用是维护整场会话的业务生命周期，并保证 Auth、Language、WebRTC、Realtime Runtime 和 PostgreSQL 状态按照正确顺序协作。

## 14. 30 秒收尾稿

> Session 模块的核心不是 CRUD，而是控制面与媒体面之间的一致性边界。业务状态、Runtime 状态和 WebRTC 连接状态分别由各自模块持有；Session 通过最小 Port 和显式 Adapter 编排它们。Create、Start、End 分别用独立的持久化幂等对象保证结果可重放；Start 用 OperationID 绑定 Runtime ownership 并支持受控补偿，End 用 EndIntent 和租约 Worker 保证清理确认后再提交终态。最终，数据库事务负责跨实例权威，进程内锁只做局部优化，完整控制面流程已有契约测试、单元测试和 PostgreSQL 集成测试覆盖。

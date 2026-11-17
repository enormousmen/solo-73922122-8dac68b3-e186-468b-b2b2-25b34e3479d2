# 单进程影子流量网关（FastAPI + aiohttp）

新版报价接口上线前的旁路比对网关：固定并行调用**主端**与**影子端**两个本地 GET 报价服务，
客户端只收到**当前报价出口端**的原始状态码与原始字节；另一端继续旁路比对，其慢、失败、
超时完全不影响出口响应。值守人员可在网页上一键把影子端升为真实报价出口，异常时立即切回旧端。

## 架构

```
浏览器 (web 控制台)
      │  GET /quote, /quote-big          POST /api/switch/promote|fallback
      ▼
┌──────────────── gateway (FastAPI, 单进程, mem_limit 96m) ───────────────┐
│  受理时固定 出口/旁路/轮次  名额信号量(最多4)  并行 aiohttp 调用        │
│  流式 SHA-256  比对记录(内存环形)  当前轮受理台账(升主核验用)          │
└───────┬───────────────────────────────────────────┬────────────────────┘
        ▼                                           ▼
   primary:8000 (主报价服务)                  shadow:8000 (影子报价服务)
   默认出口；切回时立即恢复出口                默认旁路；升主后成为出口
```

- 两端使用同一镜像 `shadow-gateway:latest`，以 `SERVICE_ROLE=primary|shadow` 区分。
- 网关只代理 `GET /quote` 与 `GET /quote-big`；网页与 `/api/*` 管理接口不拦截。

## 关键语义（与需求逐条对应）

| 需求 | 实现位置 |
| --- | --- |
| 两端并行调用，客户端只收出口端原状态码、原字节 | `app/gateway.py` `proxy()` / `body_iter()` 逐块透传 |
| 旁路慢/失败/超时不拖住出口响应 | 旁路为独立后台任务；出口流结束即返回客户端，`wait_for(2s)` 只取消旁路 |
| 两端流式 SHA-256，完整结束才比较 | 每块 `hash.update()` 后立即释放；`_compare()` 仅在两端 `completed` 时调用 |
| 未收完整不能报一致 | 中间态 `shadow_pending` / 断连 / 超时 / 出口不完整均不会产生 `match` |
| 最多 4 个旁路调用，满则跳过并注明原因 | `ShadowLimiter`（acquire/release 覆盖整个旁路通信生命周期） |
| 旁路发起起 2 秒超时，只终止旁路 | `asyncio.wait_for(coro, timeout=2.0)` 包住完整旁路请求 |
| 客户端断连终止两端 | `request.is_disconnected()` 检测后取消旁路任务并释放出口连接 |
| 通信实际退出后才归还名额 | 名额在 `run_bypass` 的 `finally` 中释放（首调度前被取消时由 `_cancel_bypass` 兜底） |
| 不整份缓存/落盘，内存限 96MiB | 仅保存 64KiB 块与哈希状态；compose `mem_limit: 96m`，实测峰值 < 60MiB |
| 升主核验：当前轮至少 3 次报价、最近 3 次双端完整/非 5xx/一致、无在途 | `accept_quote()` + `_eval_promotion_locked()` + `_quote_issue()` |
| 跳过/断流/超时不能凑数，拒绝时说明原因 | `_quote_issue()` 逐条给出中文原因，`POST /api/switch/promote` 返回 409 + `reasons` |
| 切回无需核验 | `POST /api/switch/fallback` 直接切换，不做任何统计校验 |
| 切换与请求并发时，每次报价固定受理时的出口及轮次 | `accept_quote()` 在 `SwitchState.lock` 内同时读出口、取轮次、登记台账 |
| 在途响应不得混入另一端字节 | 每条请求的出口 URL、状态码、响应流在受理时绑定，后续切换不改变该请求 |
| 新出口原状态码与字节仍逐块透传，另一端继续旁路比对 | `proxy()` 的出口/旁路角色来自 `ctx.exit_role` / `ctx.bypass_role` |
| 沿用原有名额、超时和内存限制 | 切换只换 URL，不改变 `ShadowLimiter` / `SHADOW_TIMEOUT_S` / `mem_limit` |
| 记录标明出口和轮次 | 每条记录含 `exit`、`bypass_role`、`round`、`seq` 字段 |
| 旧轮迟到的结果不得充作新轮升主依据 | 轮次推进时清空当前轮台账；旧轮请求的 `ctx.round` 固定，结果只回写自己的记录 |

### 升主判定细节

升主只统计**当前轮**按受理顺序最近的 3 条报价（`sw.ledger`，受理时登记），逐条要求：

1. 双端都收到完整响应（有状态码与摘要，"跳过/超时/断流/断连"都不算）；
2. 两端状态码都不属于 5xx；
3. 两端状态码相同且 SHA-256 相同（即 `match`）。

另外要求当前轮至少受理 3 次、`active == 0`（出口流与旁路通信都已退出）。任一条件不满足时
`POST /api/switch/promote` 返回 409，`reasons` 数组逐条说明是哪一条受理、什么原因。
轮次在每次成功升主或切回后 +1，并清空当前轮台账；在途请求的轮次在受理时已固定，
其迟到结果只会写回自己的记录，不会进入新轮台账。

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 网页控制台:  http://localhost:8000/
# 网关健康检查: http://localhost:8000/healthz
```

### 本地直接运行（无 Docker）

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
bash scripts/run_local.sh
```

## 报价服务的可控参数（query string 会被网关原样转发）

| 参数 | 作用 |
| --- | --- |
| `delay=<秒>` | 两端首字节前共同延迟 |
| `p_delay` / `s_delay` | 仅主端 / 仅影子端延迟（验证超时隔离） |
| `diff=1` | 仅影子端制造差异（JSON 改价 / 大响应改首字节） |
| `status=<码>` | 自定义 HTTP 状态码（验证原状态码透传） |
| `fail=1` | 两端返回相同的 500 |
| `size=<字节>` | `/quote-big` 响应大小，默认 33554432（32MiB） |
| `chunk_ms=<毫秒>` | 大响应每块间隔（验证流式与客户端断连） |
| `abort_after=<字节>` | `/quote-big` 发出指定字节后中断连接（模拟断流） |
| `p_abort_after` / `s_abort_after` | 仅主端 / 仅影子端断流 |

## 网页控制台

- 顶部常驻显示当前出口、轮次、在途数、本轮受理数。
- 左侧表单可设置延迟、并发数（最多 20）、差异/失败/断流场景并**并发发起**。
- 左侧"出口切换"：**升为真实报价出口**（不满足条件时列出原因）、**立即切回旧端**，
  并展示最近 3 条受理记录的双端状态码与是否可计入。
- 右上"出口响应（逐块）"按请求分 Tab，展示每块序号、字节数、内容预览与累计字节。
- 右下"比对记录"每 1.5s 自动刷新：结果药丸、**出口/轮次**、两端状态码、耗时、
  SHA-256 摘要与原因。

## 管理 API

- `GET /healthz`：上下游地址、当前出口、轮次、在途数、旁路名额与超时配置。
- `GET /api/switch/status`：出口、轮次、在途、本轮受理数、最近 3 条台账、拒绝原因、切换事件。
- `POST /api/switch/promote`：升主；不满足条件返回 409 + `reasons`。
- `POST /api/switch/fallback`：立即切回主端，不做核验。
- `GET /api/records`：比对记录（最新在前，内存环形缓冲，默认保留 100 条）。
- `GET /api/state`：出口、轮次、在途数、旁路在途数、记录数、网关峰值 RSS。

## 自测

```bash
# 先按 scripts/run_local.sh 启动三服务, 然后:
.venv/bin/python -I tests/selftest.py http://127.0.0.1:8000
.venv/bin/python -I tests/switch_selftest.py http://127.0.0.1:8000
```

- `tests/selftest.py`：一致/不一致比对、出口字节与状态码原样透传、旁路 2s 超时隔离、
  4 名额满跳过且注明原因、名额归还、32MiB 流式完整性与摘要、96MiB 内存约束、客户端断连双端终止。
- `tests/switch_selftest.py`：不足 3 次 / 5xx / 断流 / 跳过 / 超时 / 在途时拒绝升主并给出原因；
  最近 3 条合格才升主且轮次 +1；升主后字节来自影子端；切回无需核验；
  切换与在途请求并发时出口/轮次固定在受理时刻且字节不混；旧轮迟到结果不能充作新轮依据。

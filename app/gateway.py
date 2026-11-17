"""
单进程影子流量网关 (FastAPI + aiohttp)

- 固定两个本地 GET 报价服务: 主端 PRIMARY_URL / 影子端 SHADOW_URL
- 默认主端是报价出口; 值班人员可在网页/管理 API 把影子端升为出口, 异常时立即切回主端
- 两端并行调用, 客户端只收到"受理时出口端"的原始状态码和原始字节(逐块透传)
- 另一端继续旁路比对; 慢/失败/超时(发起起 2s)只影响旁路端, 不拖住出口响应
- 两端各自流式计算 SHA-256, 两端都完整结束才比较状态码与摘要
- 最多 4 个旁路并发, 名额满时跳过并注明原因, 出口请求照常通过
- 客户端断连同时终止两端; 旁路通信实际退出后才归还名额
- 不整份缓存/落盘响应, 每块读取后立即更新哈希并释放

切换语义:
- 每条报价在"受理"时原子地固定出口角色、旁路角色和轮次; 之后发生的切换不影响
  在途请求, 另一端的字节不会混入该响应
- 轮次在每次成功升主/切回后 +1。升主只统计"当前轮"按受理顺序最近的 3 条报价,
  旧轮迟到的结果不会进入新轮的受理台账, 因此不能充作新轮升主依据
- 升主要求当前轮至少受理 3 次, 且按受理顺序最近 3 次均双端完整、非 5xx、状态码与
  SHA-256 一致, 同时没有在途报价或比对; 不满足时拒绝并逐条说明原因
- 切回旧端不做任何核验, 可立即生效(在途请求仍固定在原出口/轮次)
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import resource
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

PRIMARY_URL = os.environ.get("PRIMARY_URL", "http://primary:8001")
SHADOW_URL = os.environ.get("SHADOW_URL", "http://shadow:8002")
MAX_SHADOW_CALLS = int(os.environ.get("MAX_SHADOW_CALLS", "4"))
SHADOW_TIMEOUT_S = float(os.environ.get("SHADOW_TIMEOUT_S", "2.0"))
READ_CHUNK = 64 * 1024
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "100"))
MAX_EVENTS = int(os.environ.get("MAX_EVENTS", "50"))
# 当前轮的受理台账只需保留最近一段; 升主只看最近 3 条
MAX_LEDGER = int(os.environ.get("MAX_LEDGER", "200"))
PROMOTE_MIN_QUOTES = 3

ROLES = {"primary": PRIMARY_URL, "shadow": SHADOW_URL}
ROLE_LABEL = {"primary": "主端", "shadow": "影子端"}

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    # 长度由实际透传的字节流重新决定, 避免上游声明与透传块边界不一致
    "content-length",
}

# 已定性的终态: finalize 不得覆盖
TERMINAL_RESULTS = {
    "match", "mismatch", "shadow_timeout", "shadow_error",
    "client_disconnected", "primary_error", "primary_incomplete",
    "shadow_skipped_no_slot",
}

PENDING_RESULTS = {
    "pending", "shadow_pending", "shadow_completed_primary_incomplete",
}

RESULT_TEXT = {
    "match": "状态码与 SHA-256 均一致",
    "mismatch": "状态码或摘要不一致",
    "shadow_timeout": "旁路超时",
    "shadow_error": "旁路失败",
    "shadow_skipped_no_slot": "旁路名额满被跳过",
    "client_disconnected": "客户端断连",
    "primary_error": "出口连接失败",
    "primary_incomplete": "出口响应不完整",
    "shadow_pending": "旁路比对仍在途",
    "shadow_completed_primary_incomplete": "出口响应未完成",
    "pending": "处理中",
    "unknown": "未知",
}


def other_role(role: str) -> str:
    return "shadow" if role == "primary" else "primary"


@dataclass
class ShadowLimiter:
    """旁路名额: acquire 到 release 之间覆盖整个旁路通信的生命周期。"""
    capacity: int = MAX_SHADOW_CALLS
    in_flight: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> bool:
        async with self.lock:
            if self.in_flight >= self.capacity:
                return False
            self.in_flight += 1
            return True

    async def release(self) -> None:
        async with self.lock:
            self.in_flight -= 1


@dataclass
class SwitchState:
    """出口角色/轮次/受理台账, 所有读写都在 lock 内完成。"""
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    exit_role: str = "primary"
    round: int = 1
    active: int = 0              # 在途报价数(出口流未结束或旁路比对未结束)
    round_accepts: int = 0       # 当前轮已受理报价数
    seq: int = 0                 # 全局受理序号
    ledger: deque = field(default_factory=lambda: deque(maxlen=MAX_LEDGER))
    events: deque = field(default_factory=lambda: deque(maxlen=MAX_EVENTS))


@dataclass
class CallContext:
    rid: str
    seq: int
    target: str
    started_at: float
    round: int
    exit_role: str
    bypass_role: str
    record: dict
    primary_done: bool = False   # 主端这一路(不一定是出口)是否完整结束
    shadow_done: bool = False    # 影子端这一路(不一定是旁路)是否完整结束
    exit_done: bool = False      # 出口响应生命周期是否结束
    bypass_done: bool = False    # 旁路通信是否退出(或未启动)
    bypass_acquired: bool = False
    finished: bool = False       # 在途计数是否已归还
    cleanup_scheduled: bool = False
    saved: bool = False
    bypass_task: Optional[asyncio.Task] = None
    cancel_result: Optional[str] = None


state: dict = {}
# 后台旁路任务的强引用集合: 防止生成器返回后 ctx 被回收导致任务被 GC 销毁
background_tasks: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    connector = aiohttp.TCPConnector(limit=64)
    # 出口不用总超时(由客户端断连控制), 仅设连接级保护
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=30)
    state["session"] = aiohttp.ClientSession(connector=connector, timeout=timeout)
    state["limiter"] = ShadowLimiter()
    state["switch"] = SwitchState()
    state["records"] = []
    state["records_lock"] = asyncio.Lock()
    try:
        yield
    finally:
        await state["session"].close()


app = FastAPI(title="shadow-traffic-gateway", lifespan=lifespan)


def now_ms() -> int:
    return int(time.time() * 1000)


async def append_record(rec: dict) -> None:
    async with state["records_lock"]:
        recs = state["records"]
        recs.append(rec)
        if len(recs) > MAX_RECORDS:
            del recs[:-MAX_RECORDS]


def filtered_headers(resp: aiohttp.ClientResponse) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}


def _leg_completed(rec: dict, role: str) -> bool:
    return (
        rec.get(role) == "completed"
        and rec.get(f"{role}_status") is not None
        and rec.get(f"{role}_sha256") is not None
    )


async def accept_quote(target: str) -> CallContext:
    """
    受理一条报价: 在同一把锁内固定出口端与轮次, 并登记到当前轮台账。
    这样切换与受理并发时, 每条请求只可能完整地落在切换前或切换后。
    """
    sw: SwitchState = state["switch"]
    started = now_ms()
    async with sw.lock:
        sw.seq += 1
        ctx = CallContext(
            rid=f"r{started}-{sw.seq:06d}",
            seq=sw.seq,
            target=target,
            started_at=started,
            round=sw.round,
            exit_role=sw.exit_role,
            bypass_role=other_role(sw.exit_role),
            record={},
        )
        ctx.record = {
            "id": ctx.rid,
            "seq": ctx.seq,
            "target": target,
            "started_at": started,
            # 出口与轮次在受理时固定, 只作为该请求的记录, 不受后续切换影响
            "exit": ctx.exit_role,
            "bypass_role": ctx.bypass_role,
            "round": ctx.round,
            "result": "pending",
            "primary": "not_started",
            "shadow": "not_started",
            "bypass": "not_started",
        }
        ctx.record[ctx.exit_role] = "pending"
        sw.active += 1
        sw.round_accepts += 1
        sw.ledger.append(ctx)
        return ctx


async def maybe_finish(ctx: CallContext) -> None:
    """出口流与旁路通信都结束后, 才归还在途计数(升主以此为'无在途'依据)。"""
    if ctx.finished or not (ctx.exit_done and ctx.bypass_done):
        return
    sw: SwitchState = state["switch"]
    async with sw.lock:
        if ctx.finished:
            return
        ctx.finished = True
        sw.active -= 1
        ctx.bypass_task = None


async def run_bypass(ctx: CallContext) -> None:
    """
    执行一次旁路调用 (从发起起整体 2s 硬超时)。
    无论成功/失败/超时/取消, 通信实际退出后才归还旁路名额。
    只更新记录字段, 记录由出口生命周期落库, 避免出现半条记录。
    """
    rec = ctx.record
    role = ctx.bypass_role
    try:
        session = state["session"]
        try:
            coro = session.get(ROLES[role] + ctx.target, allow_redirects=False)
            # wait_for 从任务创建(发起)时刻起计时, 超时只取消旁路请求
            async with await asyncio.wait_for(coro, timeout=SHADOW_TIMEOUT_S) as resp:
                bypass_status = resp.status
                bypass_hash = hashlib.sha256()
                async for chunk in resp.content.iter_chunked(READ_CHUNK):
                    bypass_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                bypass_digest = bypass_hash.hexdigest()
        except asyncio.TimeoutError:
            rec["result"] = "shadow_timeout"
            rec["bypass"] = "timeout"
            rec[role] = f"timeout after {SHADOW_TIMEOUT_S:g}s"
            return
        except asyncio.CancelledError:
            rec["bypass"] = "cancelled"
            rec[role] = "cancelled with exit"
            # 出口失败/断连时已写定结果; 兜底不覆盖已有终态
            if rec["result"] == "pending":
                rec["result"] = ctx.cancel_result or "client_disconnected"
            raise
        except aiohttp.ClientError as exc:
            rec["result"] = "shadow_error"
            rec["bypass"] = "error"
            rec[role] = f"connection error: {type(exc).__name__}"
            return
        except Exception as exc:  # 防御: 旁路侧任何意外都不能拖垮出口流程
            logging.getLogger("shadow").exception("bypass task failed: %r", exc)
            rec["result"] = "shadow_error"
            rec["bypass"] = "error"
            rec[role] = f"unexpected error: {type(exc).__name__}: {exc}"
            return

        rec[f"{role}_status"] = bypass_status
        rec[f"{role}_sha256"] = bypass_digest
        rec[f"{role}_ms"] = now_ms() - ctx.started_at
        rec[role] = "completed"
        rec["bypass"] = "completed"
        ctx.primary_done = ctx.primary_done or role == "primary"
        ctx.shadow_done = ctx.shadow_done or role == "shadow"

        if not ctx.exit_done:
            # 出口尚未完整结束, 暂时不能比较; 最终结果由出口生命周期补算
            if rec["result"] == "pending":
                rec["result"] = "shadow_completed_primary_incomplete"
            return

        _compare(rec)
    finally:
        ctx.bypass_done = True
        if ctx.bypass_acquired:
            # 通信实际退出(含超时/取消)后才归还名额
            await state["limiter"].release()
            ctx.bypass_acquired = False
        if ctx.exit_done:
            # 出口已完整: 在这里收口并原地更新已落库的记录
            finalize(rec, ctx)
            if not ctx.saved:
                await save_record(ctx)
        await maybe_finish(ctx)


def _compare(rec: dict) -> None:
    """两端都完整结束后才调用: 比较状态码与 SHA-256 摘要。"""
    same_status = rec["shadow_status"] == rec["primary_status"]
    same_digest = rec["shadow_sha256"] == rec["primary_sha256"]
    if same_status and same_digest:
        rec["result"] = "match"
    else:
        rec["result"] = "mismatch"
        if not same_status:
            rec["detail"] = (
                f"status differs: primary={rec['primary_status']} "
                f"shadow={rec['shadow_status']}"
            )
        else:
            rec["detail"] = "same status but sha256 differs"


def finalize(rec: dict, ctx: CallContext) -> None:
    """出口完整结束后的统一收口: 补算比较结果。"""
    if rec["result"] in TERMINAL_RESULTS:
        return
    if not ctx.bypass_done:
        # 旁路仍在跑(受 2s 硬超时保护): 先挂起, 等其退出时再更新,
        # 绝不能在未收完整时报一致
        rec["result"] = "shadow_pending"
        rec["bypass"] = "in flight"
        return
    if rec["result"] in ("shadow_completed_primary_incomplete", "shadow_pending"):
        _compare(rec)
    elif rec["result"] == "pending":
        rec["result"] = "unknown"


async def save_record(ctx: CallContext) -> None:
    """记录只落库一次: 谁先收口谁保存。"""
    if ctx.saved:
        return
    ctx.saved = True
    await append_record(ctx.record)


async def _cancel_bypass(ctx: CallContext, result: str) -> None:
    """出口失败/断连: 终止旁路并等待通信实际退出(名额随任务 finally 归还)。"""
    ctx.cancel_result = result
    task = ctx.bypass_task
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        try:
            await asyncio.shield(task)
        except (asyncio.CancelledError, Exception):
            pass
    if not ctx.bypass_done:
        # 任务在首次调度前就被取消时协程体不会执行, finally 也不会跑:
        # 这里兜底归还名额, 保证在途计数与名额都不泄漏
        ctx.bypass_done = True
        if ctx.record.get("bypass") == "started":
            ctx.record["bypass"] = "cancelled"
        if ctx.bypass_acquired:
            ctx.bypass_acquired = False
            await state["limiter"].release()


async def _finish_aborted(ctx: CallContext, result: str) -> None:
    await _cancel_bypass(ctx, result)
    await save_record(ctx)
    await maybe_finish(ctx)


async def _close_exit_if_needed(ctx: CallContext) -> None:
    """响应发送完毕后调用: 若出口生命周期未正常收尾, 立即兜底清理。"""
    if not ctx.exit_done:
        _abort_exit(
            ctx, "client_disconnected",
            f"exit({ROLE_LABEL[ctx.exit_role]}) response closed before completion",
        )


def _abort_exit(ctx: CallContext, result: str, detail: str) -> Optional[asyncio.Task]:
    """
    响应生成器被提前关闭(客户端断连/意外异常)时的兜底:
    标记出口生命周期结束, 并异步终止旁路、归还名额与在途计数,
    保证在途计数不会永久泄漏(否则升主会被永远拒绝)。
    """
    if ctx.cleanup_scheduled:
        return None
    ctx.cleanup_scheduled = True
    ctx.exit_done = True
    rec = ctx.record
    role = ctx.exit_role
    if rec.get(role) != "completed" and rec.get("result") not in TERMINAL_RESULTS:
        rec[role] = "aborted before completion"
        rec["result"] = result
        rec["detail"] = detail
    task = asyncio.create_task(_finish_aborted(ctx, result))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


async def proxy(request: Request, target: str) -> StreamingResponse:
    ctx = await accept_quote(target)
    rec = ctx.record
    exit_role = ctx.exit_role
    bypass_role = ctx.bypass_role

    # 先占旁路名额(非阻塞); 满了直接跳过, 但出口请求照常通过
    if await state["limiter"].acquire():
        ctx.bypass_acquired = True
        rec["bypass"] = "started"
        task = asyncio.create_task(run_bypass(ctx))
        ctx.bypass_task = task
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    else:
        rec["bypass"] = "skipped"
        rec[bypass_role] = "skipped"
        rec["result"] = "shadow_skipped_no_slot"
        rec["detail"] = (
            f"bypass skipped: {MAX_SHADOW_CALLS} bypass calls already in flight"
        )
        # 没有旁路通信在途, 该条件直接满足; 不依赖出口结束路径, 避免计数泄漏
        ctx.bypass_done = True

    session = state["session"]
    try:
        exit_resp = await session.get(ROLES[exit_role] + target, allow_redirects=False)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        rec[exit_role] = f"connection error: {type(exc).__name__}"
        rec["result"] = "primary_error"
        rec["detail"] = (
            f"exit({ROLE_LABEL[exit_role]}) connection error: {type(exc).__name__}"
        )
        ctx.exit_done = True
        await _finish_aborted(ctx, "primary_error")
        return JSONResponse(
            status_code=502,
            content={"detail": f"{exit_role} quote service unavailable"},
        )
    except asyncio.CancelledError:
        # 客户端在响应头之前断开: 终止旁路并归还计数, 不留悬挂请求
        ctx.exit_done = True
        _abort_exit(ctx, "client_disconnected", "client disconnected before response")
        raise
    except Exception as exc:  # 防御: 未知异常也不能让在途计数永久悬挂
        logging.getLogger("gateway").exception("exit request failed: %r", exc)
        rec[exit_role] = f"unexpected error: {type(exc).__name__}"
        rec["result"] = "primary_error"
        rec["detail"] = f"exit({ROLE_LABEL[exit_role]}) error: {type(exc).__name__}"
        ctx.exit_done = True
        await _finish_aborted(ctx, "primary_error")
        return JSONResponse(
            status_code=502,
            content={"detail": f"{exit_role} quote service unavailable"},
        )

    async def body_iter():
        exit_hash = hashlib.sha256()
        try:
            async for chunk in exit_resp.content.iter_chunked(READ_CHUNK):
                if await request.is_disconnected():
                    raise asyncio.CancelledError
                exit_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                yield chunk
        except asyncio.CancelledError:
            # 客户端断连: 同时终止出口读取与旁路调用。
            # 清理放到独立任务里执行, 避免本任务再次被取消时清理半途而废。
            exit_resp.release()
            if not ctx.exit_done:
                rec[exit_role] = "aborted before completion"
                rec["result"] = "client_disconnected"
                ctx.exit_done = True
                _abort_exit(ctx, "client_disconnected", "client disconnected")
            raise
        except aiohttp.ClientError as exc:
            # 出口传输中途断开: 响应不完整, 同样终止旁路
            exit_resp.release()
            if not ctx.exit_done:
                rec[exit_role] = f"stream error: {type(exc).__name__}"
                rec["result"] = "primary_incomplete"
                rec["detail"] = (
                    f"exit({ROLE_LABEL[exit_role]}) stream error: {type(exc).__name__}"
                )
                ctx.exit_done = True
                await _finish_aborted(ctx, "primary_incomplete")
        else:
            # 出口完整结束: 客户端响应到此立刻收尾, 不等待旁路
            exit_resp.release()
            ctx.exit_done = True
            rec[exit_role] = "completed"
            rec[f"{exit_role}_status"] = exit_resp.status
            rec[f"{exit_role}_sha256"] = exit_hash.hexdigest()
            rec[f"{exit_role}_ms"] = now_ms() - ctx.started_at
            if exit_role == "primary":
                ctx.primary_done = True
            else:
                ctx.shadow_done = True

            if ctx.bypass_task is None:
                # 名额满被跳过: 没有旁路结果, 不产生一致性结论
                await save_record(ctx)
                await maybe_finish(ctx)
                return

            # 出口响应完整结束并已立即返回客户端, 不等待旁路
            finalize(rec, ctx)
            await save_record(ctx)  # 旁路仍在跑时先落 shadow_pending, 其退出后原地更新
            await maybe_finish(ctx)
        finally:
            if not ctx.exit_done:
                # 生成器被其他路径提前关闭: 兜底回收在途计数
                _abort_exit(
                    ctx, "primary_incomplete",
                    f"exit({ROLE_LABEL[exit_role]}) stream closed early",
                )

    return StreamingResponse(
        body_iter(),
        status_code=exit_resp.status,
        headers=filtered_headers(exit_resp),
        media_type=exit_resp.headers.get("Content-Type"),
        # 客户端在首个块之前就断开时生成器可能从未启动: 在响应收尾时兜底清理
        background=BackgroundTask(_close_exit_if_needed, ctx),
    )


# --------------------------------------------------------------------------
# 出口切换: 升主核验 / 立即切回
# --------------------------------------------------------------------------

def _quote_issue(ctx: CallContext) -> Optional[str]:
    """返回该报价不能作为升主依据的原因; 可计入时返回 None。"""
    rec = ctx.record
    result = rec.get("result", "pending")
    exit_role = ctx.exit_role

    if result in PENDING_RESULTS:
        return f"尚未完成({RESULT_TEXT.get(result, result)})"
    if result == "shadow_skipped_no_slot":
        return "旁路名额满被跳过, 没有双端比对结果, 不能计入"
    if result == "shadow_timeout":
        return "旁路超时未收完整, 不能计入"
    if result == "shadow_error":
        return f"旁路失败/断流({rec.get('detail') or rec.get(ctx.bypass_role) or ''}), 不能计入"
    if result == "client_disconnected":
        return "客户端断连, 不能计入"
    if result == "primary_error":
        return f"出口({ROLE_LABEL[exit_role]})连接失败, 不能计入"
    if result == "primary_incomplete":
        return f"出口({ROLE_LABEL[exit_role]})响应不完整, 不能计入"
    if result not in ("match", "mismatch"):
        return f"结果异常({result}), 不能计入"

    # 双端完整: 两路都必须收完整(跳过/断流/超时不会有完整摘要)
    for role in ("primary", "shadow"):
        if not _leg_completed(rec, role):
            return f"{ROLE_LABEL[role]}响应不完整, 不能计入"

    # 非 5xx: 两端都必须是 5xx 以外的状态码
    for role in ("primary", "shadow"):
        status = rec[f"{role}_status"]
        if status >= 500:
            return f"{ROLE_LABEL[role]}状态码 {status} 属于 5xx, 不能计入"

    if result != "match":
        return f"两端不一致({rec.get('detail') or RESULT_TEXT.get(result, result)}), 不能计入"
    return None


def _eval_promotion_locked(sw: SwitchState) -> tuple[list[str], list[CallContext]]:
    """在持有 switch.lock 时评估升主条件, 返回拒绝原因与最近 3 条台账。"""
    reasons: list[str] = []
    if sw.active > 0:
        reasons.append(f"仍有 {sw.active} 条报价或比对在途, 需全部结束后再升主")
    if sw.round_accepts < PROMOTE_MIN_QUOTES:
        reasons.append(
            f"当前第 {sw.round} 轮仅受理 {sw.round_accepts} 次报价, "
            f"至少需要 {PROMOTE_MIN_QUOTES} 次"
        )

    recent = list(sw.ledger)[-PROMOTE_MIN_QUOTES:]
    if len(recent) < PROMOTE_MIN_QUOTES:
        reasons.append(
            f"当前轮可用于核验的受理记录只有 {len(recent)} 条, "
            f"需要最近 {PROMOTE_MIN_QUOTES} 条"
        )
    for i, ctx in enumerate(recent, start=1):
        issue = _quote_issue(ctx)
        if issue:
            reasons.append(
                f"最近第 {i} 条(受理 #{ctx.seq}, 第 {ctx.round} 轮, "
                f"出口={ROLE_LABEL[ctx.exit_role]}): {issue}"
            )
    return reasons, recent


def _recent_summary(ctx: CallContext) -> dict:
    rec = ctx.record
    return {
        "seq": ctx.seq,
        "round": ctx.round,
        "exit": ctx.exit_role,
        "target": ctx.target,
        "result": rec.get("result"),
        "result_text": RESULT_TEXT.get(rec.get("result"), rec.get("result")),
        "primary_status": rec.get("primary_status"),
        "shadow_status": rec.get("shadow_status"),
        "issue": _quote_issue(ctx),
        "exit_role": ctx.exit_role,
        "bypass_role": ctx.bypass_role,
        "primary_sha256": rec.get("primary_sha256"),
        "shadow_sha256": rec.get("shadow_sha256"),
        "primary_ms": rec.get("primary_ms"),
        "shadow_ms": rec.get("shadow_ms"),
        "detail": rec.get("detail"),
    }


def _switch_payload_locked(sw: SwitchState, reasons: list[str],
                           recent: list[CallContext]) -> dict:
    return {
        "exit": sw.exit_role,
        "exit_label": ROLE_LABEL[sw.exit_role],
        "round": sw.round,
        "active": sw.active,
        "round_accepts": sw.round_accepts,
        "promote_min_quotes": PROMOTE_MIN_QUOTES,
        "eligible": not reasons,
        # 影子端已是出口时升主不适用(条件统计仍照常返回, 便于观察)
        "promote_applicable": sw.exit_role != "shadow",
        "reasons": reasons,
        "recent": [_recent_summary(ctx) for ctx in recent],
        "events": list(sw.events),
    }


def _log_event(sw: SwitchState, action: str, from_role: str, to_role: str,
               note: str) -> None:
    sw.events.appendleft({
        "at": now_ms(),
        "action": action,
        "from": from_role,
        "to": to_role,
        "round": sw.round,
        "note": note,
    })


@app.post("/api/switch/promote")
async def promote_exit():
    """把影子端升为报价出口; 不满足条件时 409 并逐条说明原因。"""
    sw: SwitchState = state["switch"]
    async with sw.lock:
        if sw.exit_role == "shadow":
            reasons = ["影子端已经是报价出口, 无需升主"]
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "action": "promote",
                    **_switch_payload_locked(sw, reasons, list(sw.ledger)[-PROMOTE_MIN_QUOTES:]),
                },
            )
        reasons, recent = _eval_promotion_locked(sw)
        if reasons:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "action": "promote",
                    **_switch_payload_locked(sw, reasons, recent),
                },
            )
        previous_round = sw.round
        sw.exit_role = "shadow"
        sw.round += 1
        sw.round_accepts = 0
        sw.ledger.clear()
        _log_event(sw, "promote", "primary", "shadow",
                   f"第 {previous_round} 轮最近 3 条报价核验通过")
        reasons, recent = _eval_promotion_locked(sw)
        return {
            "ok": True,
            "action": "promote",
            "changed": True,
            "previous_round": previous_round,
            **_switch_payload_locked(sw, reasons, recent),
        }


@app.post("/api/switch/fallback")
async def fallback_exit():
    """异常时立即切回旧端(主端): 不做任何核验, 在途请求保持原出口/轮次。"""
    sw: SwitchState = state["switch"]
    async with sw.lock:
        if sw.exit_role == "primary":
            reasons, recent = _eval_promotion_locked(sw)
            return {
                "ok": True,
                "action": "fallback",
                "changed": False,
                "note": "主端已经是报价出口",
                **_switch_payload_locked(sw, reasons, recent),
            }
        previous_round = sw.round
        sw.exit_role = "primary"
        sw.round += 1
        sw.round_accepts = 0
        sw.ledger.clear()
        _log_event(sw, "fallback", "shadow", "primary", "值班切回旧端, 无需核验")
        reasons, recent = _eval_promotion_locked(sw)
        return {
            "ok": True,
            "action": "fallback",
            "changed": True,
            "previous_round": previous_round,
            **_switch_payload_locked(sw, reasons, recent),
        }


@app.get("/api/switch/status")
async def switch_status():
    sw: SwitchState = state["switch"]
    async with sw.lock:
        reasons, recent = _eval_promotion_locked(sw)
        return _switch_payload_locked(sw, reasons, recent)


@app.get("/healthz")
async def healthz():
    lim: ShadowLimiter = state["limiter"]
    sw: SwitchState = state["switch"]
    return {
        "status": "ok",
        "primary": PRIMARY_URL,
        "shadow": SHADOW_URL,
        "exit": sw.exit_role,
        "round": sw.round,
        "active": sw.active,
        "round_accepts": sw.round_accepts,
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "shadow_timeout_s": SHADOW_TIMEOUT_S,
    }


@app.get("/api/records")
async def get_records():
    async with state["records_lock"]:
        return list(reversed(state["records"]))


@app.get("/api/state")
async def get_state():
    lim: ShadowLimiter = state["limiter"]
    sw: SwitchState = state["switch"]
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "exit": sw.exit_role,
        "round": sw.round,
        "active": sw.active,
        "round_accepts": sw.round_accepts,
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "records": len(state["records"]),
        "max_rss_mib": round(rss_kb / 1024, 2),
    }


# 报价相关 GET 请求走代理; 网页与管理 API 不拦截
@app.get("/quote")
@app.get("/quote-big")
async def quote_proxy(request: Request):
    target = request.url.path
    if request.url.query:
        target += "?" + request.url.query
    return await proxy(request, target)


app.mount("/", StaticFiles(
    directory=os.path.join(os.path.dirname(__file__), "web"), html=True),
    name="web")

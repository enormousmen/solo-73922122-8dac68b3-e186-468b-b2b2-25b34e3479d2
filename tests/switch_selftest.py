"""出口切换(升主/切回)关键语义自测: 真实 HTTP, 直接驱动网关。

前置: 三服务已启动, 且网关为初始状态(出口=主端, 第 1 轮)。
测试会改变出口状态, 结束时再切回主端。
"""
import asyncio
import hashlib
import json
import sys
import time

import aiohttp

GW = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8020"
PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


async def status(s):
    return await (await s.get(f"{GW}/api/switch/status")).json()


async def post(s, path):
    async with s.post(f"{GW}/api/switch/{path}") as r:
        return r.status, await r.json()


async def quote(s, qs):
    async with s.get(f"{GW}/quote?{qs}") as r:
        return r.status, await r.read()


async def wait_idle(s, timeout_s=8.0):
    """等待网关无在途报价/比对。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        st = await status(s)
        if st["active"] == 0:
            return st
        await asyncio.sleep(0.1)
    raise AssertionError(f"gateway still has {st['active']} in-flight")


async def wait_rec(s, marker, timeout_s=8.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        recs = await (await s.get(f"{GW}/api/records")).json()
        hit = [x for x in recs if marker in x["target"]]
        if hit and hit[0]["result"] not in (
                "pending", "shadow_pending", "shadow_completed_primary_incomplete"):
            return hit[0]
        await asyncio.sleep(0.1)
    raise AssertionError(f"no terminal record for {marker}")


async def clean_quotes(s, tag, n=3):
    for _ in range(n):
        await quote(s, f"marker={tag}-{time.time_ns()}")
    await wait_idle(s)


async def main():
    run = f"sw{int(time.time() * 1000) % 1000000}"
    async with aiohttp.ClientSession() as s:
        st0 = await status(s)
        if st0["exit"] != "primary":
            await post(s, "fallback")
        await wait_idle(s)
        st0 = await status(s)
        check("A 初始出口为主端", st0["exit"] == "primary", st0["exit"])
        rnd0 = st0["round"]

        # ---- B: 不足 3 次报价时拒绝, 并说明原因 ----
        st = await status(s)
        if st["round_accepts"] < 3:
            code, body = await post(s, "promote")
            check("B 不足3次报价拒绝升主", code == 409, str(code))
            check("B 拒绝说明受理次数不足",
                  any("仅受理" in x or "至少" in x for x in body["reasons"]),
                  str(body["reasons"]))

        # ---- C: fail=1 两端500, 不得计入 ----
        await quote(s, f"fail=1&marker={run}-c1")
        rec = await wait_rec(s, f"marker={run}-c1")
        check("C 两端500记录为match(状态码一致)",
              rec["result"] == "match" and rec["primary_status"] == 500, rec["result"])
        await clean_quotes(s, f"{run}-cprep", 2)
        code, body = await post(s, "promote")
        check("C 5xx 不能作为升主依据", code == 409, str(code))
        check("C 拒绝原因指出5xx",
              any("5xx" in x for x in body["reasons"]), str(body["reasons"]))

        # ---- D: 断流(旁路端)不能凑数 ----
        try:
            async with s.get(f"{GW}/quote-big?size=8388608&s_abort_after=300000"
                             f"&marker={run}-d1") as r:
                await r.read()
        except Exception:
            pass
        rec = await wait_rec(s, f"marker={run}-d1")
        check("D 旁路断流被标记为 shadow_error", rec["result"] == "shadow_error",
              rec["result"])
        await clean_quotes(s, f"{run}-dprep", 2)
        code, body = await post(s, "promote")
        check("D 断流记录不能凑数", code == 409, str(code))
        check("D 拒绝原因指出断流/失败",
              any("断流" in x or "失败" in x or "不完整" in x for x in body["reasons"]),
              str(body["reasons"]))

        # ---- E: 跳过(名额满)不能凑数 ----
        async def holder(i):
            async with aiohttp.ClientSession() as cs:
                async with cs.get(f"{GW}/quote?s_delay=1.2&marker={run}-e-hold{i}") as r:
                    return r.status
        holders = [asyncio.create_task(holder(i)) for i in range(4)]
        await asyncio.sleep(0.15)
        await asyncio.gather(*(quote(s, f"marker={run}-e-skip{i}") for i in range(3)))
        await asyncio.gather(*holders)
        st = await wait_idle(s)
        recent = st["recent"]
        check("E 最近3条均为名额满跳过",
              len(recent) == 3 and all(x["issue"] and "跳过" in x["issue"]
                                       for x in recent),
              str([x["issue"] for x in recent]))
        code, body = await post(s, "promote")
        check("E 跳过不能凑数", code == 409, str(code))
        check("E 拒绝原因指出跳过",
              any("跳过" in x for x in body["reasons"]), str(body["reasons"]))

        # ---- F: 超时不能凑数 ----
        await quote(s, f"s_delay=3&marker={run}-f1")
        rec = await wait_rec(s, f"marker={run}-f1")
        check("F 旁路超时记录", rec["result"] == "shadow_timeout", rec["result"])
        await clean_quotes(s, f"{run}-fprep", 2)
        code, body = await post(s, "promote")
        check("F 超时不能凑数", code == 409, str(code))
        check("F 拒绝原因指出超时",
              any("超时" in x for x in body["reasons"]), str(body["reasons"]))

        # ---- G: 在途时拒绝升主 ----
        await clean_quotes(s, f"{run}-gprep", 3)
        # 一条慢的出口响应 (1.2s) 占住"在途", 期间发起升主
        slow_task = asyncio.create_task(quote(s, f"p_delay=1.2&marker={run}-g1"))
        await asyncio.sleep(0.2)
        code, body = await post(s, "promote")
        check("G 有在途报价时拒绝升主", code == 409, str(code))
        check("G 拒绝原因指出在途",
              any("在途" in x for x in body["reasons"]), str(body["reasons"]))
        await slow_task
        await wait_idle(s)

        # ---- H: 最近 3 条都合格才升主, 轮次 +1 ----
        await clean_quotes(s, f"{run}-h-ok", 3)
        st = await status(s)
        check("H 升主前状态可升主", st["eligible"] is True, str(st["reasons"]))
        code, body = await post(s, "promote")
        check("H 升主成功", code == 200 and body["exit"] == "shadow", str(code))
        check("H 轮次+1", body["round"] == rnd0 + 1, f'{body["round"]} vs {rnd0}')
        check("H 记下切换事件", bool(body["events"]) and body["events"][0]["action"] == "promote")
        new_round = body["round"]

        # ---- I: 升主后业务字节来自影子端 ----
        _, data = await quote(s, f"marker={run}-i1")
        rec = await wait_rec(s, f"marker={run}-i1")
        check("I 出口为影子端", rec["exit"] == "shadow", rec["exit"])
        check("I 影子端字节被原样透传",
              rec["shadow_sha256"] == hashlib.sha256(data).hexdigest())
        check("I 记录轮次为新轮", rec["round"] == new_round, str(rec["round"]))
        _, diff_body = await quote(s, f"diff=1&marker={run}-i2")
        check("I 升主后 diff 差异出现在出口响应中",
              json.loads(diff_body).get("shadow_diff") is True, diff_body[:80])

        # ---- J: 切回无需核验 + 非 5xx 状态码可升主 ----
        code, body = await post(s, "fallback")
        check("J 切回成功且无需核验", code == 200 and body["exit"] == "primary", str(code))
        check("J 切回也推进轮次", body["round"] == new_round + 1, str(body["round"]))
        await clean_quotes(s, f"{run}-j", 3)
        code, body = await post(s, "promote")
        check("J 非5xx状态码一致可升主", code == 200, str(code) + str(body["reasons"]))
        await post(s, "fallback")

    # ---- K/L 需要独立网关状态, 用新的 ClientSession 继续 ----
    async with aiohttp.ClientSession() as s:
        await wait_idle(s)
        await clean_quotes(s, f"{run}-kprep", 3)
        old_round = (await status(s))["round"]

        async def slow_exit(ms, marker):
            async with aiohttp.ClientSession() as cs:
                async with cs.get(f"{GW}/quote-big?size=4194304&chunk_ms={ms}"
                                  f"&marker={marker}") as r:
                    return r.status, await r.read()

        task = asyncio.create_task(slow_exit(30, f"{run}-k1"))
        await asyncio.sleep(0.15)
        code, body = await post(s, "promote")
        check("K 有在途时不允许升主", code == 409, str(code) + str(body.get("reasons")))
        st_after = await status(s)
        check("K 拒绝后出口未变", st_after["exit"] == "primary", st_after["exit"])
        status_code, data = await task
        rec = await wait_rec(s, f"marker={run}-k1")
        check("K 在途请求记录旧出口/旧轮次",
              rec["exit"] == "primary" and rec["round"] == old_round,
              f'{rec["exit"]}/{rec["round"]} old={old_round}')
        check("K 在途响应未被影子字节污染",
              rec["primary_sha256"] == hashlib.sha256(data).hexdigest())

        # 在途结束后升主, 并与新请求并发
        await clean_quotes(s, f"{run}-kprep2", 3)
        old_round = (await status(s))["round"]
        task = asyncio.create_task(slow_exit(20, f"{run}-k2"))
        await asyncio.sleep(0.15)
        # 此时该请求在途, 升主必须被拒绝
        code, body = await post(s, "promote")
        check("K 在途比对期间升主被拒绝", code == 409, str(code))
        # 等这条在途结束再升主
        await task
        await wait_idle(s)
        code, body = await post(s, "promote")
        check("K 在途结束后升主成功", code == 200, str(code) + str(body.get("reasons")))
        new_round = body["round"]
        rec = await wait_rec(s, f"marker={run}-k2")
        check("K 切换前受理的请求仍属旧轮/旧出口",
              rec["exit"] == "primary" and rec["round"] == old_round,
              f'{rec["exit"]}/{rec["round"]}')
        _, data2 = await quote(s, f"marker={run}-k3")
        rec2 = await wait_rec(s, f"marker={run}-k3")
        check("K 切换后新请求用新出口/新轮次",
              rec2["exit"] == "shadow" and rec2["round"] == new_round,
              f'{rec2["exit"]}/{rec2["round"]}')

        # ---- L: 旧轮迟到的结果不能充作新轮升主依据 ----
        old_exit_round = (await status(s))["round"]
        late = asyncio.create_task(slow_exit(25, f"{run}-l1"))
        await asyncio.sleep(0.1)
        await post(s, "fallback")
        r_after = (await status(s))["round"]
        check("L 切回推进轮次并清空台账", r_after == old_exit_round + 1, str(r_after))
        await late
        await wait_idle(s)
        rec = await wait_rec(s, f"marker={run}-l1")
        check("L 迟到记录保留旧轮次", rec["round"] == old_exit_round,
              f'{rec["round"]} vs {old_exit_round}')
        st = await status(s)
        seqs = [x["seq"] for x in st["recent"]]
        check("L 迟到记录未进入新轮台账", rec["seq"] not in seqs,
              f'seq={rec["seq"]} ledger={seqs}')
        check("L 迟到记录未抬高新轮受理数", st["round_accepts"] < 3,
              str(st["round_accepts"]))
        code, body = await post(s, "promote")
        check("L 旧轮迟到结果不能升主", code == 409, str(code))
        check("L 拒绝原因说明受理次数不足",
              any("仅受理" in x or "至少" in x for x in body["reasons"]),
              str(body["reasons"]))

        # 收尾: 回到主端
        await post(s, "fallback")
        await clean_quotes(s, f"{run}-end", 3)

    print(f"\n汇总: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("失败项:", FAIL)
        sys.exit(1)


asyncio.run(main())

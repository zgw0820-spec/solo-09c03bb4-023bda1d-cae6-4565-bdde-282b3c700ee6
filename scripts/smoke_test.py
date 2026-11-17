#!/usr/bin/env python3
"""End-to-end smoke test for the edge gateway.

Usage:
  smoke_test.py BASE_URL SAVE_FILE phase1   # address-replacement interleaves,
                                            # switch semantics, arms an interrupted request
  smoke_test.py BASE_URL SAVE_FILE phase2   # post-restart recovery checks

Env:
  GW2_BASE    base URL of the second gateway sharing the same database
              (default http://127.0.0.1:8001)
  ALPHA2_URL  URL of the replacement node for alpha (default :9003)
  UP_A_URL    URL of the alpha node itself, for admin hooks (default :9001)
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAVE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/edge-gw-phase1.json"
PHASE = sys.argv[3] if len(sys.argv) > 3 else "phase1"

BASE2 = os.environ.get("GW2_BASE", "http://127.0.0.1:8001")
ALPHA2_URL = os.environ.get("ALPHA2_URL", "http://127.0.0.1:9003")
UP_A_URL = os.environ.get("UP_A_URL", "http://127.0.0.1:9001")

FAILURES: list[str] = []


def _req(method, url, body=None, timeout=70):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, method=method, data=data, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def api(method, path, body=None, timeout=70):
    return _req(method, BASE + path, body, timeout)


def api2(method, path, body=None, timeout=70):
    """Call the *other* gateway instance sharing the same database."""
    return _req(method, BASE2 + path, body, timeout)


def check(name, cond, extra=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def inflight_of(state, name):
    for u in state["upstreams"]:
        if u["name"] == name:
            return u["inflight"]
    return 0


def upstream_of(state, name):
    return next(u for u in state["upstreams"] if u["name"] == name)


def wait_state(pred, what, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, st = api("GET", "/api/state")
        if pred(st):
            return st
        time.sleep(0.1)
    check(f"等待状态: {what}", False)
    return api("GET", "/api/state")[1]


def brief(body):
    return json.dumps(body, ensure_ascii=False)[:220]


def slow_call(store, delay_ms):
    try:
        s, b = api("POST", "/api/call", {"delay_ms": delay_ms})
        store["status"], store["body"] = s, b
    except Exception as e:  # noqa: BLE001
        store["error"] = str(e)


def phase1():
    _, st = api("GET", "/api/state")
    check("初始当前目标为 alpha", st["route"]["current_target"] == "alpha")
    check("初始版号为 0", st["route"]["version"] == 0)
    check("初始无在途请求", len(st["inflight"]) == 0)
    alpha_url = upstream_of(st, "alpha")["url"]

    # ---- A. 接单与地址替换交错 ----
    # A1) slow request admitted while alpha still points at the OLD address
    slow = {}
    t = threading.Thread(target=slow_call, args=(slow, 3000), daemon=True)
    t.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途变为 1")

    # A2) the OTHER gateway instance replaces alpha's address mid-flight
    s, u = api2("POST", "/api/upstreams/update", {"name": "alpha", "url": ALPHA2_URL})
    check("另一实例替换 alpha 地址成功(版号+1)",
          s == 200 and u.get("ok") is True and u.get("version") == 1, brief(u))

    # A3) requests admitted after the update go to the NEW address
    s, r = api("POST", "/api/call", {"delay_ms": 30})
    check("更新后接单走新地址(alpha2 节点)",
          r.get("target") == "alpha" and r.get("url") == ALPHA2_URL
          and r.get("upstream", {}).get("upstream") == "alpha2", brief(r))

    # A4) the pre-update admission still completes from the OLD address
    t.join(timeout=15)
    check("慢请求已结束", "body" in slow, slow.get("error", ""))
    body = slow.get("body", {})
    check("更新前接单仍访问旧地址(alpha 节点)",
          body.get("ok") is True and body.get("url") == alpha_url
          and body.get("upstream", {}).get("upstream") == "alpha", brief(body))

    # A5) old address fails mid-flight -> error, never a silent re-route
    s, u = api2("POST", "/api/upstreams/update", {"name": "alpha", "url": alpha_url})
    check("alpha 地址改回旧节点", s == 200 and u.get("version") == 2, brief(u))
    slow2 = {}
    t2 = threading.Thread(target=slow_call, args=(slow2, 3000), daemon=True)
    t2.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途再次变为 1")
    s, u = api2("POST", "/api/upstreams/update", {"name": "alpha", "url": ALPHA2_URL})
    check("在途期间另一实例再次换地址", s == 200 and u.get("version") == 3, brief(u))
    _req("POST", UP_A_URL + "/admin/work_mode", {"fail": True})  # 旧节点开始故障
    t2.join(timeout=15)
    _req("POST", UP_A_URL + "/admin/work_mode", {"fail": False})
    body2 = slow2.get("body", {})
    check("旧地址故障只报错，不偷偷改投新地址",
          body2.get("ok") is False and body2.get("status") == 503
          and body2.get("url") == alpha_url
          and body2.get("upstream", {}).get("upstream") == "alpha", brief(body2))

    # A6) an unreachable address yields 502 at that address, nothing else
    s, u = api2("POST", "/api/upstreams/update",
                {"name": "alpha", "url": "http://127.0.0.1:9"})
    check("替换为不可达地址", s == 200 and u.get("version") == 4, brief(u))
    s, r = api("POST", "/api/call", {"delay_ms": 30})
    check("不可达地址返回 502 且不另投他处",
          s == 502 and r.get("target") == "alpha"
          and r.get("url") == "http://127.0.0.1:9", brief(r))
    s, u = api2("POST", "/api/upstreams/update", {"name": "alpha", "url": alpha_url})
    check("恢复 alpha 地址", s == 200 and u.get("version") == 5, brief(u))
    s, r = api("POST", "/api/call", {"delay_ms": 30})
    check("恢复后请求正常",
          r.get("ok") is True and r.get("upstream", {}).get("upstream") == "alpha",
          brief(r))

    # A7) validation and idempotence
    s, u = api("POST", "/api/upstreams/update",
               {"name": "nosuch", "url": "http://127.0.0.1:9"})
    check("未知上游更新返回 404", s == 404 and u.get("error") == "unknown_target", brief(u))
    s, u = api("POST", "/api/upstreams/update", {"name": "alpha", "url": "ftp://bad"})
    check("非法地址返回 400", s == 400 and u.get("error") == "bad_url", brief(u))
    beta_url = upstream_of(api("GET", "/api/state")[1], "beta")["url"]
    s, u = api("POST", "/api/upstreams/update", {"name": "beta", "url": beta_url})
    check("地址未变的更新为空操作(版号不变)",
          s == 200 and u.get("unchanged") is True and u.get("version") == 5, brief(u))

    # ---- B. 切换语义（在地址替换之后继续，版号动态读取） ----
    _, st = api("GET", "/api/state")
    v = st["route"]["version"]
    check("地址替换后当前目标仍为 alpha", st["route"]["current_target"] == "alpha")

    # B1) slow request admitted to alpha, keeps running through the switch
    slow3 = {}
    t3 = threading.Thread(target=slow_call, args=(slow3, 4000), daemon=True)
    t3.start()
    wait_state(lambda s: inflight_of(s, "alpha") == 1, "alpha 在途变为 1")

    # B2) before any switch, a fast call still goes to alpha
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换前快请求走 alpha",
          r["target"] == "alpha" and r["upstream"]["upstream"] == "alpha", brief(r))

    # B3) probe beta; while/after probing (before commit) traffic stays on alpha
    s, p = api("POST", "/api/switch/probe", {"target": "beta", "base_version": v})
    check("探测 beta 成功", p["ok"] is True and p["version_matches"] is True, brief(p))
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("探测后未确认前请求仍走 alpha", r["target"] == "alpha", brief(r))
    _, st = api("GET", "/api/state")
    check("探测不改变路由",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == v)

    # B4) commit the switch
    s, c = api("POST", "/api/switch/commit",
               {"target": "beta", "base_version": v, "probe_id": p["probe_id"]})
    check("确认切换成功，版号+1", c.get("ok") is True and c.get("version") == v + 1, brief(c))

    # B5) new requests immediately go to beta, no waiting for the old one
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("切换后新请求立即走 beta",
          r["target"] == "beta" and r["upstream"]["upstream"] == "beta", brief(r))
    _, st = api("GET", "/api/state")
    check("旧目标 alpha 仍有 1 条在途（未等待其结束）", inflight_of(st, "alpha") == 1)
    check("alpha 在途未清零，不可下线", upstream_of(st, "alpha")["can_offline"] is False)

    # B6) stale commit (page version outdated) must not override the effective route
    s, p_stale = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": v})
    check("过期版号的探测被标记 version_matches=false",
          p_stale["ok"] is True and p_stale["version_matches"] is False, brief(p_stale))
    s, c_stale = api("POST", "/api/switch/commit",
                     {"target": "alpha", "base_version": v, "probe_id": p_stale["probe_id"]})
    check("过期版号的确认被拒绝(409 version_conflict)",
          s == 409 and c_stale["error"] == "version_conflict", brief(c_stale))
    _, st = api("GET", "/api/state")
    check("路由未被迟到提交覆盖",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == v + 1)

    # B7) failed probe keeps the original route
    s, pd = api("POST", "/api/switch/probe", {"target": "dead", "base_version": v + 1})
    check("探测不可达候选失败", pd["ok"] is False, brief(pd))
    _, st = api("GET", "/api/state")
    check("探测失败后维持原路由",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == v + 1)
    s, cd = api("POST", "/api/switch/commit",
                {"target": "dead", "base_version": v + 1, "probe_id": pd["probe_id"]})
    check("失败探测对应的确认被拒绝", s == 400 and cd["error"] == "probe_invalid", brief(cd))

    # B8) two interleaved switches on the same base version: exactly one wins
    s, px = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": v + 1})
    s, py = api("POST", "/api/switch/probe", {"target": "alpha", "base_version": v + 1})
    s, cx = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": v + 1, "probe_id": px["probe_id"]})
    check("交错切换：先到的确认生效", cx.get("ok") is True and cx.get("version") == v + 2, brief(cx))
    s, cy = api("POST", "/api/switch/commit",
                {"target": "alpha", "base_version": v + 1, "probe_id": py["probe_id"]})
    check("交错切换：迟到的确认被版号拒绝", s == 409 and cy["error"] == "version_conflict", brief(cy))

    # B9) late probe must not overwrite an already-effective route
    s, pz = api("POST", "/api/switch/probe", {"target": "beta", "base_version": v + 2})
    check("探测 beta 成功", pz["ok"] is True, brief(pz))
    s, pz2 = api("POST", "/api/switch/probe", {"target": "beta", "base_version": v + 2})
    s, cz2 = api("POST", "/api/switch/commit",
                 {"target": "beta", "base_version": v + 2, "probe_id": pz2["probe_id"]})
    check("另一值班员先完成切换", cz2.get("ok") is True and cz2.get("version") == v + 3, brief(cz2))
    s, cz = api("POST", "/api/switch/commit",
                {"target": "beta", "base_version": v + 2, "probe_id": pz["probe_id"]})
    check("迟到探测的确认不得覆盖已生效路由", s == 409 and cz["error"] == "version_conflict", brief(cz))
    _, st = api("GET", "/api/state")
    check("路由保持最新",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == v + 3)

    # B10) the slow pre-switch request completes with the full response from alpha
    t3.join(timeout=15)
    check("慢请求已结束", "body" in slow3, slow3.get("error", ""))
    body3 = slow3.get("body", {})
    check("慢请求从原上游 alpha 取回完整响应",
          body3.get("ok") is True and body3.get("url") == alpha_url
          and body3.get("upstream", {}).get("upstream") == "alpha"
          and body3.get("upstream", {}).get("delay_ms") == 4000, brief(body3))

    # B11) alpha drains to zero -> only then may it be taken offline
    wait_state(lambda s: inflight_of(s, "alpha") == 0, "alpha 在途清零")
    _, st = api("GET", "/api/state")
    check("alpha 在途清零后提示可下线", upstream_of(st, "alpha")["can_offline"] is True)
    check("当前目标 beta 不标记可下线", upstream_of(st, "beta")["can_offline"] is False)

    # B12) leave alpha at a non-env address: phase2 proves the registration
    # survives the restart instead of being silently reverted to env values
    s, u = api2("POST", "/api/upstreams/update", {"name": "alpha", "url": ALPHA2_URL})
    check("终态：alpha 指向替换节点", s == 200 and u.get("ok") is True, brief(u))

    # B13) arm one in-flight request that the harness will interrupt with kill -9
    def interrupted():
        try:
            api("POST", "/api/call", {"delay_ms": 15000})
        except Exception:  # noqa: BLE001 - connection reset when gateway dies
            pass

    threading.Thread(target=interrupted, daemon=True).start()
    wait_state(lambda s: inflight_of(s, "beta") == 1, "重启前 beta 在途为 1")
    _, st = api("GET", "/api/state")
    with open(SAVE, "w") as f:
        json.dump(
            {"target": st["route"]["current_target"], "version": st["route"]["version"]}, f
        )
    print(f"PHASE1 READY: 已在 beta 上挂起 1 条在途请求，等待网关被杀 "
          f"(目标 {st['route']['current_target']}@v{st['route']['version']})")

    # keep the client connection open until the gateway is killed
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            api("GET", "/api/state", timeout=2)
            time.sleep(0.2)
        except Exception:  # noqa: BLE001
            print("PHASE1 OK: 网关已停止")
            return
    check("等待网关被杀", False)


def phase2():
    with open(SAVE) as f:
        saved = json.load(f)
    _, st = api("GET", "/api/state")
    check("重启后按已登记路由接流量(目标不变)", st["route"]["current_target"] == saved["target"])
    check("重启后路由版号保持", st["route"]["version"] == saved["version"])
    check("中断请求未留作在途", len(st["inflight"]) == 0)
    check("启动时清理了中断在途记录", st["boot"]["cleared_inflight"] >= 1)
    check("地址替换结果在重启后保留(未被 env 还原)",
          upstream_of(st, "alpha")["url"] == ALPHA2_URL,
          upstream_of(st, "alpha")["url"])
    s, r = api("POST", "/api/call", {"delay_ms": 50})
    check("重启后新请求正常发往已登记目标",
          r["target"] == saved["target"] and r["ok"] is True, brief(r))


if PHASE == "phase1":
    phase1()
else:
    phase2()

if FAILURES:
    print(f"\n{len(FAILURES)} 项检查失败:")
    for f in FAILURES:
        print(" -", f)
    sys.exit(1)
print(f"\n全部检查通过 ({PHASE})")

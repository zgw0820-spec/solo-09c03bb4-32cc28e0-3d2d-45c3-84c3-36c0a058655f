#!/usr/bin/env python3
"""撞号隔离端到端试跑（与 tests/e2e_test.py 共用 local_pg.sh 起的双库）。

覆盖需求点：
  A. 演示数据：旧库同一事务两张工单，其中一张在新库撞号；
  B. 撞号时旧库整笔事务在订阅端原子卡住（第二张也不可见），apply worker 报错重试；
  C. 隔离前置条件（旧库同号、新库本地试写、状态机 OLD、版本令牌）；
  D. 隔离两步一事务：原件入隔离表、工单表移除；失败回滚；
  E. 重复确认幂等：即使订阅已回填同号工单，仍返回原隔离结果且不改动回填行；
  F. 订阅开关全程不被隔离操作改变，启用时 PostgreSQL 自动重试、整笔入库；
  G. 试写 API 与非 OLD 状态门槛。

前置：./scripts/local_pg.sh start 且 ./scripts/local_app.sh 已启动。用 -I 运行。
"""
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg2

BASE = "http://127.0.0.1:8000"
OLD_DSN = "host=127.0.0.1 port=55433 user=node dbname=inspection"
NEW_DSN = "host=127.0.0.1 port=55434 user=node dbname=inspection"
ROOT = Path(__file__).resolve().parents[1]
STATE_DB = ROOT / ".localrun" / "data" / "handover_state.db"
NEW_LOG = ROOT / ".localrun" / "logs" / "new.log"

TRIAL_ID = 2000
OLD_IDS = (2000, 2001)

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        mark = "PASS"
    else:
        FAIL += 1
        mark = "FAIL"
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))


def api(method: str, path: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"error": raw.decode("utf-8", "replace")}


def sql(dsn: str, query: str, args=()):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(query, args)
        if cur.description:
            return cur.fetchall()
        conn.commit()


def get_row(dsn: str, table: str, wo_id: int):
    rows = sql(dsn, f"SELECT id, title, detail, source_db, created_at FROM {table} WHERE id=%s", (wo_id,))
    return rows[0] if rows else None


def set_state(state: str) -> None:
    conn = sqlite3.connect(STATE_DB)
    conn.execute("UPDATE kv SET value=? WHERE key='state'", (state,))
    conn.commit()
    conn.close()


def wait_for(predicate, timeout: float, what: str):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.5)
    raise AssertionError(f"等待超时: {what}（last={last}）")


def sub_enabled() -> bool:
    rows = sql(NEW_DSN, "SELECT subenabled FROM pg_subscription WHERE subname='inspection_sub'")
    return bool(rows and rows[0][0])


def cleanup_footprint() -> None:
    """清理上一轮（含崩溃中断的）测试痕迹，使本测试可在同一套库上重复运行。

    上一轮撞号事务可能仍在被 apply worker 持续重试；先暂停订阅，
    否则删掉试写件的瞬间重试会立即把旧库 2000/2001 回填，清理不净。
    """
    set_state("OLD")
    sql(NEW_DSN, "ALTER SUBSCRIPTION inspection_sub DISABLE")
    sql(NEW_DSN, """
        DROP TRIGGER IF EXISTS trg_fail_delete ON work_orders;
        DROP FUNCTION IF EXISTS fail_delete();
        DELETE FROM work_orders_quarantine WHERE id IN (2000, 3000);
        DELETE FROM work_orders WHERE id IN (2000, 2001, 3000);
    """)
    # 旧库删除会经逻辑复制同步到新库；直接删旧库同号行，保证 seed 可重新整事务提交
    sql(OLD_DSN, "DELETE FROM work_orders WHERE id IN (2000, 2001)")
    sql(NEW_DSN, "ALTER SUBSCRIPTION inspection_sub ENABLE")
    # 等订阅 worker 重新进入 streaming，避免恢复瞬间的状态抖动影响后续断言
    deadline = time.time() + 15
    while time.time() < deadline:
        rows = sql(
            OLD_DSN,
            "SELECT state FROM pg_stat_replication WHERE application_name='inspection_sub'",
        )
        if rows and rows[0][0] == "streaming":
            break
        time.sleep(0.5)


def main() -> int:
    print("== 0. 准备：清理上轮痕迹，状态拨回 OLD，订阅保持启用 ==")
    cleanup_footprint()
    wait_for(lambda: api("GET", "/api/status")[1]["state"] == "OLD", 5, "状态为 OLD")
    check("订阅初始为启用状态", sub_enabled() is True)

    print("== 1. 试写 API：新库写本地试写件（独立编号，不参与复制）==")
    code, body = api("POST", "/api/trial-work-orders",
                     {"title": "接口试写件", "detail": "通过网页 API 写入"})
    check("试写件写入新库成功", code == 200 and body["trial_work_order"]["source_db"] == "new-trial",
          str(body))
    trial_api_id = body["trial_work_order"]["id"]
    check("试写响应带版本令牌", len(body["confirm_token"]) == 64)
    check("试写件只在新库、旧库没有（不参与复制）",
          get_row(OLD_DSN, "work_orders", trial_api_id) is None
          and get_row(NEW_DSN, "work_orders", trial_api_id) is not None)

    print("== 2. 灌入演示数据：新库试写件 2000 + 旧库同事务 2000/2001 ==")
    code, body = api("POST", "/api/demo/seed-collision")
    check("演示数据灌入成功", code == 200, str(body))
    trial_row = get_row(NEW_DSN, "work_orders", TRIAL_ID)
    check("新库 2000 是本地试写件", trial_row is not None and trial_row[3] == "new-trial")
    old_rows = sql(OLD_DSN, "SELECT id FROM work_orders WHERE id = ANY(%s) ORDER BY id", (list(OLD_IDS),))
    check("旧库同事务两张工单均已提交", [r[0] for r in old_rows] == list(OLD_IDS))
    # 幂等：再灌一次应跳过
    code, body2 = api("POST", "/api/demo/seed-collision")
    check("演示数据接口幂等（重复灌入跳过）", code == 200 and len(body2["skipped"]) >= 2, str(body2))

    print("== 3. 撞号导致旧库整笔事务在订阅端原子卡住 ==")
    time.sleep(2.0)  # 给 apply worker 足够时间撞到冲突
    check("新库没有 2001：同事务第二张连带卡住（整笔原子）",
          get_row(NEW_DSN, "work_orders", OLD_IDS[1]) is None)
    check("新库 2000 仍是试写件（旧库行被挡住，未回填）",
          (get_row(NEW_DSN, "work_orders", TRIAL_ID) or [None] * 4)[3] == "new-trial")
    log_tail = NEW_LOG.read_text(encoding="utf-8", errors="replace")[-4000:]
    check("新库日志出现主键冲突与事务重试记录",
          "duplicate key value" in log_tail and "2000" in log_tail,
          "见 .localrun/logs/new.log")

    print("== 4. 交接页对照：两库原文与前置条件 ==")
    code, body = api("GET", f"/api/quarantine/compare?work_order_id={TRIAL_ID}")
    check("对照接口 200 且判定可隔离", code == 200 and body["eligible"] is True, str(body))
    check("对照返回旧库同号工单原文", body["old"] and body["old"]["id"] == TRIAL_ID
          and body["old"]["source_db"] == "old")
    check("对照返回新库试写件原文与版本令牌",
          body["new_trial"] and body["new_trial"]["source_db"] == "new-trial"
          and body["confirm_token"])
    token = body["confirm_token"]
    code, body_missing = api("GET", "/api/quarantine/compare?work_order_id=999999")
    check("旧库无同号工单时判定不可隔离并给出原因",
          code == 200 and body_missing["eligible"] is False
          and any("旧库不存在" in r for r in body_missing["reasons"]))

    print("== 5. 前置条件不满足时拒绝隔离（数据保持不变）==")
    # 5a. 版本令牌错误
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": "deadbeef" * 8})
    check("令牌错误返回 409 且试写件未被移动",
          code == 409 and get_row(NEW_DSN, "work_orders", TRIAL_ID) is not None
          and get_row(NEW_DSN, "work_orders_quarantine", TRIAL_ID) is None, str(body))
    # 5b. 新库同号不是试写件（构造一个 source_db 非 new-trial 的行）
    sql(NEW_DSN, "INSERT INTO work_orders(id,title,detail,source_db) VALUES (3000,'非试写件','','old')")
    code, body = api("GET", "/api/quarantine/compare?work_order_id=3000")
    check("非试写件同号记录判定不可隔离", body["eligible"] is False
          and any("不是本地试写件" in r for r in body["reasons"]))
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": 3000, "confirm_token": body.get("confirm_token") or "x"})
    check("非试写件确认隔离返回 409", code == 409, str(body))
    # 5c. 非 OLD 状态门槛
    set_state("SWITCHING")
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": token})
    check("SWITCHING 状态拒绝隔离（409）", code == 409, str(body))
    code, body = api("POST", "/api/trial-work-orders", {"title": "状态门槛试写"})
    check("非 OLD 状态拒绝试写（409）", code == 409)
    set_state("OLD")

    print("== 6. 库内两步原子性的失败回滚验证（直接 SQL 模拟第二步失败）==")
    # 人为给 work_orders 加一个触发异常的行级触发器，使 DELETE 失败，
    # 验证“插入隔离表 + 删除工单表”任一失败时整体回滚、两边都不留痕。
    sql(NEW_DSN, """
        CREATE OR REPLACE FUNCTION fail_delete() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'injected failure for rollback test'; END; $$;
        DROP TRIGGER IF EXISTS trg_fail_delete ON work_orders;
        CREATE TRIGGER trg_fail_delete BEFORE DELETE ON work_orders
          FOR EACH ROW WHEN (OLD.id = %s) EXECUTE FUNCTION fail_delete();
    """, (TRIAL_ID,))
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": token})
    check("第二步失败时接口返回错误且已回滚", code in (409, 500)
          and "injected failure" in str(body), str(body))
    check("回滚后工单表试写件仍在", get_row(NEW_DSN, "work_orders", TRIAL_ID) is not None)
    check("回滚后隔离表无残留", get_row(NEW_DSN, "work_orders_quarantine", TRIAL_ID) is None)
    sql(NEW_DSN, "DROP TRIGGER trg_fail_delete ON work_orders; DROP FUNCTION fail_delete();")

    print("== 7. 正式确认隔离：两步一事务提交 ==")
    enabled_before = sub_enabled()
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": token})
    check("隔离成功返回 200", code == 200 and body["ok"] is True and body["idempotent"] is False,
          str(body))
    check("响应声明订阅开关未被改变",
          body["subscription_changed"] is False
          and body["subscription_enabled_before"] == enabled_before
          and body["subscription_enabled_after"] == enabled_before)
    check("隔离后订阅开关实际仍为原状态", sub_enabled() == enabled_before)
    check("试写件已移出工单表", get_row(NEW_DSN, "work_orders", TRIAL_ID) is None)
    q = get_row(NEW_DSN, "work_orders_quarantine", TRIAL_ID)
    check("隔离表保存的原件五列与试写件原样一致",
          q is not None and q[0] == trial_row[0] and q[1] == trial_row[1]
          and q[2] == trial_row[2] and q[3] == trial_row[3] and q[4] == trial_row[4],
          f"trial={trial_row} quarantine={q}")

    print("== 8. PostgreSQL 自动重试，旧库整笔事务入库（2000 与 2001 同时可见）==")
    def both_landed():
        rows = sql(NEW_DSN, "SELECT id, source_db FROM work_orders WHERE id = ANY(%s) ORDER BY id",
                   (list(OLD_IDS),))
        return rows if len(rows) == 2 and all(r[1] == "old" for r in rows) else None
    landed = wait_for(both_landed, 30, "2000/2001 整笔入库")
    check("整笔事务重试成功：新库 2000、2001 均为旧库来源", landed == [(2000, "old"), (2001, "old")])
    check("隔离表中的 2000 原件不受回填影响",
          get_row(NEW_DSN, "work_orders_quarantine", TRIAL_ID) is not None)

    print("== 9. 重送同一次确认：幂等返回原隔离结果（订阅已回填同号工单）==")
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": token})
    check("重复确认返回 200 且标记 idempotent/same_confirmation",
          code == 200 and body["ok"] is True and body["idempotent"] is True
          and body["same_confirmation"] is True, str(body))
    check("重复确认回显原隔离原件且编号一致",
          body["quarantine"]["id"] == TRIAL_ID
          and body["quarantine"]["title"] == trial_row[1])
    check("回填的旧库 2000 工单未被删除或覆盖",
          get_row(NEW_DSN, "work_orders", TRIAL_ID)[3] == "old")
    check("隔离表仍只有一条 2000（未重复插入）",
          sql(NEW_DSN, "SELECT count(*) FROM work_orders_quarantine WHERE id=%s", (TRIAL_ID,))[0][0] == 1)
    # 带不同令牌重复确认同样幂等（按工单号回显原结果）
    code, body = api("POST", "/api/quarantine/confirm",
                     {"work_order_id": TRIAL_ID, "confirm_token": "0" * 64})
    check("异令牌重复确认仍幂等回显（same_confirmation=false）",
          code == 200 and body["idempotent"] is True and body["same_confirmation"] is False)
    code, body = api("GET", f"/api/quarantine/compare?work_order_id={TRIAL_ID}")
    check("对照接口对已隔离工单号回显 already_quarantined",
          body["already_quarantined"] is True and body["quarantine"]["id"] == TRIAL_ID)

    print("== 10. 隔离清单接口与事件日志 ==")
    code, body = api("GET", "/api/quarantine/list")
    ids = [r["id"] for r in body["quarantined"]]
    check("隔离清单包含 2000 原件", code == 200 and TRIAL_ID in ids)
    events = api("GET", "/api/events")[1]
    check("事件日志记录隔离成功与幂等重放",
          any("已原样隔离" in e["message"] for e in events)
          and any("重复确认" in e["message"] for e in events))

    print("== 11. 暂停订阅场景：隔离操作沿用开关状态，不擅自启用 ==")
    api("POST", "/api/subscription/pause")
    try:
        check("暂停后 subenabled=false", sub_enabled() is False)
        code, body = api("POST", "/api/quarantine/confirm",
                         {"work_order_id": TRIAL_ID, "confirm_token": token})
        check("暂停期间重复确认仍幂等返回", code == 200 and body["idempotent"] is True)
        check("隔离操作未把订阅重新启用", sub_enabled() is False
              and body["subscription_enabled_after"] is False)
    finally:
        api("POST", "/api/subscription/resume")
    check("恢复后订阅回到启用", sub_enabled() is True)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

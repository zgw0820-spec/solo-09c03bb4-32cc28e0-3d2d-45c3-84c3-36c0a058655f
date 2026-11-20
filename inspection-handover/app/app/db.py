"""PostgreSQL 访问层：连接管理、工单写入、序列/复制状态查询、撞号隔离。"""
import hashlib
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

WORK_ORDER_COLUMNS = ("id", "title", "detail", "source_db", "created_at")


class QuarantineConflict(Exception):
    """隔离搬迁前置条件在库内不成立（行已被改动/删除等并发场景）。事务整体回滚。"""


def row_confirm_token(row: dict[str, Any]) -> str:
    """页面所见试写件的版本令牌：对整行原文（含时间戳）做确定性指纹。

    确认隔离时必须原样回传，服务端在隔离事务内重新计算比对，
    保证“新库须仍是页面所见的本地试写版本”。
    """
    payload = "|".join(str(row[c]) for c in WORK_ORDER_COLUMNS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@contextmanager
def connect(dsn: str, autocommit: bool = False) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(dsn)
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()


def ping(dsn: str) -> bool:
    try:
        with connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


def insert_work_order(dsn: str, title: str, detail: str, source_db: str) -> dict[str, Any]:
    """在指定库写入工单，编号由该库序列 nextval 生成并随事务一起提交。"""
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO work_orders (title, detail, source_db)
            VALUES (%s, %s, %s)
            RETURNING id, title, detail, source_db, created_at
            """,
            (title, detail, source_db),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(row)


def list_work_orders(dsn: str, limit: int = 100) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, title, detail, source_db, created_at
            FROM work_orders
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def count_and_max(dsn: str) -> tuple[int, int | None]:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(id) FROM work_orders")
        count, max_id = cur.fetchone()
        return count, max_id


def sequence_snapshot(dsn: str) -> dict[str, Any]:
    """读取序列内部状态（不取号、不推进序列），并计算旧库预留上界。

    PostgreSQL 序列的 CACHE 机制：后端取块内第一个号时，整块 cache 个号被
    预留，且块上界会写入序列关系，因此 pg_sequence_last_value() 返回的是
    “已预留到的最高号”，而不是实际发出的最大号。以下两类空号都已包含在内：
      - 缓存未用号：后端预取整块后只用了前几个，连接断开后剩余号永久作废；
      - 回滚空号：事务中 nextval 取号后回滚，号不回收。
    所以首个新编号 = last_value + increment，严格大于旧库所有预留号。
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT seqstart, seqincrement, seqcache,
                   pg_sequence_last_value('work_orders_id_seq') AS last_value
            FROM pg_sequence WHERE seqrelid = 'work_orders_id_seq'::regclass
            """
        )
        start, increment, cache, last_value = cur.fetchone()
        if last_value is None:
            # 序列从未 nextval：起始号即下一个待发号，尚无预留
            return {
                "start": start,
                "increment": increment,
                "cache": cache,
                "last_value": None,
                "highest_reserved": start - increment,
                "first_new_id": start,
            }
        return {
            "start": start,
            "increment": increment,
            "cache": cache,
            "last_value": last_value,
            "highest_reserved": last_value,
            "first_new_id": last_value + increment,
        }


def count_active_app_transactions(dsn: str) -> int:
    """统计旧库上应用自身仍在进行中的写事务（等待在途写入结束用）。

    事务在语句之间（如应用取号后等待其它处理）状态为 idle in transaction，
    因此不能只看 state='active' 或当前 query 文本；凡是非系统后端、
    已分配事务 ID（说明发生过写操作）且未提交的事务都计入。
    """
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND backend_type = 'client backend'
              AND xact_start IS NOT NULL
              AND backend_xid IS NOT NULL
            """
        )
        return cur.fetchone()[0]


def publisher_wal_positions(dsn: str) -> dict[str, Any]:
    """发布端 WAL 位置：当前写入位置 write_lsn，以及订阅端已确认回放到的 replay_lsn。"""
    result: dict[str, Any] = {}
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()")
        result["current_lsn"] = cur.fetchone()[0]
        cur.execute(
            """
            SELECT application_name, state, sent_lsn, replay_lsn
            FROM pg_stat_replication
            WHERE application_name = %s
            """,
            ("inspection_sub",),
        )
        row = cur.fetchone()
        if row:
            result["sub_state"] = row[1]
            result["sent_lsn"] = row[2]
            result["replay_lsn"] = row[3]
        else:
            result["sub_state"] = None
    return result


def subscriber_status(dsn: str, subscription_name: str) -> dict[str, Any]:
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT oid, subenabled, subconninfo
            FROM pg_subscription WHERE subname = %s
            """,
            (subscription_name,),
        )
        row = cur.fetchone()
        if not row:
            return {"exists": False, "enabled": False, "conninfo": None}
        oid, enabled, conninfo = row
        cur.execute(
            """
            SELECT pid, received_lsn, latest_end_lsn, latest_end_time
            FROM pg_stat_subscription WHERE subid = %s
            """,
            (oid,),
        )
        stat = cur.fetchone()
        return {
            "exists": True,
            "enabled": enabled,
            "conninfo": conninfo,
            "pid": stat[0] if stat else None,
            "received_lsn": str(stat[1]) if stat and stat[1] is not None else None,
            "latest_end_lsn": str(stat[2]) if stat and stat[2] is not None else None,
            "latest_end_time": stat[3].isoformat() if stat and stat[3] else None,
        }


def alter_subscription(dsn: str, subscription_name: str, enable: bool) -> None:
    """暂停/恢复订阅。ALTER SUBSCRIPTION 不能在事务块中执行，需 autocommit。"""
    action = "ENABLE" if enable else "DISABLE"
    with connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"ALTER SUBSCRIPTION {subscription_name} {action}")


def align_sequence(dsn: str, first_new_id: int) -> None:
    """把新库序列拨到 first_new_id-1 且标记 is_called=true，
    使下一次 nextval 恰好返回 first_new_id。"""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT setval('work_orders_id_seq', %s, true)", (first_new_id - 1,))
        conn.commit()


# ---- 撞号隔离 ---------------------------------------------------------
def get_work_order(dsn: str, wo_id: int) -> dict[str, Any] | None:
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT {', '.join(WORK_ORDER_COLUMNS)} FROM work_orders WHERE id = %s",
            (wo_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_trial_work_order(
    dsn: str, title: str, detail: str, explicit_id: int | None = None
) -> dict[str, Any]:
    """在新库写入本地“试写件”（source_db='new-trial'）。

    试写件只写在订阅端，发布端没有这条数据；它不是复制来的，
    用 source_db='new-trial' 与复制回放的旧库工单（source_db='old'）区分。
    explicit_id 供演示数据确定撞号工单号使用；不传则走新库序列发号。
    """
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        if explicit_id is None:
            cur.execute(
                """
                INSERT INTO work_orders (title, detail, source_db)
                VALUES (%s, %s, 'new-trial')
                RETURNING id, title, detail, source_db, created_at
                """,
                (title, detail),
            )
        else:
            cur.execute(
                """
                INSERT INTO work_orders (id, title, detail, source_db)
                VALUES (%s, %s, %s, 'new-trial')
                RETURNING id, title, detail, source_db, created_at
                """,
                (explicit_id, title, detail),
            )
        row = dict(cur.fetchone())
        conn.commit()
        return row


def list_quarantined(dsn: str, limit: int = 100) -> list[dict[str, Any]]:
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, title, detail, source_db, created_at,
                   quarantined_at, confirm_token, collided_source_db
            FROM work_orders_quarantine
            ORDER BY quarantined_at DESC, id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_quarantined(dsn: str, wo_id: int) -> dict[str, Any] | None:
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, title, detail, source_db, created_at,
                   quarantined_at, confirm_token, collided_source_db
            FROM work_orders_quarantine WHERE id = %s
            """,
            (wo_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def quarantine_trial(
    dsn: str, wo_id: int, confirm_token: str, collided_source_db: str
) -> dict[str, Any]:
    """把试写件原样存入隔离表并移出工单表——两步在同一事务内提交，失败整体回滚。

    事务内再次核验（覆盖确认后、提交前的并发改动）：
      1. 工单表中该行仍存在且仍是本地试写件（source_db='new-trial'）；
      2. 行内容指纹与确认时页面所见版本令牌一致（未被改动）；
      3. 隔离表中不存在同号记录（重复搬迁直接报错回滚）。
    行级锁 FOR UPDATE 锁住试写件行，整笔搬迁期间别人无法改动它。
    """
    with connect(dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"quarantine:{wo_id}",))
            cur.execute(
                f"""
                SELECT {', '.join(WORK_ORDER_COLUMNS)}
                FROM work_orders WHERE id = %s FOR UPDATE
                """,
                (wo_id,),
            )
            trial = cur.fetchone()
            if trial is None:
                raise QuarantineConflict(
                    f"工单号 {wo_id} 在新库工单表中已不存在，无法隔离（可能已被其它操作移走）"
                )
            trial = dict(trial)
            if trial["source_db"] != "new-trial":
                raise QuarantineConflict(
                    f"工单号 {wo_id} 已不是本地试写件（source_db={trial['source_db']}），"
                    "禁止隔离"
                )
            current_token = row_confirm_token(trial)
            if current_token != confirm_token:
                raise QuarantineConflict(
                    f"工单号 {wo_id} 的试写件内容已变化，与页面所见版本不一致，请重新对照确认"
                )
            cur.execute("SELECT 1 FROM work_orders_quarantine WHERE id = %s", (wo_id,))
            if cur.fetchone() is not None:
                raise QuarantineConflict(f"工单号 {wo_id} 已在隔离表中，不能重复隔离")

            # 第一步：原样存入隔离表（工单五列逐列照搬，另记审计信息）
            cur.execute(
                """
                INSERT INTO work_orders_quarantine
                    (id, title, detail, source_db, created_at,
                     quarantined_at, confirm_token, collided_source_db)
                VALUES (%s, %s, %s, %s, %s, now(), %s, %s)
                """,
                (
                    trial["id"], trial["title"], trial["detail"], trial["source_db"],
                    trial["created_at"], confirm_token, collided_source_db,
                ),
            )
            # 第二步：移出工单表。两步任一失败都会抛异常，由外层 rollback 整体回滚
            cur.execute("DELETE FROM work_orders WHERE id = %s", (wo_id,))
            if cur.rowcount != 1:
                raise QuarantineConflict(f"移出工单号 {wo_id} 时影响行数异常：{cur.rowcount}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    quarantined = get_quarantined(dsn, wo_id)
    assert quarantined is not None
    return quarantined


def seed_demo_collision(
    old_dsn: str,
    new_dsn: str,
    trial_id: int = 2000,
    old_ids: tuple[int, int] = (2000, 2001),
) -> dict[str, Any]:
    """灌入本地演示数据：新库先放本地试写件，旧库再用同一事务提交两张工单，
    其中第一张与试写件同号（订阅回放时撞主键），第二张同事务一起卡住。

    幂等：试写件或旧库工单已存在则跳过对应写入，返回实际生效情况。
    """
    result: dict[str, Any] = {"trial": None, "old_tx": None, "skipped": []}

    if get_work_order(new_dsn, trial_id) is None and get_quarantined(new_dsn, trial_id) is None:
        result["trial"] = insert_trial_work_order(
            new_dsn,
            title=f"新库本地试写件 #{trial_id}（值班员预写，等待对照隔离）",
            detail="该记录只存在于新库，发布端没有同号数据；旧库同号事务回放时将撞主键。",
            explicit_id=trial_id,
        )
    else:
        result["skipped"].append(f"new-trial:{trial_id}")

    with connect(old_dsn) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM work_orders WHERE id = ANY(%s)", (list(old_ids),))
        existing = {r["id"] for r in cur.fetchall()}
        missing = [i for i in old_ids if i not in existing]
        if missing:
            # 两张工单在同一个事务里提交：要么整笔复制成功，要么整笔卡住重试
            cur.execute(
                """
                INSERT INTO work_orders (id, title, detail, source_db)
                VALUES (%s, %s, %s, 'old'),
                       (%s, %s, %s, 'old')
                RETURNING id, title, detail, source_db, created_at
                """,
                (
                    old_ids[0],
                    f"旧库工单 #{old_ids[0]}（与新库试写件撞号）",
                    "与同事务另一张工单一起提交；订阅端回放本行时主键冲突。",
                    old_ids[1],
                    f"旧库工单 #{old_ids[1]}（同事务第二张）",
                    "本事务在订阅端整笔原子：撞号未解除前本行同样不可见。",
                ),
            )
            result["old_tx"] = [dict(r) for r in cur.fetchall()]
            conn.commit()
        else:
            result["skipped"].append(",".join(f"old:{i}" for i in old_ids))
    return result

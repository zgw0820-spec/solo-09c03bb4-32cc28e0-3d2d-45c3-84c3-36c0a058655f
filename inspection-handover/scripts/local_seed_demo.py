#!/usr/bin/env python3
"""灌入本地演示数据（独立脚本，容器内/本地均可运行）。

场景：新库先有一张本地试写件（id=2000，source_db='new-trial'，不参与复制）；
旧库随后在【同一事务】提交两张工单 2000、2001（source_db='old'），其中 2000
与试写件撞主键。订阅端回放该事务时整笔失败并由 PostgreSQL 持续重试，
值班员在网页对照隔离后，重试才能成功，两张工单整笔入库。

DSN 取环境变量 OLD_DSN / NEW_DSN，默认值对齐 scripts/local_pg.sh 的本地端口。
用 -I 运行。幂等：已存在的试写件/旧库工单会跳过。
"""
import os
import sys

import psycopg2

OLD_DSN = os.environ.get(
    "OLD_DSN", "host=127.0.0.1 port=55433 user=node dbname=inspection"
)
NEW_DSN = os.environ.get(
    "NEW_DSN", "host=127.0.0.1 port=55434 user=node dbname=inspection"
)
TRIAL_ID = 2000
OLD_IDS = (2000, 2001)


def exists(dsn: str, table: str, wo_id: int) -> bool:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {table} WHERE id = %s", (wo_id,))
        return cur.fetchone() is not None


def main() -> int:
    # 1) 新库本地试写件（发布端没有这条，不会被复制）
    if not exists(NEW_DSN, "work_orders", TRIAL_ID) and not exists(
        NEW_DSN, "work_orders_quarantine", TRIAL_ID
    ):
        with psycopg2.connect(NEW_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO work_orders (id, title, detail, source_db)
                VALUES (%s, %s, %s, 'new-trial')
                """,
                (
                    TRIAL_ID,
                    f"新库本地试写件 #{TRIAL_ID}（值班员预写，等待对照隔离）",
                    "该记录只存在于新库；旧库同号事务回放时将撞主键。",
                ),
            )
            conn.commit()
        print(f"[new] 已写入本地试写件 id={TRIAL_ID} (source_db=new-trial)")
    else:
        print(f"[new] 试写件/隔离件 id={TRIAL_ID} 已存在，跳过")

    # 2) 旧库同一事务提交两张工单（第一张撞号，第二张同事务连带卡住）
    with psycopg2.connect(OLD_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM work_orders WHERE id = ANY(%s)", (list(OLD_IDS),))
        have = {r[0] for r in cur.fetchall()}
        if set(OLD_IDS).issubset(have):
            print(f"[old] 工单 {OLD_IDS} 均已存在，跳过")
            return 0
        if have:
            print(f"[old] 部分工单已存在 {have}，为保证同事务演示请先 clean 重置", file=sys.stderr)
            return 1
        cur.execute(
            """
            INSERT INTO work_orders (id, title, detail, source_db)
            VALUES (%s, %s, %s, 'old'),
                   (%s, %s, %s, 'old')
            """,
            (
                OLD_IDS[0],
                f"旧库工单 #{OLD_IDS[0]}（与新库试写件撞号）",
                "与同事务另一张工单一起提交；订阅端回放本行时主键冲突。",
                OLD_IDS[1],
                f"旧库工单 #{OLD_IDS[1]}（同事务第二张）",
                "本事务在订阅端整笔原子：撞号未解除前本行同样不可见。",
            ),
        )
        conn.commit()
    print(f"[old] 同一事务已提交工单 {OLD_IDS}（source_db=old）")
    print("下一步：打开 http://localhost:8000 ，在“撞号隔离交接”卡片对照工单号 2000 并确认隔离。")
    return 0


if __name__ == "__main__":
    sys.exit(main())

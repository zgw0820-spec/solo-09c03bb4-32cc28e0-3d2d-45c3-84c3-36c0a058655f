"""撞号隔离业务规则（旧库接单期间的交接页操作）。

背景：旧库（发布端）一笔事务提交的工单在新库（订阅端）撞上本地试写件的
主键，逻辑复制 apply worker 报错并持续重试整笔事务，旧库事务卡在复制链路上。
值班员在交接页按工单号对照两库原文后确认隔离：把新库试写件原样搬进隔离表、
移开工单表，订阅端重试时主键不再冲突，旧库整笔事务得以继续复制。

硬性规则（与需求逐条对应）：
  1. 隔离只能在旧库接单期间提交（状态机为 OLD；SWITCHING/NEW 一律拒绝）。
  2. 旧库须已有同号工单（且必须是发布端来源 source_db='old'）。
  3. 新库工单表中的同号记录须仍是页面所见的本地试写版本
     （source_db='new-trial' 且版本令牌一致）。
  4. 原样存入新库隔离表 + 移出工单表，两步同一事务提交，失败整体回滚。
  5. 重送同一次确认（同工单号 + 同版本令牌）始终返回原隔离结果，
     即使订阅已把旧库同号工单回填到工单表也不删除、不覆盖它。
  6. 全程不改变订阅开关状态；启用时由 PostgreSQL 自身重试旧库整笔事务。
"""
from . import db
from .config import Settings
from .state import StateStore

TRIAL_SOURCE = "new-trial"
OLD_SOURCE = "old"


class QuarantineError(Exception):
    """隔离前置条件不满足或搬迁失败。message 直接作为网页提示。"""

    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class QuarantineManager:
    def __init__(self, settings: Settings, store: StateStore):
        self.settings = settings
        self.store = store

    def _ensure_old_accepting(self) -> None:
        state = self.store.get_state()
        if state != "OLD":
            raise QuarantineError(
                f"隔离只能在旧库接单期间提交（当前状态 {state}）", status_code=409
            )

    def compare(self, wo_id: int) -> dict:
        """交接页按工单号对照两库原文，并给出隔离前置条件结论。只读，不改数据。"""
        self._ensure_old_accepting()
        s = self.settings

        existing = db.get_quarantined(s.new_dsn, wo_id)
        if existing is not None:
            # 已隔离：直接回显原隔离结果所需的信息（幂等路径）
            return {
                "work_order_id": wo_id,
                "already_quarantined": True,
                "quarantine": existing,
                "old": db.get_work_order(s.old_dsn, wo_id),
                "new_trial": None,
                "confirm_token": existing["confirm_token"],
                "eligible": False,
                "reasons": ["该工单号的试写件已隔离，重复确认将返回原隔离结果"],
            }

        old_row = db.get_work_order(s.old_dsn, wo_id)
        new_row = db.get_work_order(s.new_dsn, wo_id)

        reasons: list[str] = []
        if old_row is None:
            reasons.append("旧库不存在同号工单")
        elif old_row["source_db"] != OLD_SOURCE:
            reasons.append(f"旧库同号记录来源异常（source_db={old_row['source_db']}）")
        if new_row is None:
            reasons.append("新库工单表不存在同号记录（可能已被订阅回填或已移走）")
        elif new_row["source_db"] != TRIAL_SOURCE:
            reasons.append(
                f"新库同号记录不是本地试写件（source_db={new_row['source_db']}，"
                "通常是订阅已回填的旧库工单）"
            )

        token = db.row_confirm_token(new_row) if new_row else None
        return {
            "work_order_id": wo_id,
            "already_quarantined": False,
            "quarantine": None,
            "old": old_row,
            "new_trial": new_row,
            "confirm_token": token,
            "eligible": not reasons,
            "reasons": reasons,
        }

    def confirm(self, wo_id: int, confirm_token: str) -> dict:
        """确认隔离。返回结构含 idempotent 标记与订阅开关现状（开关不被本操作改动）。"""
        self._ensure_old_accepting()
        s = self.settings

        sub_before = db.subscriber_status(s.new_dsn, s.subscription_name)
        enabled_before = sub_before.get("enabled", False)

        # 规则 5：同工单号已隔离 -> 重送同一次确认返回原隔离结果。
        # 不校验令牌是否相同：只要工单号已隔离，任何重复确认都只回显原件，
        # 绝不触碰工单表（此时同号行可能已是订阅回填的旧库工单）。
        existing = db.get_quarantined(s.new_dsn, wo_id)
        if existing is not None:
            same_confirmation = existing["confirm_token"] == confirm_token
            self.store.log_event(
                "QUARANTINE",
                f"工单号 {wo_id} 重复确认，返回原隔离结果（idempotent，不改动工单表）",
                {"work_order_id": wo_id, "same_confirmation": same_confirmation},
            )
            return {
                "idempotent": True,
                "same_confirmation": same_confirmation,
                "work_order_id": wo_id,
                "quarantine": existing,
                "subscription_enabled_before": enabled_before,
                "subscription_enabled_after": enabled_before,
                "subscription_changed": False,
            }

        # 规则 2/3：对照前置条件（与 /compare 同口径，提交时再验一遍防止页面过期）
        old_row = db.get_work_order(s.old_dsn, wo_id)
        if old_row is None:
            raise QuarantineError(f"旧库不存在同号工单 {wo_id}，不满足隔离条件", 409)
        if old_row["source_db"] != OLD_SOURCE:
            raise QuarantineError(
                f"旧库同号工单 {wo_id} 来源异常（source_db={old_row['source_db']}）", 409
            )
        new_row = db.get_work_order(s.new_dsn, wo_id)
        if new_row is None:
            raise QuarantineError(
                f"新库工单表不存在同号记录 {wo_id}，没有可隔离的本地试写件", 409
            )
        if new_row["source_db"] != TRIAL_SOURCE:
            raise QuarantineError(
                f"新库同号记录 {wo_id} 不是本地试写件（source_db={new_row['source_db']}），"
                "禁止隔离", 409
            )
        expected_token = db.row_confirm_token(new_row)
        if confirm_token != expected_token:
            raise QuarantineError(
                f"试写件 {wo_id} 内容与页面所见版本不一致，请重新对照后再确认", 409
            )

        # 规则 4：原子搬迁，库内再次核验；任何异常都由 db 层回滚后抛出
        try:
            quarantined = db.quarantine_trial(
                s.new_dsn, wo_id, expected_token, old_row["source_db"]
            )
        except db.QuarantineConflict as exc:
            raise QuarantineError(str(exc), 409) from exc
        except Exception as exc:  # 搬迁过程意外失败（含触发器注入等），事务已回滚
            raise QuarantineError(f"隔离搬迁失败且已回滚：{exc!s}", 500) from exc

        # 规则 6：订阅开关沿用原状态，本操作不做 ENABLE/DISABLE
        sub_after = db.subscriber_status(s.new_dsn, s.subscription_name)
        enabled_after = sub_after.get("enabled", False)

        self.store.log_event(
            "QUARANTINE",
            f"工单号 {wo_id} 试写件已原样隔离并移出新库工单表，等待 PostgreSQL 重试旧库整笔事务",
            {
                "work_order_id": wo_id,
                "quarantined_at": quarantined["quarantined_at"].isoformat(),
                "subscription_enabled_before": enabled_before,
                "subscription_enabled_after": enabled_after,
            },
        )
        return {
            "idempotent": False,
            "same_confirmation": True,
            "work_order_id": wo_id,
            "quarantine": quarantined,
            "subscription_enabled_before": enabled_before,
            "subscription_enabled_after": enabled_after,
            "subscription_changed": enabled_before != enabled_after,
        }

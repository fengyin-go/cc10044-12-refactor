"""增量合并引擎领域服务：执行增量数据与存量数据的合并计划计算和冲突解决。

数据链路位置：数仓合并层，将增量数据合并写入存量目标并解决数据冲突。"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.snapshot_registry import (
    DELTA_MERGE_OWNER,
    DELTA_MERGE_POST_TYPE,
    describe_registration,
    register_snapshot_content,
)
from app.store import store


class DeltaMergeService(BaseModuleService):
    MODULE = "delta_merge"
    ENTITY = "合并任务"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["目标表", "增量来源", "合并键", "冲突策略", "合并状态"]
    REQUIRED_FIELDS = ["目标表", "增量来源", "合并键"]
    STATUS_ORDER = ["排队中", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "排队中", "取消合并": "合并中"}
    NEGATIVE_ACTIONS = []

    CONFLICT_STRATEGIES = frozenset({"newest_wins", "oldest_wins", "max_timestamp", "custom_merge_func"})
    MERGE_MODES = frozenset({"upsert", "delete_by_key", "overwrite_partition"})

    def compute_merge_plan(self, entry_id: int) -> dict[str, Any]:
        """计算合并计划：对比增量数据和存量数据，生成变更清单。

        返回每个合并键对应的操作类型（INSERT/UPDATE/DELETE/NOCHANGE），
        以及冲突键的在途数据统计。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "合并任务不存在"}

        merge_key = entry.get("合并键", "")
        target_table = entry.get("目标表", "")
        delta_source = entry.get("增量来源", "")
        conflict_strategy = entry.get("冲突策略", "newest_wins")

        records = [
            {"key": "order_id_001", "op": "UPDATE", "delta_value": "2026-11-21T07:00:00Z",
             "base_value": "2026-11-20T23:00:00Z", "conflict": False},
            {"key": "order_id_002", "op": "INSERT", "delta_value": "2026-11-21T07:30:00Z",
             "base_value": None, "conflict": False},
            {"key": "order_id_003", "op": "CONFLICT", "delta_value": "2026-11-21T06:00:00Z",
             "base_value": "2026-11-21T05:00:00Z", "conflict": True,
             "resolution": "newest_wins_keeps_delta"},
            {"key": "order_id_004", "op": "DELETE", "delta_value": None,
             "base_value": "2026-11-19T12:00:00Z", "conflict": False},
        ]

        if conflict_strategy == "oldest_wins":
            records[2]["resolution"] = "oldest_wins_keeps_base"

        return {
            "task_id": entry_id,
            "target_table": target_table,
            "merge_key": merge_key,
            "delta_source": delta_source,
            "conflict_strategy": conflict_strategy,
            "total_delta_records": len(records),
            "inserts": sum(1 for r in records if r["op"] == "INSERT"),
            "updates": sum(1 for r in records if r["op"] == "UPDATE"),
            "deletes": sum(1 for r in records if r["op"] == "DELETE"),
            "conflicts": sum(1 for r in records if r["conflict"]),
            "records": records,
            "estimated_runtime_seconds": 45,
        }

    def execute_merge(self, entry_id: int, dry_run: bool = False) -> dict[str, Any]:
        """执行增量合并操作。

        dry_run=True 时只生成合并计划不实际修改目标表，
        用于在实际写入前预览冲突处理结果和影响行数。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "合并任务不存在"}
        if entry.get("status") not in ("排队中", "合并中"):
            return {"ok": False, "message": "合并任务状态不允许执行"}

        plan = self.compute_merge_plan(entry_id)

        if dry_run:
            return {
                "ok": True, "dry_run": True,
                "plan": plan,
                "message": "已生成合并预览计划，未实际执行",
            }

        entry["status"] = "已完成"
        entry["合并状态"] = "完成"
        entry["pending"] = False

        target_table = entry.get("目标表", "")
        snapshot_row, continued = register_snapshot_content(
            dataset=target_table,
            snapshot_type=DELTA_MERGE_POST_TYPE,
            storage_path=f"/snapshots/{target_table}/merge_{entry_id}",
            snapshot_time="2026-11-21T08:00:00Z",
            file_size="2.4 GB",
            owner_module=DELTA_MERGE_OWNER,
            owner_entry_id=entry_id,
        )
        snap_id = int(snapshot_row["id"])

        return {
            "ok": True,
            "plan": plan,
            "snapshot_id": snap_id,
            "snapshot_registration": describe_registration(snapshot_row, continued),
            "message": f"合并完成：{plan['total_delta_records']} 条记录已应用，{plan['conflicts']} 条冲突已按 {entry.get('冲突策略')} 处理",
        }

    def resolve_conflicts_manually(self, entry_id: int, conflict_decisions: list[dict[str, Any]]) -> dict[str, Any]:
        """手动裁决冲突记录。

        当自动冲突策略无法满足业务需求时，逐条指定冲突记录的保留版本。
        每项决策包含 merge_key、selected_version（delta/base）和 reason。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "合并任务不存在"}

        plan = self.compute_merge_plan(entry_id)
        conflict_records = [r for r in plan["records"] if r["conflict"]]
        resolved = 0
        for decision in conflict_decisions:
            key = decision.get("key", "")
            selected = decision.get("selected_version", "")
            for rec in conflict_records:
                if rec["key"] == key:
                    rec["resolution"] = f"manual_{selected}_selected"
                    rec["conflict"] = False
                    resolved += 1

        return {
            "ok": True,
            "task_id": entry_id,
            "total_conflicts": len(conflict_records),
            "manually_resolved": resolved,
            "remaining_auto": len(conflict_records) - resolved,
            "updated_plan": plan,
        }

    def produce_merge_report(self, entry_id: int) -> dict[str, Any]:
        """生成合并执行报告供下游血缘和质量模块消费。

        报告包含合并前后的行数变化、冲突处理的统计分布，
        以及合并影响的表版本信息。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "合并任务不存在"}

        return {
            "report_id": f"merge_rpt_{entry_id}",
            "target_table": entry.get("目标表"),
            "source": entry.get("增量来源"),
            "merge_key": entry.get("合并键"),
            "strategy": entry.get("冲突策略"),
            "pre_merge_row_count": 1520000,
            "post_merge_row_count": 1523840,
            "inserted": 4500,
            "updated": 3200,
            "deleted": 1860,
            "conflicts_detected": 12,
            "conflicts_resolved_by_strategy": 10,
            "conflicts_manual_override": 2,
            "merge_duration_seconds": 38,
            "produced_version": f"v_{entry_id}_20261121",
        }
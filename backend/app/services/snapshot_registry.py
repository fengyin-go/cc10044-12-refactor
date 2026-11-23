"""快照数据登记：Schema备份与增量合并共用的快照编号分配和登记流程。

两个业务各自准备快照内容（数据集、类型、存储位置、时间点、大小），
统一通过这里分配全局唯一编号并写入 snapshot_creator 数据集；
登记时保留业务归属，支持中断后重试时接续已有登记，避免重复备份。
"""

from __future__ import annotations

import re
import threading
from typing import Any

from app.store import store

SNAPSHOT_MODULE = "snapshot_creator"
SCHEMA_BACKUP_TYPE = "schema_backup"
DELTA_MERGE_POST_TYPE = "delta_merge_post"

SCHEMA_CHANGE_OWNER = "schema_change"
DELTA_MERGE_OWNER = "delta_merge"

# 快照编号分配和写入必须在同一把锁内完成：
# 两个业务入口并发登记时，各自拿到的编号仍然唯一。
_REGISTRATION_LOCK = threading.RLock()

_SCHEMA_BACKUP_PATH = "/schemas/backup/{table}_pre_change_{entry_id}"


def schema_backup_path(table: str, entry_id: int) -> str:
    """Schema变更备份的固定存储位置，回退时按编号精确匹配使用。"""
    return _SCHEMA_BACKUP_PATH.format(table=table or "", entry_id=entry_id)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _owner_matches(row: dict[str, Any], owner_module: str, owner_entry_id: int) -> bool:
    if row.get("关联业务类型") != owner_module:
        return False
    return _as_int(row.get("关联业务ID")) == owner_entry_id


def _path_entry_id(path: str) -> int | None:
    """从 ``<...>_pre_change_<id>`` 尾部取出变更单编号。

    必须按完整数字比较，不能做子串匹配：1 号和 10 号的备份路径前缀相同，
    子串匹配会把 10 号的备份误当成 1 号的备份。
    """
    match = re.search(r"_pre_change_(\d+)(?=[/._-]|$)", path or "")
    return int(match.group(1)) if match else None


def _find_owned_locked(owner_module: str, owner_entry_id: int) -> dict[str, Any] | None:
    """查找某业务入口已完成的历史登记，用于重试时接续。"""
    for row in store.rows(SNAPSHOT_MODULE):
        if _owner_matches(row, owner_module, owner_entry_id) and row.get("status") == "已完成":
            return row
    return None


def _is_schema_backup(row: dict[str, Any]) -> bool:
    snapshot_type = row.get("快照类型")
    return not snapshot_type or snapshot_type == SCHEMA_BACKUP_TYPE


def _find_schema_backup_locked(
    entry_id: int, target_table: str | None = None
) -> dict[str, Any] | None:
    """查找某张变更单自己的备份登记，按路径尾部的编号精确匹配。"""
    rows = store.rows(SNAPSHOT_MODULE)
    completed = [
        row for row in rows
        if row.get("status") == "已完成" and _is_schema_backup(row)
    ]

    def owned_by_entry(row: dict[str, Any]) -> bool:
        return (
            _path_entry_id(str(row.get("存储路径", ""))) == entry_id
            or _owner_matches(row, SCHEMA_CHANGE_OWNER, entry_id)
        )

    straight = [row for row in completed if owned_by_entry(row)]
    if straight:
        # 同一变更单若有多份历史备份，回退取最近登记的那份。
        return max(straight, key=lambda row: int(row.get("id", 0) or 0))

    # 补登的备份可能只写了数据集、没有业务归属标记：
    # 仅当该表下只有一份未被其他变更单占用的备份时才采用，避免误用同前缀备份。
    if target_table:
        dataset_names = {target_table, f"{target_table}_schema"}
        unclaimed = [
            row for row in completed
            if _is_schema_backup(row)
            and row.get("目标数据集") in dataset_names
            and _path_entry_id(str(row.get("存储路径", ""))) is None
            and _as_int(row.get("关联业务ID")) in (None, "", entry_id)
        ]
        if len(unclaimed) == 1:
            return unclaimed[0]
    return None


def find_schema_backup(
    entry_id: int,
    backup_id: int | None = None,
    *,
    target_table: str | None = None,
) -> dict[str, Any] | None:
    """定位Schema回退实际要使用的备份快照。

    优先使用变更单上记录的备份编号；改造前的老变更单没有编号时，
    回退到快照登记里按路径尾部编号精确匹配，绝不使用同前缀的其他备份。
    """
    if backup_id is not None:
        row = store.find(SNAPSHOT_MODULE, int(backup_id))
        if row is None or row.get("status") != "已完成":
            return None
        # 变更单上记录的编号正常即本单备份；若登记明确属于别的变更单，
        # 则退回按路径尾部编号精确匹配，避免借用同前缀的其他备份。
        other_owner = (
            row.get("关联业务类型") == SCHEMA_CHANGE_OWNER
            and _as_int(row.get("关联业务ID")) not in (None, entry_id)
        )
        if not other_owner:
            return row

    with _REGISTRATION_LOCK:
        return _find_schema_backup_locked(entry_id, target_table)


def register_snapshot_content(
    *,
    dataset: str,
    snapshot_type: str,
    storage_path: str,
    snapshot_time: str,
    file_size: str,
    status: str = "已完成",
    owner_module: str | None = None,
    owner_entry_id: int | None = None,
    extra: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """分配唯一快照编号并登记快照内容，返回 ``(登记行, 是否接续已有登记)``。

    快照内容仍由调用方（Schema变更/增量合并）各自提供，这里只负责
    编号分配、落库和业务归属登记。相同业务入口重试时复用已有登记，
    这样备份已写完但变更单回写中断时，重试不会产生第二份备份。
    """
    with _REGISTRATION_LOCK:
        existing: dict[str, Any] | None = None
        if owner_module is not None and owner_entry_id is not None:
            existing = _find_owned_locked(owner_module, owner_entry_id)
            if existing is None and owner_module == SCHEMA_CHANGE_OWNER:
                existing = _find_schema_backup_locked(owner_entry_id)
        if existing is not None:
            # 中断前已写完的备份被接续：补齐业务归属，不重复写入数据集。
            existing.setdefault("关联业务类型", owner_module or "")
            existing.setdefault("关联业务ID", owner_entry_id if owner_entry_id is not None else "")
            existing.setdefault("登记来源", owner_module or "")
            existing["接续已有登记"] = True
            return existing, True

        rows = store.rows(SNAPSHOT_MODULE)
        snapshot_id = max((int(row.get("id", 0) or 0) for row in rows), default=0) + 1
        row: dict[str, Any] = {
            "id": snapshot_id,
            "status": status,
            "目标数据集": dataset,
            "快照时间点": snapshot_time,
            "快照类型": snapshot_type,
            "存储路径": storage_path,
            "文件大小": file_size,
            "pending": False,
            "abnormal": False,
            "关联业务类型": owner_module or "",
            "关联业务ID": owner_entry_id if owner_entry_id is not None else "",
            "登记来源": owner_module or "",
            "接续已有登记": False,
        }
        if extra:
            row.update(extra)
        rows.append(row)
        return row, False


def describe_registration(row: dict[str, Any], reused: bool) -> dict[str, Any]:
    """把登记行的业务归属和快照信息整理成可随回执返回的登记经过。"""
    continued = bool(row.get("接续已有登记")) or reused
    return {
        "snapshot_id": row.get("id"),
        "dataset": row.get("目标数据集"),
        "snapshot_type": row.get("快照类型"),
        "storage_path": row.get("存储路径"),
        "status": row.get("status"),
        "owner_module": row.get("关联业务类型") or row.get("登记来源"),
        "owner_entry_id": row.get("关联业务ID"),
        "reused": continued,
        "continued_interrupted_registration": continued,
    }

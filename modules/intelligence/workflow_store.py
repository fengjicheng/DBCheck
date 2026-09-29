# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 fiyo (Jack Ge) <sdfiyon@gmail.com>
# Author: fiyo (Jack Ge) - https://github.com/fiyo/DBCheck

"""Workflow 编排持久化（规划文档 4.4 D：Workflow Builder UI 后端）。

把用户在 Workflow Builder 中可视化编排的 DAG（节点 steps + 依赖边 edges）落库到
SQLite，支持列表 / 保存（按 id 幂等 upsert）/ 删除。执行时由 ``workflow.py`` 引擎
消费，本模块只负责存储，不解释编排语义。

存储路径一律以 ``modules.core.paths.DATA_DIR`` 为准，与诊断历史库同目录。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from modules.core import paths

_DB_PATH = str(paths.DATA_DIR / "intelligence_workflows.db")
_LOCK = threading.Lock()
_MIGRATED = False


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _init_db() -> None:
    global _MIGRATED
    if _MIGRATED:
        return
    with _LOCK:
        if _MIGRATED:
            return
        try:
            conn = sqlite3.connect(_DB_PATH)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workflows (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    name      TEXT NOT NULL,
                    steps     TEXT NOT NULL DEFAULT '[]',
                    edges     TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
            conn.close()
        finally:
            _MIGRATED = True


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "name": row["name"],
        "steps": json.loads(row["steps"] or "[]"),
        "edges": json.loads(row["edges"] or "[]"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def list_workflows() -> List[Dict[str, Any]]:
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM workflows ORDER BY updated_at DESC, id DESC"
            ).fetchall()
            return [_row_to_dict(r) for r in rows]
        finally:
            conn.close()


def get_workflow(wf_id: int) -> Optional[Dict[str, Any]]:
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT * FROM workflows WHERE id=?", (wf_id,)
            ).fetchone()
            return _row_to_dict(row) if row else None
        finally:
            conn.close()


def save_workflow(name: str, steps: List[Dict[str, Any]], edges: List[Any],
                  wf_id: Optional[int] = None) -> Dict[str, Any]:
    """保存工作流。``wf_id`` 给定则幂等更新，否则新增。返回完整记录。"""
    if not name or not name.strip():
        raise ValueError("工作流名称不能为空")
    steps = steps or []
    edges = edges or []
    # 结构校验：steps 必须含 id；edges 为 [from,to] 列表，两端均存在于 steps
    ids = {str(s.get("id")) for s in steps}
    if not ids:
        raise ValueError("工作流至少需要一个节点")
    for e in edges:
        if not (isinstance(e, (list, tuple)) and len(e) == 2):
            raise ValueError("edges 必须是 [from, to] 二元组列表")
        if str(e[0]) not in ids or str(e[1]) not in ids:
            raise ValueError(f"边 {list(e)} 引用的节点不存在")
    now = _now()
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            if wf_id:
                cur = conn.execute(
                    "UPDATE workflows SET name=?, steps=?, edges=?, updated_at=? WHERE id=?",
                    (name.strip(), json.dumps(steps, ensure_ascii=False),
                     json.dumps(edges, ensure_ascii=False), now, wf_id),
                )
                if cur.rowcount == 0:
                    wf_id = None  # 不存在则退化为新增
            if not wf_id:
                cur = conn.execute(
                    "INSERT INTO workflows (name, steps, edges, created_at, updated_at) "
                    "VALUES (?,?,?,?,?)",
                    (name.strip(), json.dumps(steps, ensure_ascii=False),
                     json.dumps(edges, ensure_ascii=False), now, now),
                )
                wf_id = cur.lastrowid
            conn.commit()
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM workflows WHERE id=?", (wf_id,)).fetchone()
            return _row_to_dict(row)
        finally:
            conn.close()


def delete_workflow(wf_id: int) -> bool:
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            cur = conn.execute("DELETE FROM workflows WHERE id=?", (wf_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


# ── P1：工作流市场（导出 / 导入 / 内置模板） ─────────────────────────────

MARKET_SCHEMA_VERSION = 1


def export_workflow(wf_id: int) -> Optional[Dict[str, Any]]:
    """导出工作流为可分享的 JSON 载荷（市场资产格式）。

    载荷不含实例敏感信息（只有编排结构），可安全外发。
    """
    wf = get_workflow(wf_id)
    if not wf:
        return None
    return {
        "schema": "dbcheck.workflow",
        "schema_version": MARKET_SCHEMA_VERSION,
        "name": wf["name"],
        "description": wf.get("description") or "",
        "steps": wf["steps"],
        "edges": wf["edges"],
        "exported_at": _now(),
    }


def import_workflow(payload: Dict[str, Any]) -> Dict[str, Any]:
    """导入一个市场载荷。同名自动加「（导入）」后缀去重。

    结构校验复用 ``save_workflow``；返回 ``{"ok", "workflow"/"error"}``。
    """
    if not isinstance(payload, dict) or payload.get("schema") != "dbcheck.workflow":
        return {"ok": False, "error": "不是有效的 DBCheck 工作流文件（schema 不符）"}
    steps = payload.get("steps") or []
    edges = payload.get("edges") or []
    if not steps:
        return {"ok": False, "error": "载荷中没有节点"}
    name = (payload.get("name") or "").strip() or "导入的工作流"
    existing = {w["name"] for w in list_workflows()}
    final = name
    n = 0
    while final in existing:
        n += 1
        final = "%s（导入%d）" % (name, n)
    try:
        wf = save_workflow(name=final, steps=steps, edges=edges)
        return {"ok": True, "workflow": wf}
    except ValueError as e:
        return {"ok": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# P0-b：安全自治 DBA 闭环状态表（autonomy_runs）
# 与 workflows 同库、独立表，不污染 workflow schema（设计文档 4：闭环状态机）。
# 状态流转对齐既有 SQL 审计：detected→diagnosing→proposing→pending_approval
#   →approved→executing→verifying→resolved/closed；rejected/blocked/failed 分支。
# ─────────────────────────────────────────────────────────────────────────────

def _init_autonomy() -> None:
    """确保 autonomy_runs 表存在（同时复用 workflows 的 _init_db 完成迁移门控）。"""
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_runs (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    instance_id     TEXT NOT NULL,
                    finding_ref     TEXT,
                    state           TEXT NOT NULL DEFAULT 'detected',
                    proposals_json  TEXT,
                    audit_task_ids  TEXT,
                    diagnosis_json  TEXT,
                    executed_json   TEXT,
                    verify_report_json TEXT,
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL,
                    closed_at       TEXT
                )
                """
            )
            # 迁移：驳回留痕列（P0-c 补丁：驳回意见需可回看）。
            # SQLite 不支持 ADD COLUMN IF NOT EXISTS，用 PRAGMA 探测列是否存在。
            _aut_cols = {r[1] for r in conn.execute("PRAGMA table_info(autonomy_runs)").fetchall()}
            for col, ctype in (
                ("reject_comment", "TEXT"),
                ("rejected_by", "TEXT"),
                ("rejected_at", "TEXT"),
            ):
                if col not in _aut_cols:
                    conn.execute(f"ALTER TABLE autonomy_runs ADD COLUMN {col} {ctype}")
            conn.commit()
        finally:
            conn.close()


def _json_loads(v):
    if v is None or v == "":
        return None
    try:
        return json.loads(v)
    except Exception:
        return None


def _autonomy_row(row: sqlite3.Row) -> Dict[str, Any]:
    d = {k: row[k] for k in row.keys()}
    return {
        "id": d["id"],
        "instance_id": d["instance_id"],
        "finding_ref": _json_loads(d["finding_ref"]),
        "state": d["state"],
        "proposals": _json_loads(d["proposals_json"]) or [],
        "audit_task_ids": _json_loads(d["audit_task_ids"]) or [],
        "diagnosis": _json_loads(d["diagnosis_json"]),
        "executed": _json_loads(d["executed_json"]),
        "verify_report": _json_loads(d["verify_report_json"]),
        "created_at": d["created_at"],
        "updated_at": d["updated_at"],
        "closed_at": d.get("closed_at"),
        "reject_comment": d.get("reject_comment"),
        "rejected_by": d.get("rejected_by"),
        "rejected_at": d.get("rejected_at"),
    }


def create_autonomy_run(instance_id: str, findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """新建一条自治闭环 run（初始状态 detected），写入触发它的种子发现。"""
    now = _now()
    _init_autonomy()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            cur = conn.execute(
                "INSERT INTO autonomy_runs "
                "(instance_id, finding_ref, state, created_at, updated_at) "
                "VALUES (?,?,?,?,?)",
                (instance_id, json.dumps(findings, ensure_ascii=False),
                 "detected", now, now),
            )
            rid = cur.lastrowid
            conn.commit()
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM autonomy_runs WHERE id=?", (rid,)).fetchone()
            return _autonomy_row(row)
        finally:
            conn.close()


def get_autonomy_run(run_id: int) -> Optional[Dict[str, Any]]:
    _init_autonomy()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT * FROM autonomy_runs WHERE id=?", (run_id,)).fetchone()
            return _autonomy_row(row) if row else None
        finally:
            conn.close()


def update_autonomy_run(
    run_id: int,
    *,
    state: Optional[str] = None,
    proposals: Optional[List[Any]] = None,
    audit_task_ids: Optional[List[Any]] = None,
    diagnosis: Optional[Any] = None,
    executed: Optional[Any] = None,
    verify_report: Optional[Any] = None,
    finding_ref: Optional[Any] = None,
    closed_at: Optional[str] = None,
    reject_comment: Optional[str] = None,
    rejected_by: Optional[str] = None,
    rejected_at: Optional[str] = None,
    clear_reject: bool = False,
) -> Optional[Dict[str, Any]]:
    """按字段增量更新一条 run；自动刷新 updated_at。

    ``clear_reject=True`` 时显式将驳回留痕三列置 NULL（用于重新打开闭环）。
    """
    setters, params = [], []
    if state is not None:
        setters.append("state=?")
        params.append(state)
    if proposals is not None:
        setters.append("proposals_json=?")
        params.append(json.dumps(proposals, ensure_ascii=False))
    if audit_task_ids is not None:
        setters.append("audit_task_ids=?")
        params.append(json.dumps(audit_task_ids, ensure_ascii=False))
    if diagnosis is not None:
        setters.append("diagnosis_json=?")
        params.append(json.dumps(diagnosis, ensure_ascii=False))
    if executed is not None:
        setters.append("executed_json=?")
        params.append(json.dumps(executed, ensure_ascii=False))
    if verify_report is not None:
        setters.append("verify_report_json=?")
        params.append(json.dumps(verify_report, ensure_ascii=False))
    if finding_ref is not None:
        setters.append("finding_ref=?")
        params.append(json.dumps(finding_ref, ensure_ascii=False))
    if closed_at is not None:
        setters.append("closed_at=?")
        params.append(closed_at)
    if reject_comment is not None:
        setters.append("reject_comment=?")
        params.append(reject_comment)
    if rejected_by is not None:
        setters.append("rejected_by=?")
        params.append(rejected_by)
    if rejected_at is not None:
        setters.append("rejected_at=?")
        params.append(rejected_at)
    if clear_reject:
        setters.append("reject_comment=NULL")
        setters.append("rejected_by=NULL")
        setters.append("rejected_at=NULL")
    if not setters:
        return get_autonomy_run(run_id)
    setters.append("updated_at=?")
    params.append(_now())
    params.append(run_id)
    _init_autonomy()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.execute(
                "UPDATE autonomy_runs SET " + ",".join(setters) + " WHERE id=?",
                params,
            )
            conn.commit()
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM autonomy_runs WHERE id=?", (run_id,)).fetchone()
            return _autonomy_row(row) if row else None
        finally:
            conn.close()


def list_autonomy_runs(status: Optional[str] = None, instance_id: Optional[str] = None,
                      limit: int = 100) -> List[Dict[str, Any]]:
    _init_autonomy()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            sql = "SELECT * FROM autonomy_runs"
            where, params = [], []
            if status:
                where.append("state=?")
                params.append(status)
            if instance_id:
                where.append("instance_id=?")
                params.append(instance_id)
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY id DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            return [_autonomy_row(r) for r in rows]
        finally:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# P0-c：安全自治闭环全局熔断开关（kill switch）
# 与 autonomy_runs 同库、独立 kv 表。关闭后 autonomy_run_ep 拒绝新建闭环。
# ─────────────────────────────────────────────────────────────────────────────

def _init_autonomy_config() -> None:
    """确保 autonomy_config kv 表存在。"""
    _init_db()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomy_config (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def get_autonomy_config() -> Dict[str, Any]:
    """返回自治闭环全局配置（当前仅 kill_switch）。"""
    _init_autonomy_config()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            row = conn.execute(
                "SELECT value FROM autonomy_config WHERE key='kill_switch'"
            ).fetchone()
            enabled = (row[0] if row else "0") != "1"
            return {"ok": True, "kill_switch": enabled}  # kill_switch=true 表示允许自治
        finally:
            conn.close()


def set_autonomy_config(kill_switch: bool) -> Dict[str, Any]:
    """设置自治闭环开关：kill_switch=true 允许自治，false 熔断（拒绝新建）。"""
    _init_autonomy_config()
    with _LOCK:
        conn = sqlite3.connect(_DB_PATH)
        try:
            conn.execute(
                "INSERT INTO autonomy_config(key, value) VALUES('kill_switch', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 ("0" if kill_switch else "1",),
            )
            conn.commit()
            return {"ok": True, "kill_switch": kill_switch}
        finally:
            conn.close()

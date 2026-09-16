# -*- coding: utf-8 -*-
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 fiyo (Jack Ge) <sdfiyon@gmail.com>
# Author: fiyo (Jack Ge) - https://github.com/fiyo/DBCheck

"""
监控大屏历史回放存储层（P2-4）。

设计约束（硬性）：只存「非敏感数据」。

- 底层 SQLite 单文件：<repo>/data/monitor_history.db
  （data/ 为运行时目录、已在 .gitignore 忽略，绝不进版本库）
- 表 `samples` 仅包含白名单列（见 _COLS）。任何快照里即便带了
  host / port / label / ssh / err / name / counters 等敏感字段，
  经过 `_project()` 白名单投影后一律被丢弃（双重保险见 _SENSITIVE）。
- 写入走内存缓冲 + 独立 1s flush 线程，不阻塞采集热路径。
- 默认留存 30 天，启动 + 每小时滚动清理。
- 历史库只存「不透明 iid」，展示用的实例名由实时已授权实例列表按 iid
  关联，绝不持久化实例名/地址。

合规护栏（关键）：
1. 写入侧 _COLS 硬白名单，投影时显式取键，凡不在白名单一律不写；
2. _SENSITIVE 黑名单二次过滤，即便误带敏感键也不会落库；
3. 单测（test_history_store.py）断言落库列集合 == 白名单集合，且
   host/port/label/ssh/err/name 任一都不出现在任何列/JSON 中。
"""

import os
import time
import json
import threading
import sqlite3
from collections import deque

# 仓库根：modules/monitor → modules → repo_root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(_REPO_ROOT, 'data', 'monitor_history.db')

RETENTION_DAYS = 30
FLUSH_INTERVAL = 1.0

# ── 非敏感白名单列（唯一允许进库的数据）──
_COLS = [
    'iid', 'ts', 'db_type', 'grp', 'status',
    'qps', 'tps', 'cache_hit_pct', 'lock_waits', 'repl_lag_s',
    'conn_total', 'conn_max', 'conn_usage_pct',
    'conn_active', 'conn_idle', 'conn_blocked',
    'slowq', 'tbs_free_pct', 'tbs_json', 'spark_json',
]
_COL_TYPES = {
    'iid': 'TEXT', 'ts': 'REAL', 'db_type': 'TEXT', 'grp': 'TEXT', 'status': 'TEXT',
    'qps': 'REAL', 'tps': 'REAL', 'cache_hit_pct': 'REAL', 'lock_waits': 'INTEGER', 'repl_lag_s': 'REAL',
    'conn_total': 'INTEGER', 'conn_max': 'INTEGER', 'conn_usage_pct': 'REAL',
    'conn_active': 'INTEGER', 'conn_idle': 'INTEGER', 'conn_blocked': 'INTEGER',
    'slowq': 'INTEGER', 'tbs_free_pct': 'REAL', 'tbs_json': 'TEXT', 'spark_json': 'TEXT',
}
# 敏感字段硬黑名单（投影时即便误带也会被剔除，双重保险）
_SENSITIVE = {'host', 'port', 'label', 'ssh', 'err', 'name', 'counters', 'stat_hint'}


def _project(snap):
    """白名单投影：仅取非敏感字段，构造一行待入库 dict。

    snap 可能携带 host/port/label/ssh/err/name/counters 等敏感字段，
    这里只挑 _COLS 里的键，故敏感字段天然被排除。"""
    conn = snap.get('conn') or {}
    tbs = snap.get('tbs') or []
    worst = None
    for t in tbs:
        fp = t.get('free_pct')
        if fp is not None and (worst is None or fp < worst):
            worst = fp
    row = {
        'iid': snap.get('id'),
        'ts': snap.get('ts'),
        'db_type': snap.get('db_type'),
        'grp': snap.get('group'),
        'status': snap.get('status'),
        'qps': snap.get('qps'),
        'tps': snap.get('tps'),
        'cache_hit_pct': snap.get('cache_hit_pct'),
        'lock_waits': snap.get('lock_waits'),
        'repl_lag_s': snap.get('repl_lag_s'),
        'conn_total': conn.get('total'),
        'conn_max': conn.get('max_conn'),
        'conn_usage_pct': conn.get('usage_pct'),
        'conn_active': conn.get('active'),
        'conn_idle': conn.get('idle'),
        'conn_blocked': conn.get('blocked'),
        'slowq': snap.get('slowq'),
        'tbs_free_pct': worst,
        'tbs_json': json.dumps(tbs, ensure_ascii=False),
        'spark_json': json.dumps(snap.get('spark') or [], ensure_ascii=False),
    }
    # 双重保险：只保留白名单列，剔除任何误带的敏感键
    return {k: row[k] for k in _COLS}


class HistoryStore:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._buf = deque()
        self._stop = False
        self._last_purge = 0
        self._init_db()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()

    def _conn(self):
        c = sqlite3.connect(self.db_path, check_same_thread=False)
        c.execute('PRAGMA journal_mode=WAL')
        return c

    def _init_db(self):
        with self._conn() as c:
            cols = ', '.join('%s %s' % (k, _COL_TYPES[k]) for k in _COLS)
            c.execute('CREATE TABLE IF NOT EXISTS samples (%s)' % cols)
            c.execute('CREATE INDEX IF NOT EXISTS idx_samples_iid_ts ON samples(iid, ts)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts)')
        self.purge_older_than(RETENTION_DAYS)

    def write_snap(self, snap):
        """非阻塞写入：仅做白名单投影 + 入内存缓冲，由 flush 线程落库。"""
        if not snap or 'id' not in snap:
            return
        if snap.get('status') == 'pending':
            return  # pending 占位无真实指标，不入库
        try:
            row = _project(snap)
        except Exception as e:
            print('[history] project failed: %s' % e, flush=True)
            return
        with self._lock:
            self._buf.append(row)

    def _flush_loop(self):
        while not self._stop:
            time.sleep(FLUSH_INTERVAL)
            try:
                self._flush()
            except Exception as e:
                print('[history] flush loop error: %s' % e, flush=True)
            now = time.time()
            if now - self._last_purge > 3600:  # 每小时清理一次
                try:
                    self.purge_older_than(RETENTION_DAYS)
                except Exception:
                    pass
                self._last_purge = now

    def _flush(self):
        with self._lock:
            if not self._buf:
                return
            batch = list(self._buf)
            self._buf.clear()
        try:
            placeholders = ','.join(['?'] * len(_COLS))
            cols = ','.join(_COLS)
            with self._conn() as c:
                c.executemany(
                    'INSERT INTO samples (%s) VALUES (%s)' % (cols, placeholders),
                    [[row.get(k) for k in _COLS] for row in batch])
        except Exception as e:
            print('[history] flush failed: %s' % e, flush=True)
            # 落库失败不重试堆积（避免内存膨胀），仅告警

    def query(self, from_ts=None, to_ts=None, iid=None, limit=5000):
        where = []
        args = []
        if from_ts is not None:
            where.append('ts >= ?'); args.append(from_ts)
        if to_ts is not None:
            where.append('ts <= ?'); args.append(to_ts)
        if iid is not None:
            where.append('iid = ?'); args.append(iid)
        sql = 'SELECT %s FROM samples' % ','.join(_COLS)
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' ORDER BY ts ASC'
        if limit:
            sql += ' LIMIT %d' % int(limit)
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(sql, args).fetchall()
        return [dict(r) for r in rows]

    def range_info(self):
        with self._conn() as c:
            row = c.execute(
                'SELECT MIN(ts) AS mn, MAX(ts) AS mx, COUNT(*) AS n FROM samples').fetchone()
        return {'min_ts': row['mn'], 'max_ts': row['mx'], 'count': row['n']}

    def purge_older_than(self, days=RETENTION_DAYS):
        cutoff = time.time() - days * 86400.0
        try:
            with self._conn() as c:
                c.execute('DELETE FROM samples WHERE ts < ?', (cutoff,))
        except Exception as e:
            print('[history] purge failed: %s' % e, flush=True)

    def stop(self):
        self._stop = True
        try:
            self._flush()
        except Exception:
            pass


_store = None
_store_lock = threading.Lock()


def get_history_store():
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = HistoryStore()
    return _store


def reset_for_test(db_path):
    """测试用：用指定路径重建单例。"""
    global _store
    with _store_lock:
        if _store is not None:
            try:
                _store.stop()
            except Exception:
                pass
        _store = HistoryStore(db_path=db_path)
    return _store

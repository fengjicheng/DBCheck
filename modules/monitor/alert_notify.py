# coding: utf-8
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 fiyo (Jack Ge) <sdfiyon@gmail.com>
# Author: fiyo (Jack Ge) - https://github.com/fiyo/DBCheck

"""
监控告警通知（状态机去重）
==========================
大屏采样器每轮产出各实例快照（status: ok/warn/crit/down/unsupported），
本模块按实例跟踪告警状态，只在「状态迁移」时经既有通知配置分发：

- 无告警 → 有告警：发送告警通知（含主机、库名、告警原因）
- 告警等级变化（warn→crit→down 等）：发送变化通知
- 有告警 → 恢复正常：发送恢复通知
- 状态不变：静默（同一告警绝不重复发送）

配置（dbc_config.json → notification.monitor_alert，可省略）：
    {"enabled": true, "min_level": "warn"}   # min_level: warn|crit
- enabled 默认 true；min_level 默认 warn（crit 则仅严重/宕机才通知）
- 邮件走 notification.email，IM 走 notification.webhook（设置页已可配）

防刷屏约定：
- 服务启动首轮：已处于告警的实例合并发一封「启动告警摘要」（不逐个刷屏）
- 新实例首采只记录状态不发送；同一状态每轮静默（绝不重复发送）
- 实例被删除时若处于告警，直接清状态（不发恢复）
"""

import threading
import datetime

# 大屏状态 → 通知等级（None = 正常不通知）
_LEVEL_RANK = {'ok': 0, 'unsupported': 0, 'warn': 1, 'crit': 2, 'down': 3}
_LEVEL_WORD = {'warn': '警告', 'crit': '严重', 'down': '宕机'}


def _cfg():
    """读取 monitor_alert 配置节（缺省 enabled=True / min_level='warn'）。"""
    try:
        from modules.notify import _load_config
        c = (_load_config().get('monitor_alert') or {})
    except Exception:
        c = {}
    return {
        'enabled': c.get('enabled', True),
        'min_level': c.get('min_level', 'warn'),
    }


def _summarize(s):
    """把快照压成一短语（用于主题，一眼明确问题）。"""
    if s.get('status') == 'down':
        err = str(s.get('err') or '连接失败').splitlines()[0]
        return err[:40]
    parts = []
    conn_util = (s.get('conn') or {}).get('usage_pct') or 0
    if conn_util >= 80:
        parts.append('连接利用率 %d%%' % round(conn_util))
    worst_tbs, worst_name = None, ''
    for t in (s.get('tbs') or []):
        fp = t.get('free_pct')
        if fp is not None and (worst_tbs is None or fp < worst_tbs):
            worst_tbs, worst_name = fp, t.get('name') or ''
    if worst_tbs is not None and worst_tbs <= 30:
        parts.append('表空间 %s 剩余 %d%%' % (worst_name, round(worst_tbs)))
    lag = s.get('repl_lag_s')
    if lag is not None and lag >= 30:
        parts.append('复制延迟 %ds' % lag)
    locks = s.get('lock_waits') or 0
    if locks >= 10:
        parts.append('锁等待 %d 个' % locks)
    if not parts:
        parts.append('状态 ' + str(s.get('status')))
    return '，'.join(parts)


def _detail_lines(s):
    """通知正文明细行（markdown/HTML 共用的纯文本行）。"""
    lines = ['状态: ' + _LEVEL_WORD.get(s.get('status'), s.get('status'))]
    if s.get('err'):
        lines.append('错误: ' + str(s['err']).splitlines()[0][:200])
    conn = s.get('conn') or {}
    if conn.get('usage_pct') is not None:
        lines.append('连接: %s/%s（利用率 %d%%）' % (
            conn.get('total', '-'), conn.get('max_conn', '-'),
            round(conn.get('usage_pct') or 0)))
    for t in (s.get('tbs') or []):
        if t.get('free_pct') is not None and t['free_pct'] <= 30:
            lines.append('表空间 %s 剩余 %d%%' % (t.get('name') or '-', round(t['free_pct'])))
    if s.get('repl_lag_s') is not None and s['repl_lag_s'] >= 30:
        lines.append('复制延迟: %ds' % s['repl_lag_s'])
    if (s.get('lock_waits') or 0) >= 10:
        lines.append('锁等待: %d 个' % s['lock_waits'])
    return lines


def _render_html(inst_line, level_word, detail_lines, since_ts, recovered=False):
    dur = _duration(datetime.datetime.now().timestamp() - since_ts) if since_ts else ''
    rows = ''.join(
        '<tr><td style="padding:6px 12px;border:1px solid #ddd;" colspan="2">%s</td></tr>'
        % d for d in detail_lines)
    color = '#1a7f37' if recovered else '#c62828'
    head = '已恢复正常' if recovered else '触发告警'
    dur_note = ('持续异常 %s，现已恢复。' % dur) if recovered else ('发生时间: %s' % _now_str())
    return (
        '<h2 style="margin:0 0 4px;color:%s;">DBCheck 告警通知 - %s</h2>'
        '<p style="margin:0 0 12px;color:#555;">%s</p>'
        '<p style="margin:0 0 8px;font-family:Arial,sans-serif;"><b>实例</b>: %s</p>'
        '<table style="border-collapse:collapse;font-family:Arial,sans-serif;">%s</table>'
        '<p style="margin-top:12px;color:#555;">%s</p>'
    ) % (color, level_word, head, inst_line, rows, dur_note)


def _duration(sec):
    sec = int(max(sec, 0))
    if sec >= 3600:
        return '%d小时%d分' % (sec // 3600, sec % 3600 // 60)
    if sec >= 60:
        return '%d分%d秒' % (sec // 60, sec % 60)
    return '%d秒' % sec


def _now_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class AlertTracker:
    """按实例的告警状态机（线程安全）。"""

    def __init__(self):
        self._lock = threading.Lock()
        # iid → {'level': 'ok'|'warn'|'crit'|'down', 'since': ts|None}
        # level='ok' 表示正常；since 为进入告警的时间（恢复通知算持续时长）
        self._state = {}
        # 服务启动后的首轮采样：已处于告警的实例合并发一封摘要（不逐个刷屏）
        self._first_round = True

    def update(self, snaps):
        """每轮采样后评估：snaps 为本轮全部实例快照 dict 列表。

        状态迁移即触发通知（异步线程发送，不阻塞采集循环）。
        """
        cfg = _cfg()
        if not cfg['enabled']:
            return
        min_rank = _LEVEL_RANK.get(cfg['min_level'], 1)
        events = []   # (kind, iid, snap|None, prev_state)
        startup_alerts = []   # 首轮已处于告警的快照（合并为一封启动摘要）
        with self._lock:
            first = self._first_round
            seen = set()
            for s in snaps:
                iid = s.get('id')
                if not iid:
                    continue
                seen.add(iid)
                level = s.get('status')
                rank = _LEVEL_RANK.get(level, 0)
                alerting = rank >= max(min_rank, 1)
                prev = self._state.get(iid)
                if prev is None:
                    # 首采只记录不逐个发送；首轮已告警的实例进启动摘要
                    self._state[iid] = self._mk_state(level, alerting)
                    if first and alerting:
                        startup_alerts.append(s)
                    continue
                was_alert = prev['level'] != 'ok'
                if was_alert and not alerting:
                    events.append(('recover', iid, s, prev))     # 告警 → 恢复
                    self._state.pop(iid, None)
                elif not was_alert and alerting:
                    events.append(('alert', iid, s, None))       # 正常 → 告警
                    self._state[iid] = self._mk_state(level, True)
                elif was_alert and alerting and prev['level'] != level:
                    events.append(('change', iid, s, prev))      # 等级变化（warn→crit 等）
                    self._state[iid] = {'level': level, 'since': prev['since']}
                # 其余：状态不变 → 静默（同一告警不重复发送）
            # 已删除实例：清状态（处于告警也不发恢复）
            for gone in [k for k in self._state if k not in seen]:
                self._state.pop(gone, None)
            self._first_round = False
        for ev in events:
            threading.Thread(target=self._dispatch, args=ev, daemon=True,
                             name='alert-notify').start()
        if first and startup_alerts:
            threading.Thread(target=self._dispatch_startup, args=(startup_alerts,),
                             daemon=True, name='alert-notify-startup').start()

    @staticmethod
    def _mk_state(level, alerting):
        return {'level': level if alerting else 'ok',
                'since': datetime.datetime.now().timestamp() if alerting else None}

    # ── 通知分发 ──
    def _dispatch(self, kind, iid, snap, prev):
        try:
            self._notify(kind, iid, snap, prev)
        except Exception as e:  # noqa: BLE001
            print('[alert] 通知发送失败 %s: %s' % (snap.get('label', iid), e), flush=True)

    def _dispatch_startup(self, snaps):
        """服务启动摘要：首轮已处于告警的实例合并为一封（不逐个刷屏）。"""
        try:
            self._notify_startup(snaps)
        except Exception as e:  # noqa: BLE001
            print('[alert] 启动摘要发送失败: %s' % e, flush=True)

    def _notify_startup(self, snaps):
        from modules.notify import EmailNotifier, WebhookNotifier, _load_config
        cfg = _load_config()
        rows = []
        for s in snaps:
            name = s.get('name') or s.get('label') or str(s.get('id'))
            addr = '%s:%s' % (s.get('host') or '?', s.get('port') or '?')
            word = _LEVEL_WORD.get(s.get('status'), str(s.get('status')))
            detail = str(s.get('err') or '').splitlines()[0][:80] if s.get('err') else ''
            rows.append((name, addr, word, detail))

        n = len(rows)
        subject = '[DBCheck][告警] 服务启动时 %d 个实例处于异常状态' % n
        md_lines = ['### DBCheck 启动告警摘要', '- **异常实例**: %d 个' % n]
        md_lines += ['- **%s（%s）** %s%s' % (nm, ad, wd, (' — ' + dt) if dt else '')
                     for nm, ad, wd, dt in rows]
        md = '\n'.join(md_lines)
        tr_rows = ''.join(
            '<tr><td style="padding:6px 12px;border:1px solid #ddd;">%s</td>'
            '<td style="padding:6px 12px;border:1px solid #ddd;">%s</td></tr>'
            % ('%s（%s）' % (nm, ad),
               '<span style="color:#c62828;font-weight:bold;">%s</span>%s'
               % (wd, (' — ' + dt) if dt else ''))
            for nm, ad, wd, dt in rows)
        html = (
            '<h2 style="margin:0 0 4px;color:#c62828;">DBCheck 启动告警摘要</h2>'
            '<p style="margin:0 0 12px;color:#555;">服务启动时以下 %d 个实例处于异常状态：</p>'
            '<table style="border-collapse:collapse;font-family:Arial,sans-serif;">%s</table>'
            '<p style="margin-top:12px;color:#555;">时间: %s</p>'
        ) % (n, tr_rows, _now_str())

        ecfg = cfg.get('email') or {}
        if ecfg.get('host') and ecfg.get('user') and ecfg.get('recipients'):
            ok, err = EmailNotifier(ecfg).send_alert_mail(subject, html)
            if not ok:
                print('[alert] 启动摘要邮件发送失败: %s' % err, flush=True)
        wcfg = cfg.get('webhook') or {}
        if wcfg.get('url'):
            if not WebhookNotifier(wcfg).send_markdown(subject, md):
                print('[alert] 启动摘要 Webhook 发送失败', flush=True)
        print('[alert] %s' % subject, flush=True)

    def _notify(self, kind, iid, snap, prev):
        from modules.notify import EmailNotifier, WebhookNotifier, _load_config
        cfg = _load_config()
        name = snap.get('name') or snap.get('label') or str(iid)
        addr = '%s:%s' % (snap.get('host') or '?', snap.get('port') or '?')
        db_type = snap.get('db_type') or ''
        level = snap.get('status')
        word = _LEVEL_WORD.get(level, str(level))
        summary = _summarize(snap)

        if kind == 'recover':
            since = prev['since'] if prev else None
            subject = '[DBCheck][恢复] %s(%s) 已恢复正常' % (name, addr)
            md = ('### DBCheck 恢复通知\n- **实例**: %s（%s / %s）\n- **状态**: 已恢复正常\n- **持续异常**: %s\n- **时间**: %s'
                  % (name, db_type, addr, _duration(datetime.datetime.now().timestamp() - since) if since else '-',
                     _now_str()))
            html = _render_html('%s（%s / %s）' % (name, db_type, addr), word,
                                ['状态: 已恢复正常'], since or 0, recovered=True)
        else:
            subject = '[DBCheck][%s] %s(%s) %s' % (word, name, addr, summary)
            detail = _detail_lines(snap)
            action = {'alert': '触发告警', 'change': '告警变化（%s → %s）' % (
                _LEVEL_WORD.get(prev['level'], prev['level']), word) if prev else '告警变化'}.get(kind, '告警')
            md_lines = ['### DBCheck 告警通知 - %s' % word, '- **实例**: %s（%s / %s）' % (name, db_type, addr)]
            md_lines += ['- %s' % d for d in detail]
            md_lines += ['- **%s**' % action, '- **时间**: %s' % _now_str()]
            md = '\n'.join(md_lines)
            html = _render_html('%s（%s / %s）' % (name, db_type, addr), word, detail, 0)

        # 邮件（配置齐全才发）
        ecfg = cfg.get('email') or {}
        if ecfg.get('host') and ecfg.get('user') and ecfg.get('recipients'):
            ok, err = EmailNotifier(ecfg).send_alert_mail(subject, html)
            if not ok:
                print('[alert] 邮件发送失败 %s: %s' % (name, err), flush=True)
        # IM Webhook（企业微信/钉钉/自定义）
        wcfg = cfg.get('webhook') or {}
        if wcfg.get('url'):
            if not WebhookNotifier(wcfg).send_markdown(subject, md):
                print('[alert] Webhook 发送失败 %s' % name, flush=True)
        print('[alert] %s' % subject, flush=True)


_tracker = None
_tracker_lock = threading.Lock()


def get_alert_tracker():
    global _tracker
    with _tracker_lock:
        if _tracker is None:
            _tracker = AlertTracker()
        return _tracker

# coding: utf-8
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 fiyo (Jack Ge) <sdfiyon@gmail.com>
# Author: fiyo (Jack Ge) - https://github.com/fiyo/DBCheck

"""
Oracle 连接测试独立模块（纯函数，无 Flask / 线程副作用）。

从 ``modules.web.app`` 抽离，供两处共用：
- 连接测试子进程 ``modules.jdbc_test_cli``（oracle 分支）在未被 gevent
  monkey-patch 的干净子进程内执行；
- 主进程 ``modules.web.app``（regular / pro 数据源测试、thick 回退）。

抽离目的：避免在「测试连接」子进程里 ``import modules.web.app`` 连带加载
整个 Flask 应用（Flask / flask-socketio / 调度器 / Redis 管控 / 控制台保活
线程等），既拖慢子进程启动，又可能在非服务上下文里触发不必要的副作用。
本模块仅依赖标准库与 ``modules.core.paths``、惰性导入 ``oracledb`` /
``modules.ssh``，无任何模块级副作用。
"""

import os
import sys
import json

from modules.core.paths import PROJECT_ROOT

# 与 modules.web.app 保持完全一致的 BASE_DIR 语义：
# 开发模式用项目根目录；PyInstaller 打包后用 exe 所在目录
# （frozen 下 BASE_DIR=<exe>，真实运行时数据目录是 <exe>/_internal）。
if getattr(sys, "frozen", False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = str(PROJECT_ROOT)


def _find_oracle_client_lib_dir(platform_key=None):
    """查找 Oracle Client 的 lib 目录，支持根目录和 lib/ 子目录

    搜索顺序：
    1. drivers/oracle_client/<platform>/ 根目录
    2. drivers/oracle_client/<platform>/lib/ 子目录
    3. drivers/oracle_client/<platform>/ 下递归搜索标记文件

    Returns:
        str or None: 找到的 lib 目录路径，如果没找到返回 None
    """
    import platform as _pl
    if platform_key is None:
        sys_name = _pl.system().lower()
        arch = _pl.machine().lower()
        if sys_name == 'windows':
            platform_key = 'windows_x64'
        elif sys_name == 'linux':
            platform_key = 'linux_x64'
        elif sys_name == 'darwin':
            platform_key = 'darwin_arm64' if arch in ('arm64', 'aarch64') else 'darwin_x64'
        else:
            return None

    # 确定标记文件
    if 'windows' in platform_key:
        marker = 'oci.dll'
    elif 'linux' in platform_key:
        marker = 'libclntsh.so'
    else:
        marker = 'libclntsh.dylib'

    client_dir = os.path.join(BASE_DIR, 'drivers', 'oracle_client', platform_key)

    if not os.path.isdir(client_dir):
        return None

    # 1. 先检查根目录
    if os.path.isfile(os.path.join(client_dir, marker)):
        return client_dir

    # 2. 再检查 lib/ 子目录（Oracle 完整客户端常见结构）
    lib_dir = os.path.join(client_dir, 'lib')
    if os.path.isdir(lib_dir) and (
        os.path.isfile(os.path.join(lib_dir, marker)) or
        os.path.isfile(os.path.join(lib_dir, marker + '.11.1')) or
        any(f.startswith(marker) for f in os.listdir(lib_dir) if marker in f)
    ):
        return lib_dir

    # 3. 自动发现子目录（递归搜索，处理 instantclient_xx_x 子目录）
    for root, dirs, files in os.walk(client_dir):
        if marker in files:
            return root

    return None


def _normalize_oracle_dsn(dsn, host, port):
    """规范化 Oracle TNS 描述符中的占位符。

    用户常把 DSN 写成 ``(DESCRIPTION=(HOST=host)(PORT=1521)(CONNECT_DATA=...))``，
    其中 ``HOST=host`` 是占位符，需要替换为表单中填写的真实地址。若 DSN 里
    已经写了明确的非占位符地址，则保持不变。
    """
    import re as _re

    if not (dsn and str(dsn).strip().upper().startswith('(DESCRIPTION')):
        return dsn

    def _replace_host(m):
        val = m.group(1).strip().lower()
        if val in ('host', 'localhost', '127.0.0.1', ''):
            return f'(HOST={host})'
        return m.group(0)

    def _replace_port(m):
        val = m.group(1).strip()
        if val.lower() == 'port':
            return f'(PORT={port})'
        try:
            int(val)
        except (TypeError, ValueError):
            return f'(PORT={port})'
        return m.group(0)

    dsn = _re.sub(r'\(HOST\s*=\s*([^)]*)\)', _replace_host, dsn, flags=_re.IGNORECASE)
    dsn = _re.sub(r'\(PORT\s*=\s*([^)]*)\)', _replace_port, dsn, flags=_re.IGNORECASE)
    return dsn


def _ct_oracle_pro(data):
    """复刻原 /api/pro/datasources/test-connection 中 oracle 分支（含 SSH / thick mode）。

    修复点：
    1. 用户填写 DSN 描述符且启用 SSH 时，必须把 DSN 里的 HOST/PORT 替换为 SSH 隧道
       本地端口，否则 oracledb 仍按原 DSN 直连，导致"配了 SSH 仍超时"。
    2. 未启用 SSH 时，DSN 里的占位符（如 HOST=host）也要替换为表单填写的真实地址，
       否则会出现"填了主机地址却仍连 host"的误导性超时。
    3. 隧道本地监听 127.0.0.1，DSN 用 127.0.0.1 避免 localhost 被解析到 IPv6。
    4. 错误提示区分"未配 SSH"、"已配 SSH 但隧道后仍连不上"以及 DSN 占位符问题。
    5. thick mode 失败等异常路径也确保关闭 SSH 隧道。

    注：本函数已抽离至独立模块，主进程与「测试连接」子进程共用，避免子进程
    加载整个 Flask 应用。
    """
    import oracledb
    import re as _re

    _jdbc = (data.get('jdbc_url') or '').strip()
    _has_dsn = bool(_jdbc and _jdbc.lstrip().upper().startswith('(DESCRIPTION'))
    if _has_dsn:
        # 先把 DSN 里的占位符（HOST=host / PORT=port）换成表单真实地址
        dsn = _normalize_oracle_dsn(_jdbc, data['host'], int(data['port']))
        # 防御性检查：若还有明显占位符未替换，立即给出明确错误，避免连到不存在的 host
        if _re.search(r'\(HOST\s*=\s*host\s*\)', dsn, flags=_re.IGNORECASE):
            return {'ok': False, 'error': f'DSN 中 HOST 占位符未解析（当前 DSN: {dsn}），请检查连接串或清空 DSN 使用上方主机地址'}
    else:
        dsn = f"{data['host']}:{data['port']}/{data.get('service_name', '')}" if data.get('service_name') \
            else f"{data['host']}:{data['port']}"

    ssh_host = data.get('ssh_host', '')
    _tunnel = None
    try:
        if ssh_host:
            try:
                from modules.ssh import SSHTunnel
                _tunnel = SSHTunnel(
                    ssh_host=ssh_host,
                    ssh_port=int(data.get('ssh_port', 22)),
                    ssh_user=data.get('ssh_user', 'root'),
                    ssh_password=data.get('ssh_password', ''),
                    remote_host=data['host'],
                    remote_port=int(data['port']),
                )
                _tunnel.__enter__()
                _local = _tunnel.local_port
                if _has_dsn:
                    # 把 DSN 中的 HOST/PORT 替换为隧道本地端点，确保流量走 SSH
                    dsn = _re.sub(r'\(HOST\s*=\s*[^)]+\)', '(HOST=127.0.0.1)', dsn, flags=_re.IGNORECASE)
                    dsn = _re.sub(r'\(PORT\s*=\s*\d+\)', f'(PORT={_local})', dsn, flags=_re.IGNORECASE)
                else:
                    dsn = f"127.0.0.1:{_local}/{data.get('service_name', '')}" if data.get('service_name') \
                        else f"127.0.0.1:{_local}"
            except Exception as te:
                return {'ok': False, 'error': f'SSH 隧道建立失败: {te}'}

        params = {"user": data['user'], "password": data['password'], "dsn": dsn,
                  "tcp_connect_timeout": 15}
        if data.get('sysdba'):
            params["mode"] = oracledb.SYSDBA

        def _try_connect():
            try:
                conn = oracledb.connect(**params)
            except TypeError as te:
                # 极个别 oracledb 旧版本不认 tcp_connect_timeout，去掉后重试（子进程超时仍兜底）
                if 'tcp_connect_timeout' in str(te):
                    params.pop('tcp_connect_timeout', None)
                    conn = oracledb.connect(**params)
                else:
                    raise
            conn.close()

        try:
            _try_connect()
            return {'ok': True, 'message': '连接成功'}
        except Exception as e:
            err_msg = str(e)
            if 'DPY-3010' in err_msg or 'DPY-3015' in err_msg:
                _thick_ok = False
                try:
                    oracledb.init_oracle_client()
                    _thick_ok = True
                except Exception:
                    pass
                if not _thick_ok:
                    _lib_dir = _find_oracle_client_lib_dir()
                    if _lib_dir:
                        try:
                            oracledb.init_oracle_client(lib_dir=_lib_dir)
                            _thick_ok = True
                        except Exception:
                            pass
                if not _thick_ok:
                    try:
                        with open(os.path.join(BASE_DIR, 'dbc_config.json')) as f:
                            _cfg = json.load(f)
                        _lib_dir = _cfg.get('oracle_client_lib_dir', '')
                        if _lib_dir and os.path.isdir(_lib_dir):
                            oracledb.init_oracle_client(lib_dir=_lib_dir)
                            _thick_ok = True
                    except Exception:
                        pass
                if not _thick_ok:
                    return {'ok': False, 'error': 'Oracle 11g 及以下版本需要 Oracle Instant Client。'
                                                '请通过左侧导航"Oracle Client"设置页，点击"一键下载并安装"按钮自动下载安装。'}
                try:
                    _try_connect()
                    return {'ok': True, 'message': '连接成功'}
                except Exception as e2:
                    return {'ok': False, 'error': f'Oracle 连接失败（thick mode）: {e2}'}
            elif 'unexpected keyword argument' in err_msg.lower() or type(e).__name__ == 'TypeError':
                # 驱动连接参数错误（如传入了 oracledb 不认识的 kwarg），不应误判为「连接超时」
                return {'ok': False, 'error': f'Oracle 驱动连接参数错误: {type(e).__name__}: {err_msg[:300]}'}
            elif 'timed out' in err_msg.lower() or 'timeout' in err_msg.lower():
                # 把实际用于连接的 DSN/地址 + oracledb 原始异常暴露出来，方便排查：
                # 真 TCP 超时（DPY-6001/ORA-12170）vs 握手/权限问题被误判为超时（如 SYSDBA+服务名）
                _safe_dsn = _re.sub(r'(PASSWORD\s*=\s*)[^)]+', r'\1***', str(dsn),
                                    flags=_re.IGNORECASE) if isinstance(dsn, str) else str(dsn)
                _detail = f'（原始异常: {type(e).__name__}: {err_msg[:400]}）'
                if ssh_host:
                    return {'ok': False, 'error': f'连接超时，SSH 隧道已建立但无法访问 Oracle（实际 DSN: {_safe_dsn}）{_detail}，'
                                                f'请检查数据库监听地址、Service Name/SID 及防火墙'}
                return {'ok': False, 'error': f'连接超时，Oracle 可能无法直连（实际 DSN: {_safe_dsn}）{_detail}，'
                                              f'请在数据源中配置 SSH，或确认上方主机地址/端口/服务名/SYSDBA 是否正确'}
            else:
                return {'ok': False, 'error': str(e)}
    finally:
        if _tunnel:
            _tunnel.close()

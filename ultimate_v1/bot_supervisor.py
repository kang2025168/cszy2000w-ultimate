from __future__ import annotations

"""机器人进程主管：网页开关打开就启动，关闭就停止。"""

import subprocess
import sys
import time
from dataclasses import dataclass
from os import environ
from pathlib import Path

from .config import settings
from .state_store import bot_controls, heartbeat, log_bot_lifecycle, set_bot_enabled


@dataclass(frozen=True)
class BotSpec:
    module: str
    args: tuple[str, ...]
    env: dict[str, str] | None = None


BOT_SPECS: dict[str, BotSpec] = {
    "dashboard_bot": BotSpec("app.bots.dashboard_bot", ("--loop", "--interval", str(settings().position_sync_interval_sec))),
    "risk_bot": BotSpec("app.bots.risk_bot", ("--loop", "--interval", "60")),
    "rebalance_bot": BotSpec("app.bots.rebalance_bot", ("--loop", "--interval", environ.get("REBALANCE_INTERVAL_SEC", "900"))),
    "quick_trade_bot": BotSpec("app.bots.quick_trade_bot", ("--loop", "--interval", environ.get("QUICK_BOT_INTERVAL_SEC", "5"))),
    "ac_bot": BotSpec("app.bots.ac_bot", ("scan", "--loop", "--interval", "300")),
    # B/F 买卖各自用独立入口进程，底层复用 split_core 调度但策略互不混跑。
    "b_buy_bot": BotSpec(
        "app.bots.b_buy_bot",
        (),
        {
            "SPLIT_BOT_FORCE_PHASE": "regular",
            "ALLOW_LIVE_FORCE_PHASE": "1",
        },
    ),
    "b_sell_bot": BotSpec(
        "app.bots.b_sell_bot",
        (),
        {
            "SPLIT_BOT_FORCE_PHASE": "regular",
            "ALLOW_LIVE_FORCE_PHASE": "1",
        },
    ),
    "f_buy_bot": BotSpec(
        "app.bots.f_buy_bot",
        (),
    ),
    "f_sell_bot": BotSpec(
        "app.bots.f_sell_bot",
        (),
    ),
    "d_buy_bot": BotSpec("app.bots.d_buy_bot", ("--loop", "--interval", "30")),
    "d_sell_bot": BotSpec("app.bots.d_sell_bot", ("--loop", "--interval", "30")),
    "q_sell_bot": BotSpec("app.bots.q_sell_bot", ("--loop", "--interval", "30")),
}

_PROCESSES: dict[str, subprocess.Popen] = {}
_LOG_HANDLES: dict[str, object] = {}


def managed_bot_names() -> set[str]:
    """返回网页可控的机器人名称。"""
    return set(BOT_SPECS)


def _process_running(proc: subprocess.Popen | None) -> bool:
    return bool(proc and proc.poll() is None)


def start_bot(bot_name: str) -> bool:
    """启动一个机器人进程。"""
    spec = BOT_SPECS.get(bot_name)
    if not spec:
        raise ValueError(f"不支持的机器人: {bot_name}")
    proc = _PROCESSES.get(bot_name)
    if _process_running(proc):
        heartbeat(bot_name, "running", "机器人已经运行")
        log_bot_lifecycle(bot_name, "START", "ALREADY_RUNNING", "机器人已经运行", proc.pid if proc else None)
        return True
    cmd = [sys.executable, "-u", "-m", spec.module, *spec.args]
    heartbeat(bot_name, "starting", "正在启动机器人")
    log_bot_lifecycle(bot_name, "START", "STARTING", "正在启动机器人")
    child_env = dict(environ)
    if spec.env:
        child_env.update(spec.env)
    log_dir = Path(child_env.get("LOG_DIR") or child_env.get("BOT_LOG_DIR") or "/tmp/logs")
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        log_dir = Path("/tmp")
    trade_env = (child_env.get("TRADE_ENV") or child_env.get("ALPACA_MODE") or "paper").strip().lower()
    log_path = log_dir / f"AAA_{bot_name}_{trade_env}.log"
    log_handle = open(log_path, "a", encoding="utf-8", buffering=1)
    _LOG_HANDLES[bot_name] = log_handle
    proc = subprocess.Popen(cmd, env=child_env, stdout=log_handle, stderr=subprocess.STDOUT)
    _PROCESSES[bot_name] = proc
    time.sleep(0.2)
    if proc.poll() is not None:
        try:
            log_handle.close()
        except Exception:
            pass
        _LOG_HANDLES.pop(bot_name, None)
        heartbeat(bot_name, "failed", f"机器人启动后退出 returncode={proc.returncode}")
        log_bot_lifecycle(bot_name, "START", "FAILED", f"机器人启动后退出 returncode={proc.returncode}", proc.pid)
        print(f"[BOT SUPERVISOR] {bot_name} exited immediately returncode={proc.returncode}", flush=True)
        return False
    heartbeat(bot_name, "running", f"机器人已启动 pid={proc.pid}")
    log_bot_lifecycle(bot_name, "START", "RUNNING", "机器人已启动", proc.pid)
    print(f"[BOT SUPERVISOR] started {bot_name} pid={proc.pid}", flush=True)
    return True


def stop_bot(bot_name: str) -> bool:
    """停止一个机器人进程。"""
    proc = _PROCESSES.get(bot_name)
    if not proc:
        heartbeat(bot_name, "stopped", "机器人已关闭")
        return True
    log_bot_lifecycle(bot_name, "STOP", "STOPPING", "正在关闭机器人", proc.pid)
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    log_handle = _LOG_HANDLES.pop(bot_name, None)
    if log_handle:
        try:
            log_handle.close()
        except Exception:
            pass
    heartbeat(bot_name, "stopped", "机器人已关闭")
    log_bot_lifecycle(bot_name, "STOP", "STOPPED", f"机器人已关闭 returncode={proc.returncode}", proc.pid)
    print(f"[BOT SUPERVISOR] stopped {bot_name}", flush=True)
    return True


def set_bot_runtime(bot_name: str, enabled: bool) -> bool:
    """写入开关，并启动或停止对应进程。"""
    set_bot_enabled(bot_name, enabled)
    if enabled:
        return start_bot(bot_name)
    stop_bot(bot_name)
    return False


def sync_from_controls() -> None:
    """网页服务启动时，根据数据库开关拉起应该运行的机器人。"""
    control_map = {row["bot_name"]: int(row.get("enabled") or 0) == 1 for row in bot_controls()}
    for bot_name in managed_bot_names():
        if control_map.get(bot_name, True):
            start_bot(bot_name)
        else:
            stop_bot(bot_name)


def process_status() -> list[dict]:
    """返回主管看到的进程状态，供网页合并显示。"""
    rows = []
    for bot_name in sorted(BOT_SPECS):
        proc = _PROCESSES.get(bot_name)
        running = _process_running(proc)
        if proc and not running:
            heartbeat(bot_name, "failed", f"进程已退出 returncode={proc.returncode}")
        rows.append(
            {
                "bot_name": bot_name,
                "pid": proc.pid if proc else None,
                "running": running,
                "returncode": None if running or not proc else proc.returncode,
            }
        )
    return rows

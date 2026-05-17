from __future__ import annotations

"""机器人进程主管：网页开关打开就启动，关闭就停止。"""

import subprocess
import sys
from dataclasses import dataclass
from os import environ

from .config import settings
from .state_store import bot_controls, heartbeat, set_bot_enabled


@dataclass(frozen=True)
class BotSpec:
    module: str
    args: tuple[str, ...]
    env: dict[str, str] | None = None


BOT_SPECS: dict[str, BotSpec] = {
    "dashboard_bot": BotSpec("app.bots.dashboard_bot", ("--loop", "--interval", str(settings().position_sync_interval_sec))),
    "risk_bot": BotSpec("app.bots.risk_bot", ("--loop", "--interval", "60")),
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
}

_PROCESSES: dict[str, subprocess.Popen] = {}


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
        return True
    cmd = [sys.executable, "-u", "-m", spec.module, *spec.args]
    heartbeat(bot_name, "starting", "正在启动机器人")
    child_env = dict(environ)
    if spec.env:
        child_env.update(spec.env)
    proc = subprocess.Popen(cmd, env=child_env)
    _PROCESSES[bot_name] = proc
    print(f"[BOT SUPERVISOR] started {bot_name} pid={proc.pid}", flush=True)
    return True


def stop_bot(bot_name: str) -> bool:
    """停止一个机器人进程。"""
    proc = _PROCESSES.get(bot_name)
    if not proc:
        heartbeat(bot_name, "stopped", "机器人已关闭")
        return True
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    heartbeat(bot_name, "stopped", "机器人已关闭")
    print(f"[BOT SUPERVISOR] stopped {bot_name}", flush=True)
    return True


def set_bot_runtime(bot_name: str, enabled: bool) -> None:
    """写入开关，并启动或停止对应进程。"""
    set_bot_enabled(bot_name, enabled)
    if enabled:
        start_bot(bot_name)
    else:
        stop_bot(bot_name)


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
        rows.append(
            {
                "bot_name": bot_name,
                "pid": proc.pid if proc else None,
                "running": running,
                "returncode": None if running or not proc else proc.returncode,
            }
        )
    return rows

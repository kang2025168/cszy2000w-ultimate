from __future__ import annotations

"""Alpaca account profiles and strategy-pool routing.

Secrets may come from environment variables or from app_settings. API responses
only expose masked keys so the dashboard can switch accounts without leaking
full credentials back into the browser.
"""

import json
from copy import deepcopy

from .config import env_str
from .state_store import get_app_setting, set_app_setting


CONFIG_KEY = "ALPACA_ACCOUNT_CONFIG"

POOL_ORDER = ("A", "B", "C", "D")
FIXED_POOL_PROFILES = {
    "A": "retirement",
    "B": "trading",
    "C": "trading",
    "D": "trading",
}
VISIBLE_PROFILES = ("retirement", "trading")

DEFAULT_CONFIG = {
    "active_profile": "trading",
    "pool_profiles": FIXED_POOL_PROFILES.copy(),
    "profiles": {
        "retirement": {
            "label": "养老金账户",
            "mode": "",
            "key_id": "",
            "secret_key": "",
            "base_url": "",
            "env_prefix": "RETIREMENT",
        },
        "trading": {
            "label": "原保证金账户",
            "mode": "",
            "key_id": "",
            "secret_key": "",
            "base_url": "",
            "env_prefix": "",
        },
    },
}


def _merge(default: dict, saved: dict) -> dict:
    out = deepcopy(default)
    if not isinstance(saved, dict):
        return out
    if str(saved.get("active_profile") or "").strip():
        out["active_profile"] = str(saved["active_profile"]).strip()
    if isinstance(saved.get("profiles"), dict):
        for key, profile in saved["profiles"].items():
            if not isinstance(profile, dict):
                continue
            key = str(key or "").strip()
            if not key:
                continue
            base = out["profiles"].setdefault(
                key,
                {"label": key, "mode": "paper", "key_id": "", "secret_key": "", "base_url": "", "env_prefix": key.upper()},
            )
            for field in ("label", "mode", "key_id", "secret_key", "base_url", "env_prefix"):
                if field in profile:
                    base[field] = str(profile.get(field) or "").strip()
    out["active_profile"] = "trading"
    out["pool_profiles"] = FIXED_POOL_PROFILES.copy()
    out["profiles"] = {key: out["profiles"][key] for key in VISIBLE_PROFILES if key in out["profiles"]}
    for key in VISIBLE_PROFILES:
        if key in out["profiles"]:
            out["profiles"][key]["mode"] = DEFAULT_CONFIG["profiles"][key].get("mode", "")
            out["profiles"][key]["env_prefix"] = DEFAULT_CONFIG["profiles"][key].get("env_prefix", "")
    return out


def load_account_config() -> dict:
    try:
        raw = get_app_setting(CONFIG_KEY, "")
    except Exception:
        raw = ""
    try:
        saved = json.loads(raw) if raw else {}
    except Exception:
        saved = {}
    return _merge(DEFAULT_CONFIG, saved)


def save_account_config(config: dict) -> dict:
    current = load_account_config()
    incoming = _merge(current, config if isinstance(config, dict) else {})
    incoming["active_profile"] = "trading"
    incoming["pool_profiles"] = FIXED_POOL_PROFILES.copy()
    incoming["profiles"] = {key: incoming["profiles"][key] for key in VISIBLE_PROFILES if key in incoming.get("profiles", {})}
    set_app_setting(CONFIG_KEY, json.dumps(incoming, ensure_ascii=False))
    return incoming


def _env_first(*names: str) -> str:
    for name in names:
        value = env_str(name, "")
        if value:
            return value
    return ""


def _profile_with_env(config: dict, profile_key: str) -> dict:
    profile = deepcopy((config.get("profiles") or {}).get(profile_key) or {})
    prefix = str(profile.get("env_prefix") or "").strip().upper()
    prefixed_mode = env_str(f"{prefix}_ALPACA_MODE", "") if prefix else ""
    mode = str(profile.get("mode") or prefixed_mode or env_str("ALPACA_MODE", "paper")).lower()
    mode_prefix = "LIVE" if mode == "live" else "PAPER"
    prefixed_key_names = []
    prefixed_secret_names = []
    prefixed_base_names = []
    if prefix:
        prefixed_key_names = [f"{prefix}_APCA_API_KEY_ID", f"{prefix}_ALPACA_KEY", f"{prefix}_ALPACA_API_KEY"]
        prefixed_secret_names = [f"{prefix}_APCA_API_SECRET_KEY", f"{prefix}_ALPACA_SECRET", f"{prefix}_ALPACA_API_SECRET"]
        prefixed_base_names = [f"{prefix}_ALPACA_BASE_URL"]
    key_id = str(profile.get("key_id") or "").strip() or _env_first(
        *prefixed_key_names,
        f"{mode_prefix}_APCA_API_KEY_ID",
        f"{mode_prefix}_ALPACA_KEY",
        "APCA_API_KEY_ID",
        "ALPACA_KEY",
    )
    secret = str(profile.get("secret_key") or "").strip() or _env_first(
        *prefixed_secret_names,
        f"{mode_prefix}_APCA_API_SECRET_KEY",
        f"{mode_prefix}_ALPACA_SECRET",
        "APCA_API_SECRET_KEY",
        "ALPACA_SECRET",
    )
    base_url = str(profile.get("base_url") or "").strip() or _env_first(*prefixed_base_names, "ALPACA_BASE_URL")
    profile.update({"mode": mode, "key_id": key_id, "secret_key": secret, "base_url": base_url})
    return profile


def profile_for_pool(pool: str | None = None, config: dict | None = None) -> str:
    config = config or load_account_config()
    group = str(pool or "").strip().upper()
    if group in POOL_ORDER:
        return str((config.get("pool_profiles") or {}).get(group) or config.get("active_profile") or "trading")
    return str(config.get("active_profile") or "trading")


def credentials_for_profile(profile_key: str | None = None, pool: str | None = None) -> tuple[str, str, bool]:
    config = load_account_config()
    selected = profile_key or profile_for_pool(pool, config)
    profile = _profile_with_env(config, selected)
    paper = str(profile.get("mode") or "paper").lower() != "live"
    return str(profile.get("key_id") or ""), str(profile.get("secret_key") or ""), paper


def _mask(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def public_account_config() -> dict:
    config = load_account_config()
    profiles = {}
    for key, profile in (config.get("profiles") or {}).items():
        full = _profile_with_env(config, key)
        profiles[key] = {
            "label": profile.get("label") or key,
            "mode": full.get("mode") or "paper",
            "key_id_mask": _mask(full.get("key_id", "")),
            "has_key": bool(full.get("key_id")),
            "has_secret": bool(full.get("secret_key")),
            "base_url": full.get("base_url") or "",
            "env_prefix": profile.get("env_prefix") or "",
        }
    return {
        "ok": True,
        "active_profile": config.get("active_profile"),
        "pool_profiles": config.get("pool_profiles") or {},
        "profiles": profiles,
    }

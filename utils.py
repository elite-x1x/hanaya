#!/usr/bin/env python3
"""
HANAYA Bot - Utilities & Helpers
"""

import os
import sys
import time
import signal
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
import requests


# ─── ANSI Colors ──────────────────────────────────────────────────────────────

class Colors:
    RESET   = '\033[0m'
    BOLD    = '\033[1m'
    RED     = '\033[91m'
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    BLUE    = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN    = '\033[96m'
    WHITE   = '\033[97m'
    DIM     = '\033[2m'

    @staticmethod
    def success(text): return f"{Colors.GREEN}✅ {text}{Colors.RESET}"
    @staticmethod
    def error(text):   return f"{Colors.RED}❌ {text}{Colors.RESET}"
    @staticmethod
    def warning(text): return f"{Colors.YELLOW}⚠️  {text}{Colors.RESET}"
    @staticmethod
    def info(text):    return f"{Colors.CYAN}ℹ️  {text}{Colors.RESET}"
    @staticmethod
    def status(text):  return f"{Colors.BLUE}📊 {text}{Colors.RESET}"
    @staticmethod
    def bold(text):    return f"{Colors.BOLD}{text}{Colors.RESET}"
    @staticmethod
    def dim(text):     return f"{Colors.DIM}{text}{Colors.RESET}"


# ─── Time Utilities ───────────────────────────────────────────────────────────

def format_uptime(start_time: Optional[datetime]) -> str:
    """Format uptime dari start_time ke sekarang"""
    if not start_time:
        return "N/A"
    delta = datetime.now() - start_time
    days    = delta.days
    hours   = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    seconds = delta.seconds % 60
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    return f"{hours}h {minutes}m {seconds}s"


def format_timestamp(dt: Optional[datetime]) -> str:
    if not dt:
        return "N/A"
    return dt.strftime("%H:%M:%S")


# ─── HTTP Health Check ────────────────────────────────────────────────────────

def check_health_endpoint(port: int, timeout: int = 5) -> Tuple[bool, str, dict]:
    """
    Check health endpoint bot.
    Returns: (is_healthy, message, raw_data)
    """
    try:
        url = f"http://localhost:{port}/health"
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            try:
                data = response.json()
                status       = data.get("status", "unknown")
                queue_size   = data.get("queue_size", 0)
                daily_count  = data.get("daily_count", 0)
                daily_limit  = data.get("daily_limit", 0)
                msg = f"{status} | Queue:{queue_size} | Daily:{daily_count}/{daily_limit}"
                return True, msg, data
            except Exception:
                return True, "ok", {}
        return False, f"HTTP {response.status_code}", {}
    except requests.exceptions.ConnectionError:
        return False, "Warming up / Not reachable", {}
    except requests.exceptions.Timeout:
        return False, "Timeout", {}
    except Exception as e:
        return False, str(type(e).__name__), {}


# ─── Log Utilities ────────────────────────────────────────────────────────────

LOG_TYPES = ["main", "network", "debug", "reload"]


def get_log_path(bot_name: str, log_type: str = "main") -> Path:
    return Path("logs") / f"{bot_name}_{log_type}.log"


def tail_log(bot_name: str, log_type: str = "main", lines: int = 50) -> List[str]:
    """Baca N baris terakhir dari log file"""
    path = get_log_path(bot_name, log_type)
    if not path.exists():
        return [f"[Log file tidak ditemukan: {path}]"]
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        return [line.rstrip() for line in all_lines[-lines:]]
    except Exception as e:
        return [f"[Error membaca log: {e}]"]


def get_log_size(bot_name: str, log_type: str = "main") -> str:
    """Dapatkan ukuran file log"""
    path = get_log_path(bot_name, log_type)
    if not path.exists():
        return "0 B"
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def clear_log(bot_name: str, log_type: str = "main") -> bool:
    """Clear isi log file"""
    path = get_log_path(bot_name, log_type)
    try:
        with open(path, "w") as f:
            f.write(f"[Log cleared at {datetime.now()}]\n")
        return True
    except Exception:
        return False


# ─── Process Utilities ────────────────────────────────────────────────────────

def kill_process_group(pid: int, sig: signal.Signals) -> bool:
    """Kill process group (Unix) atau single process (Windows)"""
    try:
        if hasattr(os, "killpg"):
            os.killpg(os.getpgid(pid), sig)
        else:
            os.kill(pid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def wait_for_port(port: int, timeout: int = 30) -> bool:
    """Tunggu sampai port tersedia"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(f"http://localhost:{port}/health", timeout=1)
            return True
        except Exception:
            time.sleep(1)
    return False


# ─── Input Utilities ─────────────────────────────────────────────────────────

def prompt(text: str, default: str = "") -> str:
    """Input prompt dengan default value"""
    suffix = f" [{default}]" if default else ""
    try:
        val = input(f"{Colors.CYAN}{text}{suffix}: {Colors.RESET}").strip()
        return val if val else default
    except (KeyboardInterrupt, EOFError):
        return default


def confirm(text: str, default: bool = False) -> bool:
    """Yes/No confirmation prompt"""
    suffix = "[Y/n]" if default else "[y/N]"
    try:
        val = input(f"{Colors.YELLOW}{text} {suffix}: {Colors.RESET}").strip().lower()
        if not val:
            return default
        return val in ("y", "yes", "ya")
    except (KeyboardInterrupt, EOFError):
        return False


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def pause(msg: str = "Tekan Enter untuk melanjutkan..."):
    try:
        input(f"\n{Colors.DIM}{msg}{Colors.RESET}")
    except (KeyboardInterrupt, EOFError):
        pass
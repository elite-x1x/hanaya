#!/usr/bin/env python3
"""
HANAYA Bot - Bot Process Loader & Manager
"""

import os
import sys
import time
import signal
import subprocess
from datetime import datetime
from typing import Optional, List, Tuple, Dict

from config import BotConfig
from utils import (
    Colors, format_uptime, format_timestamp,
    check_health_endpoint, kill_process_group
)


class BotProcess:
    """Representasi dan manajemen satu proses bot"""

    def __init__(self, config: BotConfig):
        self.config         = config
        self.process: Optional[subprocess.Popen] = None
        self.start_time: Optional[datetime]      = None
        self.is_running     = False
        self.restart_count  = 0
        self.health_status  = "unknown"
        self.health_detail  = ""
        self.last_error: Optional[str] = None
        self.last_health_check: Optional[datetime] = None

    # ── Environment ──────────────────────────────────────────────────────────

    def _build_env(self) -> dict:
        env = os.environ.copy()
        env.update({
            "BOT_NAME"            : self.config.name,
            "BOT_TOKEN"           : self.config.token,
            "TARGET_CHAT_ID"      : self.config.target_chat,
            "FLASK_PORT"          : str(self.config.flask_port),
            "ADMIN_CHAT_ID"       : self.config.admin_chat or "0",
            "SUPERADMINS"         : self.config.superadmins,
            "MODERATORS"          : self.config.moderators,
            "ALLOWED_SOURCE_CHATS": self.config.allowed_chats,
            "PYTHONUNBUFFERED"    : "1",
        })
        return env

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        """Start proses bot"""
        if self.is_running and self.process and self.process.poll() is None:
            print(Colors.warning(f"{self.config.name} sudah berjalan (PID: {self.process.pid})"))
            return True

        try:
            print(Colors.info(f"Starting {self.config.name} on port {self.config.flask_port}..."))

            self.process = subprocess.Popen(
                [sys.executable, "bot.py"],
                env=self._build_env(),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None
            )

            self.start_time = datetime.now()
            self.is_running = True
            self.restart_count += 1
            self.last_error = None

            print(Colors.success(f"{self.config.name} started (PID: {self.process.pid})"))
            print(Colors.info(f"Logs:"))
            for log_type in ["main", "network", "debug", "reload"]:
                print(f"   └─ {log_type.capitalize():<8}: logs/{self.config.name}_{log_type}.log")

            # Tunggu 5 detik untuk proses benar-benar start
            time.sleep(5)
            return True

        except FileNotFoundError:
            self.last_error = "File bot.py tidak ditemukan"
            print(Colors.error(self.last_error))
            return False
        except Exception as e:
            self.last_error = str(e)
            print(Colors.error(f"Gagal start {self.config.name}: {self.last_error}"))
            return False

    def stop(self, timeout: int = 10) -> bool:
        """Stop proses bot dengan graceful shutdown"""
        if not self.is_running or not self.process:
            return False

        try:
            print(Colors.warning(f"Stopping {self.config.name} (PID: {self.process.pid})..."))

            # Coba terminate dulu
            try:
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                else:
                    self.process.terminate()
                self.process.wait(timeout=timeout)
                print(Colors.success(f"{self.config.name} gracefully stopped"))
            except subprocess.TimeoutExpired:
                print(Colors.warning(f"Force killing {self.config.name}..."))
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                else:
                    self.process.kill()
                self.process.wait()
                print(Colors.success(f"{self.config.name} force stopped"))

            self.is_running = False
            self.process = None
            return True

        except Exception as e:
            self.last_error = str(e)
            print(Colors.error(f"Error stopping {self.config.name}: {self.last_error}"))
            return False

    def restart(self) -> bool:
        """Restart bot"""
        if self.stop():
            time.sleep(2)
            return self.start()
        return False

    # ── Health & Status ──────────────────────────────────────────────────────

    def check_health(self) -> Tuple[bool, str]:
        """Check kesehatan bot"""
        if not self.is_running or not self.process:
            self.health_status = "stopped"
            self.health_detail = "Not running"
            return False, self.health_detail

        if self.process.poll() is not None:
            self.health_status = "dead"
            self.health_detail = f"Exit code: {self.process.returncode}"
            self.is_running = False
            return False, self.health_detail

        # Check HTTP health endpoint
        is_healthy, msg, data = check_health_endpoint(self.config.flask_port)
        self.last_health_check = datetime.now()
        self.health_status = "up" if is_healthy else "down"
        self.health_detail = msg

        return is_healthy, msg

    def get_status(self) -> str:
        """Dapatkan status bot"""
        if not self.is_running or not self.process:
            return "Stopped"
        if self.process.poll() is not None:
            return f"Dead (exit: {self.process.returncode})"
        return "Running"

    def get_uptime(self) -> str:
        return format_uptime(self.start_time)

    def get_info(self) -> Dict:
        """Dapatkan info lengkap bot"""
        return {
            "name": self.config.name,
            "bot_num": self.config.bot_num,
            "port": self.config.flask_port,
            "status": self.get_status(),
            "uptime": self.get_uptime(),
            "restarts": self.restart_count,
            "health": self.health_status,
            "health_detail": self.health_detail,
            "pid": self.process.pid if self.process else "N/A",
            "last_health_check": format_timestamp(self.last_health_check),
            "token_preview": self.config.token_preview(),
            "target_chat": self.config.target_chat,
            "admin_chat": self.config.admin_chat or "N/A",
            "superadmins": self.config.superadmins or "None",
            "moderators": self.config.moderators or "None"
        }

    # ── Log Utilities ────────────────────────────────────────────────────────

    def get_log_info(self) -> Dict[str, str]:
        """Dapatkan info ukuran log"""
        from utils import get_log_size
        return {
            log_type: get_log_size(self.config.name, log_type)
            for log_type in ["main", "network", "debug", "reload"]
        }

    def tail_log(self, log_type: str = "main", lines: int = 20) -> List[str]:
        """Baca N baris terakhir log"""
        from utils import tail_log
        return tail_log(self.config.name, log_type, lines)

    def clear_log(self, log_type: str = "main") -> bool:
        """Clear log file"""
        from utils import clear_log
        return clear_log(self.config.name, log_type)


class BotManager:
    """Manager untuk semua bot"""

    def __init__(self):
        self.bots: Dict[int, BotProcess] = {}
        self.config_manager = None

    def load_config(self, config_manager):
        self.config_manager = config_manager
        self._load_bots()

    def _load_bots(self):
        """Load semua bot dari config"""
        if not self.config_manager:
            return

        for config in self.config_manager.get_all_configs():
            bot = BotProcess(config)
            self.bots[config.bot_num] = bot

    def get_bot(self, bot_num: int) -> Optional[BotProcess]:
        return self.bots.get(bot_num)

    def get_all_bots(self) -> List[BotProcess]:
        return list(self.bots.values())

    def get_running_bots(self) -> List[BotProcess]:
        return [bot for bot in self.bots.values() if bot.is_running]

    def get_stopped_bots(self) -> List[BotProcess]:
        return [bot for bot in self.bots.values() if not bot.is_running]

    def start_all(self) -> int:
        """Start semua bot"""
        started = 0
        for bot in self.bots.values():
            if bot.start():
                started += 1
                time.sleep(1)  # Delay antar bot
        return started

    def stop_all(self) -> int:
        """Stop semua bot"""
        stopped = 0
        for bot in self.bots.values():
            if bot.stop():
                stopped += 1
        return stopped

    def restart_all(self) -> int:
        """Restart semua bot"""
        restarted = 0
        for bot in self.bots.values():
            if bot.restart():
                restarted += 1
        return restarted

    def check_all_health(self) -> List[Tuple[int, bool, str]]:
        """Check kesehatan semua bot"""
        results = []
        for bot_num, bot in self.bots.items():
            is_healthy, msg = bot.check_health()
            results.append((bot_num, is_healthy, msg))
        return results

    def get_dashboard(self) -> List[Dict]:
        """Dapatkan dashboard info semua bot"""
        return [bot.get_info() for bot in self.bots.values()]

    def get_global_status(self) -> Dict:
        """Dapatkan status global"""
        total = len(self.bots)
        running = len(self.get_running_bots())
        stopped = len(self.get_stopped_bots())
        healthy = sum(1 for bot in self.bots.values() if bot.health_status == "up")
        return {
            "total": total,
            "running": running,
            "stopped": stopped,
            "healthy": healthy,
            "uptime": format_uptime(min((b.start_time for b in self.bots.values() if b.start_time), default=None))
        }
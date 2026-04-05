#!/usr/bin/env python3
"""
HANAYA Bot - Health Monitor
"""

import time
import threading
from datetime import datetime
from typing import List, Dict

from utils import Colors
from loader import BotManager


class HealthMonitor:
    """Monitor kesehatan bot secara berkala"""

    def __init__(self, bot_manager: BotManager):
        self.bot_manager = bot_manager
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.last_check = None
        self.check_count = 0

    def start(self):
        """Start monitoring"""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        print(Colors.info("Health monitoring started"))

    def stop(self):
        """Stop monitoring"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _monitor_loop(self):
        """Loop monitoring"""
        # Startup delay
        print(Colors.warning("Waiting 30s for bots to fully startup..."))
        time.sleep(30)

        while self.running:
            self.check_count += 1
            self.last_check = datetime.now()

            # Check health semua bot
            for bot in self.bot_manager.get_all_bots():
                is_healthy, msg = bot.check_health()
                if is_healthy:
                    if bot.health_status == "up":
                        continue  # Tidak perlu print jika tetap sehat
                    print(Colors.success(f"Bot {bot.config.bot_num}: {msg}"))
                else:
                    print(Colors.warning(f"Bot {bot.config.bot_num}: {msg}"))

            # Print summary setiap 4x check (2 menit)
            if self.check_count % 4 == 0:
                status = self.bot_manager.get_global_status()
                print(Colors.status(f"Health check #{self.check_count} - {status['healthy']}/{status['total']} bots healthy"))

            time.sleep(30)  # Check setiap 30 detik
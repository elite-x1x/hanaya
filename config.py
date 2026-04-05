#!/usr/bin/env python3
"""
HANAYA Bot - Configuration Manager
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class BotConfig:
    """Konfigurasi untuk setiap bot instance"""
    bot_num: int
    name: str = ""
    token: str = ""
    target_chat: str = ""
    admin_chat: str = ""
    flask_port: int = 5000
    superadmins: str = ""
    moderators: str = ""
    allowed_chats: str = ""

    def __post_init__(self):
        self.name = os.getenv(f"BOT_NAME_{self.bot_num}", f"bot_{self.bot_num}")
        self.token = os.getenv(f"BOT_TOKEN_{self.bot_num}", "")
        self.target_chat = os.getenv(f"TARGET_CHAT_ID_{self.bot_num}", "")
        self.admin_chat = os.getenv(f"ADMIN_CHAT_ID_{self.bot_num}", "")
        self.flask_port = int(os.getenv(f"FLASK_PORT_{self.bot_num}", 5000 + self.bot_num))
        self.superadmins = os.getenv(f"SUPERADMINS_{self.bot_num}", "")
        self.moderators = os.getenv(f"MODERATORS_{self.bot_num}", "")
        self.allowed_chats = os.getenv(f"ALLOWED_SOURCE_CHATS_{self.bot_num}", "")

    def is_valid(self) -> bool:
        return bool(self.name and self.token and self.target_chat)

    def get_error_message(self) -> str:
        if not self.name:
            return f"BOT_NAME_{self.bot_num} tidak ditemukan"
        if not self.token:
            return f"BOT_TOKEN_{self.bot_num} tidak ditemukan"
        if not self.target_chat:
            return f"TARGET_CHAT_ID_{self.bot_num} tidak ditemukan"
        return "Konfigurasi tidak valid"

    def token_preview(self) -> str:
        return f"{self.token[:20]}..." if self.token else "N/A"


@dataclass
class GlobalConfig:
    """Konfigurasi global dari environment"""
    allowed_media_types: str = field(default_factory=lambda: os.getenv("ALLOWED_MEDIA_TYPES", "video,photo,document"))
    daily_limit: str = field(default_factory=lambda: os.getenv("DAILY_LIMIT", "3500"))
    delay_between_send: str = field(default_factory=lambda: os.getenv("DELAY_BETWEEN_SEND", "1.5"))
    group_size: str = field(default_factory=lambda: os.getenv("GROUP_SIZE", "5"))
    max_queue_size: str = field(default_factory=lambda: os.getenv("MAX_QUEUE_SIZE", "2000"))
    batch_pause_every: str = field(default_factory=lambda: os.getenv("BATCH_PAUSE_EVERY", "500"))
    batch_pause_min: str = field(default_factory=lambda: os.getenv("BATCH_PAUSE_MIN", "30"))
    batch_pause_max: str = field(default_factory=lambda: os.getenv("BATCH_PAUSE_MAX", "120"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


class ConfigManager:
    """Manager untuk semua konfigurasi"""

    def __init__(self, env_file: str = ".env"):
        self.env_file = env_file
        self.global_config = GlobalConfig()
        self._loaded = False

    def load_env(self) -> bool:
        """Load environment variables dari file .env"""
        try:
            from dotenv import load_dotenv
            result = load_dotenv(self.env_file)
            self._loaded = True
            # Refresh global config setelah load
            self.global_config = GlobalConfig()
            return result
        except ImportError:
            return False

    def discover_bots(self, max_bots: int = 100) -> List[int]:
        """Temukan semua bot yang dikonfigurasi di environment"""
        available = []
        for i in range(1, max_bots + 1):
            config = BotConfig(i)
            if config.is_valid():
                available.append(i)
            elif i > 1 and not os.getenv(f"BOT_TOKEN_{i}"):
                break
        return available

    def get_bot_config(self, bot_num: int) -> BotConfig:
        return BotConfig(bot_num)

    def get_all_configs(self) -> List[BotConfig]:
        """Dapatkan semua konfigurasi bot yang valid"""
        return [BotConfig(n) for n in self.discover_bots()]

    def check_bot_file(self) -> bool:
        return Path("bot.py").exists()
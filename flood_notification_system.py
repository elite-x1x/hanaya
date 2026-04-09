# ============================================================
# === FLOOD NOTIFICATION SYSTEM (COMPREHENSIVE) ===
# ============================================================
"""
Sistem notifikasi flood yang komprehensif untuk HANAYA Bot v5.9
Fitur:
- Event logging dengan history
- Admin notifications dengan cooldown
- Recovery tracking
- Real-time dashboard integration
- Flood statistics
"""

import json
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional
from collections import deque


# ============================================================
# === FLOOD EVENT LOGGER ===
# ============================================================
class FloodEventLogger:
    """
    Logger untuk track semua flood events dengan history
    
    Features:
    - Persistent storage ke JSON file
    - In-memory cache untuk fast access
    - Automatic cleanup old events
    - Statistics calculation
    """
    
    def __init__(self, log_file: Path, max_events: int = 100):
        """
        Initialize flood event logger
        
        Args:
            log_file: Path ke file JSON untuk menyimpan events
            max_events: Maximum events untuk di-keep di memory
        """
        self.log_file = log_file
        self.max_events = max_events
        self._events: deque = deque(maxlen=max_events)
        self._lock = asyncio.Lock()
        self._last_save = time.time()
        self._save_interval = 10.0  # Save setiap 10 detik
        self._dirty = False
    
    async def log_flood_event(
        self,
        flood_count: int,
        penalty: float,
        suggested_wait: int,
        total_wait: float,
        error_msg: str = "",
        media_count: int = 0
    ) -> None:
        """
        Log flood event dengan semua detail
        
        Args:
            flood_count: Flood event number
            penalty: Penalty dalam detik
            suggested_wait: Suggested wait dari Telegram
            total_wait: Total wait time (suggested + penalty + random)
            error_msg: Error message dari Telegram API
            media_count: Jumlah media yang gagal terkirim
        """
        async with self._lock:
            try:
                event = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "flood_count": flood_count,
                    "penalty": round(penalty, 2),
                    "suggested_wait": suggested_wait,
                    "total_wait": round(total_wait, 2),
                    "error_msg": error_msg[:150] if error_msg else "",
                    "media_count": media_count,
                    "severity": self._get_severity(penalty),
                }
                
                self._events.append(event)
                self._dirty = True
                
                logging.debug(
                    f"📝 [FLOOD] Event logged: #{flood_count} | "
                    f"Penalty: {penalty:.0f}s | "
                    f"Severity: {event['severity']}"
                )
                
                # Auto-save jika interval tercapai
                now = time.time()
                if now - self._last_save >= self._save_interval:
                    await self._save_to_file()
            
            except Exception as e:
                logging.error(f"❌ [FLOOD] Error logging event: {e}")
    
    async def _save_to_file(self) -> None:
        """Save events ke file dengan atomic write"""
        if not self._dirty:
            return
        
        try:
            # Convert deque ke list
            events_list = list(self._events)
            
            # Atomic write: temp file dulu
            temp_file = self.log_file.with_suffix(".json.tmp")
            
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(events_list, f, indent=2)
                f.flush()
                import os
                os.fsync(f.fileno())
            
            # Atomic rename
            if self.log_file.exists():
                backup = self.log_file.with_suffix(".json.backup")
                try:
                    if backup.exists():
                        backup.unlink()
                    self.log_file.replace(backup)
                except Exception:
                    pass
            
            temp_file.replace(self.log_file)
            
            self._last_save = time.time()
            self._dirty = False
            
            logging.debug(
                f"💾 [FLOOD] Saved {len(events_list)} events to file"
            )
        
        except Exception as e:
            logging.error(f"❌ [FLOOD] Error saving to file: {e}")
    
    async def load_from_file(self) -> int:
        """
        Load events dari file
        
        Returns:
            Jumlah events yang di-load
        """
        try:
            if not self.log_file.exists():
                logging.info(f"📂 [FLOOD] Log file tidak ada, start fresh")
                return 0
            
            with open(self.log_file, "r", encoding="utf-8") as f:
                events_list = json.load(f)
            
            if not isinstance(events_list, list):
                logging.warning(f"⚠️ [FLOOD] Log file format invalid")
                return 0
            
            # Load ke deque (otomatis trim ke max_events)
            self._events.clear()
            for event in events_list:
                if isinstance(event, dict):
                    self._events.append(event)
            
            logging.info(
                f"📥 [FLOOD] Loaded {len(self._events)} events from file"
            )
            return len(self._events)
        
        except json.JSONDecodeError as e:
            logging.error(f"❌ [FLOOD] JSON decode error: {e}")
            
            # Try recover dari backup
            backup = self.log_file.with_suffix(".json.backup")
            if backup.exists():
                try:
                    with open(backup, "r", encoding="utf-8") as f:
                        events_list = json.load(f)
                    
                    self._events.clear()
                    for event in events_list:
                        if isinstance(event, dict):
                            self._events.append(event)
                    
                    logging.info(
                        f"✅ [FLOOD] Recovered {len(self._events)} events "
                        f"from backup"
                    )
                    return len(self._events)
                
                except Exception as e2:
                    logging.error(f"❌ [FLOOD] Backup recovery failed: {e2}")
            
            return 0
        
        except Exception as e:
            logging.error(f"❌ [FLOOD] Error loading from file: {e}")
            return 0
    
    async def get_recent_events(self, limit: int = 10) -> List[Dict]:
        """Get recent flood events"""
        async with self._lock:
            events_list = list(self._events)
            return events_list[-limit:] if events_list else []
    
    async def get_flood_stats(self) -> Dict:
        """
        Calculate flood statistics
        
        Returns:
            Dict dengan statistics
        """
        async with self._lock:
            if not self._events:
                return {
                    "total_events": 0,
                    "avg_penalty": 0.0,
                    "max_penalty": 0.0,
                    "min_penalty": 0.0,
                    "last_event": None,
                    "events_last_hour": 0,
                    "severity_distribution": {
                        "critical": 0,
                        "high": 0,
                        "medium": 0,
                    }
                }
            
            events_list = list(self._events)
            penalties = [e["penalty"] for e in events_list]
            
            # Count severity distribution
            severity_dist = {
                "critical": 0,
                "high": 0,
                "medium": 0,
            }
            for event in events_list:
                severity = event.get("severity", "medium")
                if severity in severity_dist:
                    severity_dist[severity] += 1
            
            return {
                "total_events": len(events_list),
                "avg_penalty": round(sum(penalties) / len(penalties), 2),
                "max_penalty": round(max(penalties), 2),
                "min_penalty": round(min(penalties), 2),
                "last_event": events_list[-1] ["timestamp"] if events_list else None,
                "events_last_hour": len([
                    e for e in events_list
                    if self._is_within_hours(e["timestamp"], hours=1)
                ]),
                "events_last_24h": len([
                    e for e in events_list
                    if self._is_within_hours(e["timestamp"], hours=24)
                ]),
                "severity_distribution": severity_dist,
            }
    
    @staticmethod
    def _get_severity(penalty: float) -> str:
        """Determine severity level berdasarkan penalty"""
        if penalty > 200:
            return "critical"
        elif penalty > 100:
            return "high"
        else:
            return "medium"
    
    @staticmethod
    def _is_within_hours(timestamp_str: str, hours: int = 1) -> bool:
        """Check apakah event dalam N jam terakhir"""
        try:
            event_time = datetime.fromisoformat(timestamp_str)
            now = datetime.now(timezone.utc)
            diff = (now - event_time).total_seconds()
            return diff < (hours * 3600)
        except Exception:
            return False


# ============================================================
# === ADMIN FLOOD NOTIFIER ===
# ============================================================
class AdminFloodNotifier:
    """
    Send notifications ke admin saat flood terjadi
    
    Features:
    - Per-severity cooldown untuk avoid spam
    - Detailed flood information
    - Recovery notifications
    - Thread-safe operations
    """
    
    def __init__(self, bot, admin_chat_id: int):
        """
        Initialize admin notifier
        
        Args:
            bot: Telegram bot instance
            admin_chat_id: Chat ID untuk admin notifications
        """
        self.bot = bot
        self.admin_chat_id = admin_chat_id
        self._lock = asyncio.Lock()
        self.last_notification: Dict[str, float] = {}
        
        # Cooldown periods per severity (dalam detik)
        self.notification_cooldown: Dict[str, float] = {
            "critical": 300.0,   # 5 menit untuk critical
            "high": 600.0,       # 10 menit untuk high
            "medium": 900.0,     # 15 menit untuk medium
        }
        
        self.last_recovery_notification = 0.0
        self.recovery_cooldown = 300.0  # 5 menit
    
    def _get_severity(self, penalty: float) -> str:
        """Determine severity level"""
        if penalty > 200:
            return "critical"
        elif penalty > 100:
            return "high"
        else:
            return "medium"
    
    def _get_severity_emoji(self, severity: str) -> str:
        """Get emoji untuk severity"""
        return {
            "critical": "🔴",
            "high": "🟠",
            "medium": "🟡",
        }.get(severity, "⚪")
    
    async def notify_flood(
        self,
        bot_name: str,
        flood_count: int,
        penalty: float,
        suggested_wait: int,
        total_wait: float,
        error_msg: str = "",
        media_count: int = 0
    ) -> bool:
        """
        Send flood notification ke admin
        
        Args:
            bot_name: Nama bot
            flood_count: Flood event number
            penalty: Penalty dalam detik
            suggested_wait: Suggested wait dari Telegram
            total_wait: Total wait time
            error_msg: Error message
            media_count: Jumlah media yang gagal
        
        Returns:
            True jika notification terkirim, False jika cooldown
        """
        async with self._lock:
            severity = self._get_severity(penalty)
            now = time.time()
            
            # Check cooldown
            last_time = self.last_notification.get(severity, 0)
            cooldown = self.notification_cooldown.get(severity, 600)
            
            if now - last_time < cooldown:
                logging.debug(
                    f"⏸️ [FLOOD] Notification cooldown active "
                    f"({now - last_time:.0f}s/{cooldown:.0f}s)"
                )
                return False
            
            # Prepare message
            emoji = self._get_severity_emoji(severity)
            severity_upper = severity.upper()
            
            message = (
                f"{emoji} <b>FLOOD ALERT</b> — {bot_name}\n\n"
                f"<b>📊 Event Details:</b>\n"
                f"├─ <code>Flood Count    : #{flood_count}</code>\n"
                f"├─ <code>Current Penalty: {penalty:.0f}s</code>\n"
                f"├─ <code>Suggested Wait : {suggested_wait}s</code>\n"
                f"├─ <code>Total Wait     : {total_wait:.0f}s</code>\n"
                f"├─ <code>Severity       : {severity_upper}</code>\n"
                f"├─ <code>Failed Media   : {media_count}</code>\n"
                f"└─ <code>Time           : {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}</code>\n\n"
                f"<b>⚙️ Action:</b>\n"
                f"• Bot akan pause pengiriman selama <code>{total_wait:.0f}s</code>\n"
                f"• Queue tetap tersimpan\n"
                f"• Status akan update di dashboard\n\n"
                f"<b>🔗 Commands:</b>\n"
                f"• /status - Lihat status bot\n"
                f"• /currentconfig - Lihat config detail"
            )
            
            if error_msg:
                # Truncate error message
                error_display = error_msg[:80]
                if len(error_msg) > 80:
                    error_display += "..."
                message += f"\n\n<b>❌ Error:</b>\n<code>{error_display}</code>"
            
            try:
                await self.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=message,
                    parse_mode="HTML"
                )
                
                self.last_notification[severity] = now
                
                logging.info(
                    f"📨 [FLOOD] Notification sent to admin "
                    f"(severity: {severity}, count: #{flood_count})"
                )
                return True
            
            except Exception as e:
                logging.error(
                    f"❌ [FLOOD] Failed to send notification: {e}"
                )
                return False
    
    async def notify_recovery(
        self,
        bot_name: str,
        recovery_time: float,
        total_events: int = 0
    ) -> bool:
        """
        Send recovery notification ke admin
        
        Args:
            bot_name: Nama bot
            recovery_time: Waktu recovery dalam detik
            total_events: Total flood events dalam session
        
        Returns:
            True jika notification terkirim
        """
        async with self._lock:
            now = time.time()
            
            # Check cooldown untuk recovery notifications
            if now - self.last_recovery_notification < self.recovery_cooldown:
                logging.debug(
                    f"⏸️ [FLOOD] Recovery notification cooldown active"
                )
                return False
            
            # Format recovery time
            minutes = int(recovery_time // 60)
            seconds = int(recovery_time % 60)
            recovery_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
            
            message = (
                f"✅ <b>FLOOD RECOVERED</b> — {bot_name}\n\n"
                f"<b>📊 Recovery Details:</b>\n"
                f"├─ <code>Recovery Time : {recovery_str}</code>\n"
                f"├─ <code>Total Events  : {total_events}</code>\n"
                f"├─ <code>Status        : Normal</code>\n"
                f"└─ <code>Time          : {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}</code>\n\n"
                f"<b>✨ Bot siap mengirim media lagi.</b>"
            )
            
            try:
                await self.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=message,
                    parse_mode="HTML"
                )
                
                self.last_recovery_notification = now
                
                logging.info(
                    f"📨 [FLOOD] Recovery notification sent to admin "
                    f"(recovery time: {recovery_str})"
                )
                return True
            
            except Exception as e:
                logging.error(
                    f"❌ [FLOOD] Failed to send recovery notification: {e}"
                )
                return False


# ============================================================
# === ENHANCED SMART FLOOD CONTROLLER WITH NOTIFICATIONS ===
# ============================================================
class SmartFloodControllerWithNotifications:
    """
    Enhanced flood controller dengan notifications dan recovery tracking
    
    Features:
    - Exponential backoff penalty
    - Recovery progress tracking
    - Admin notifications
    - Event logging
    - Adaptive group delays
    """
    
    def __init__(
        self,
        notifier: Optional[AdminFloodNotifier] = None,
        event_logger: Optional[FloodEventLogger] = None
    ):
        """
        Initialize flood controller
        
        Args:
            notifier: AdminFloodNotifier instance
            event_logger: FloodEventLogger instance
        """
        self.flood_count: int = 0
        self.total_flood: int = 0
        self.last_flood_time: Optional[datetime] = None
        self.penalty: float = 0.0
        self.is_cooling: bool = False
        self.group_delay_min: float = 10.0  # Default values
        self.group_delay_max: float = 40.0
        
        self.notifier = notifier
        self.event_logger = event_logger
        
        # Recovery tracking
        self.recovery_start_time: Optional[datetime] = None
        self.recovery_start_penalty: float = 0.0
        
        # Configuration constants
        self.FLOOD_RESET_AFTER = 600  # 10 menit
        self.FLOOD_RANDOM_MIN = 10
        self.FLOOD_RANDOM_MAX = 30
        self.FLOOD_PENALTY_BASE = 15.0
        self.FLOOD_MAX_PENALTY = 300.0
        
        self._lock = asyncio.Lock()
    
    async def record_flood(
        self,
        suggested_wait: int,
        error_msg: str = "",
        media_count: int = 0,
        delay_min: float = 10.0,
        delay_max: float = 40.0
    ) -> float:
        """
        Record flood event dengan notification
        
        Args:
            suggested_wait: Suggested wait dari Telegram API
            error_msg: Error message dari API
            media_count: Jumlah media yang gagal
            delay_min: Minimum group delay
            delay_max: Maximum group delay
        
        Returns:
            Total wait time dalam detik
        """
        async with self._lock:
            now = datetime.now(timezone.utc)
            
            # Check auto-reset
            if self.last_flood_time is not None:
                elapsed = (now - self.last_flood_time).total_seconds()
                if elapsed > self.FLOOD_RESET_AFTER:
                    logging.info(
                        f"🔄 [FLOOD] Auto-reset (no flood for {elapsed:.0f}s)"
                    )
                    self.flood_count = 0
                    self.penalty = 0.0
                    self.recovery_start_time = None
            
            # Increment counters
            self.flood_count += 1
            self.total_flood += 1
            self.last_flood_time = now
            self.is_cooling = True
            
            # Calculate exponential penalty
            # Formula: base * (2 ^ (count - 1))
            exponential_penalty = (
                self.FLOOD_PENALTY_BASE * (2 ** (self.flood_count - 1))
            )
            self.penalty = min(exponential_penalty, self.FLOOD_MAX_PENALTY)
            
            # Add random factor
            import random
            random_add = random.uniform(
                self.FLOOD_RANDOM_MIN,
                self.FLOOD_RANDOM_MAX
            )
            total_wait = float(suggested_wait) + random_add + self.penalty
            
            # Update group delays adaptively
            self._update_group_delays(delay_min, delay_max)
            
            # Log event
            if self.event_logger:
                await self.event_logger.log_flood_event(
                    self.flood_count,
                    self.penalty,
                    suggested_wait,
                    total_wait,
                    error_msg,
                    media_count
                )
            
            # Send notification
            if self.notifier:
                try:
                    await self.notifier.notify_flood(
                        bot_name="Bot",  # Will be passed properly
                        flood_count=self.flood_count,
                        penalty=self.penalty,
                        suggested_wait=suggested_wait,
                        total_wait=total_wait,
                        error_msg=error_msg,
                        media_count=media_count
                    )
                except Exception as e:
                    logging.error(
                        f"❌ [FLOOD] Error sending notification: {e}"
                    )
            
            # Log warning
            logging.warning(
                f"🚨 [FLOOD] Event #{self.flood_count} detected!\n"
                f"   ├─ Telegram suggestion : {suggested_wait}s\n"
                f"   ├─ Random factor       : {random_add:.1f}s\n"
                f"   ├─ Exponential penalty : {exponential_penalty:.0f}s "
                f"(capped: {self.penalty:.0f}s)\n"
                f"   ├─ Total wait time     : {total_wait:.1f}s\n"
                f"   ├─ Failed media        : {media_count}\n"
                f"   └─ Group delay         : {self.group_delay_min:.0f}s - "
                f"{self.group_delay_max:.0f}s"
            )
            
            return total_wait
    
    def _update_group_delays(self, base_min: float, base_max: float) -> None:
        """
        Update group delays adaptively berdasarkan flood count
        
        Args:
            base_min: Base minimum delay
            base_max: Base maximum delay
        """
        # Adaptive increase: base + (flood_count * multiplier)
        self.group_delay_min = min(
            90.0,  # Hard cap
            max(
                base_min,
                base_min + (self.flood_count * 10)
            )
        )
        
        self.group_delay_max = min(
            150.0,  # Hard cap
            max(
                base_max,
                base_max + (self.flood_count * 20)
            )
        )
        
        # Ensure min < max
        if self.group_delay_min >= self.group_delay_max:
            self.group_delay_min = self.group_delay_max * 0.7
    
    async def record_success(self) -> None:
        """
        Record successful send dengan gradual recovery
        """
        async with self._lock:
            now = datetime.now(timezone.utc)
            
            # Track recovery start
            if self.recovery_start_time is None and self.penalty > 0:
                self.recovery_start_time = now
                self.recovery_start_penalty = self.penalty
            
            # Proportional penalty decay
            if self.penalty > 0:
                decay_percentage = 0.08 if self.penalty > 100 else 0.10
                decay_amount = self.penalty * decay_percentage
                self.penalty = max(0.0, self.penalty - decay_amount)
                
                logging.debug(
                    f"✅ [FLOOD] Success | "
                    f"Penalty decay: {decay_amount:.1f}s → {self.penalty:.1f}s"
                )
            
            # Gradual flood count reduction
            if self.flood_count > 0:
                if self.penalty > 100:
                    pass  # Slow recovery
                elif self.penalty > 50:
                    self.flood_count = max(0, self.flood_count - 1)
                else:
                    self.flood_count = max(0, self.flood_count - 1)
                
                # Check if fully recovered
                if self.flood_count == 0:
                    self.last_flood_time = None
                    
                    # Send recovery notification
                    if self.recovery_start_time and self.notifier:
                        recovery_time = (
                            now - self.recovery_start_time
                        ).total_seconds()
                        try:
                            await self.notifier.notify_recovery(
                                bot_name="Bot",
                                recovery_time=recovery_time,
                                total_events=self.total_flood
                            )
                        except Exception as e:
                            logging.error(
                                f"❌ [FLOOD] Error sending recovery "
                                f"notification: {e}"
                            )
                    
                    self.penalty = 0.0
                    self.is_cooling = False
                    self.recovery_start_time = None
                    logging.info(f"✅ [FLOOD] Fully recovered")
    
    def get_group_delay(self) -> float:
        """
        Get random group delay dengan jitter
        
        Returns:
            Delay dalam detik
        """
        import random
        base_delay = random.uniform(
            self.group_delay_min,
            self.group_delay_max
        )
        
        # Add small jitter
        jitter = random.uniform(-2.0, 2.0)
        return max(1.0, base_delay + jitter)
    
    def get_recovery_progress(self) -> Dict:
        """
        Get recovery progress information
        
        Returns:
            Dict dengan recovery progress
        """
        if self.recovery_start_time is None:
            return {
                "in_recovery": False,
                "progress_pct": 100.0,
                "elapsed_time": 0.0,
                "estimated_remaining": 0.0,
                "current_penalty": 0.0,
            }
        
        now = datetime.now(timezone.utc)
        elapsed = (now - self.recovery_start_time).total_seconds()
        
        # Calculate progress percentage
        if self.recovery_start_penalty > 0:
            progress_pct = (
                (self.recovery_start_penalty - self.penalty) /
                self.recovery_start_penalty * 100
            )
        else:
            progress_pct = 100.0
        
        return {
            "in_recovery": True,
            "progress_pct": min(progress_pct, 100.0),
            "elapsed_time": elapsed,
            "estimated_remaining": max(0, self.penalty),
            "current_penalty": self.penalty,
        }
    
    def get_status(self) -> str:
        """
        Get status string dengan recovery info
        
        Returns:
            Status string
        """
        penalty_level = (
            "🔴 Critical" if self.penalty > 200 else (
                "🟠 High" if self.penalty > 100 else (
                    "🟡 Medium" if self.penalty > 50 else "🟢 Normal"
                )
            )
        )
        
        recovery = self.get_recovery_progress()
        recovery_str = (
            f" | Recovery: {recovery['progress_pct']:.0f}%"
            if recovery["in_recovery"] else ""
        )
        
        return (
            f"Flood | Count: {self.flood_count} | "
            f"Total: {self.total_flood} | "
            f"Penalty: {self.penalty:.0f}s ({penalty_level}){recovery_str}"
        )
    
    def to_dict(self) -> Dict:
        """Convert state ke dictionary untuk persistence"""
        return {
            "flood_count": self.flood_count,
            "total_flood": self.total_flood,
            "last_flood_time": (
                self.last_flood_time.isoformat()
                if self.last_flood_time else None
            ),
            "penalty": self.penalty,
            "is_cooling": self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
            "recovery_start_time": (
                self.recovery_start_time.isoformat()
                if self.recovery_start_time else None
            ),
            "recovery_start_penalty": self.recovery_start_penalty,
        }
    
    @classmethod
    def from_dict(
        cls,
        data: Dict,
        notifier: Optional[AdminFloodNotifier] = None,
        event_logger: Optional[FloodEventLogger] = None
    ) -> "SmartFloodControllerWithNotifications":
        """Create instance dari dictionary"""
        obj = cls(notifier, event_logger)
        obj.flood_count = int(data.get("flood_count", 0))
        obj.total_flood = int(data.get("total_flood", 0))
        obj.last_flood_time = cls._parse_datetime(
            data.get("last_flood_time")
        )
        obj.penalty = float(data.get("penalty", 0.0))
        obj.is_cooling = bool(data.get("is_cooling", False))
        obj.group_delay_min = float(
            data.get("group_delay_min", 10.0)
        )
        obj.group_delay_max = float(
            data.get("group_delay_max", 40.0)
        )
        obj.recovery_start_time = cls._parse_datetime(
            data.get("recovery_start_time")
        )
        obj.recovery_start_penalty = float(
            data.get("recovery_start_penalty", 0.0)
        )
        return obj
    
    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse ISO format datetime string"""
        if value is None:
            return None
        
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None
    
    async def save_state(self, state_manager) -> None:
        """Save state ke file"""
        try:
            await state_manager.save_flood(self.to_dict())
        except Exception as e:
            logging.error(f"❌ [FLOOD] Error saving state: {e}")

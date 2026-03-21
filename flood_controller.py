import random
import logging
import json
from datetime import datetime, timezone

from config import (
    FLOOD_RANDOM_MIN, FLOOD_RANDOM_MAX, FLOOD_PENALTY_STEP,
    FLOOD_MAX_PENALTY, FLOOD_RESET_AFTER,
    DELAY_BETWEEN_GROUP_MIN, DELAY_BETWEEN_GROUP_MAX
)
from redis_client import r_set_json
from config import KEY_FLOOD_CTRL


# ============================================================
# === SMART FLOOD CONTROLLER ===
# ============================================================
class SmartFloodController:

    def __init__(self):
        self.flood_count:     int             = 0
        self.total_flood:     int             = 0
        self.last_flood_time: datetime | None = None
        self.penalty:         float           = 0.0
        self.is_cooling:      bool            = False
        self.group_delay_min: float           = float(DELAY_BETWEEN_GROUP_MIN)
        self.group_delay_max: float           = float(DELAY_BETWEEN_GROUP_MAX)

    @staticmethod
    def _parse_last_flood_time(value) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError) as e:
                logging.warning(f"⚠️ Gagal parse last_flood_time '{value}': {e}")
                return None
        logging.warning(f"⚠️ Tipe last_flood_time tidak dikenal: {type(value)}")
        return None

    async def record_flood(self, suggested_wait: int) -> float:
        now = datetime.now(timezone.utc)

        if self.last_flood_time is not None:
            elapsed = (now - self.last_flood_time).total_seconds()
            if elapsed > FLOOD_RESET_AFTER:
                logging.info(
                    f"🔄 Flood counter direset "
                    f"(tidak ada flood selama {elapsed:.0f}s)"
                )
                self.flood_count = 0
                self.penalty     = 0.0

        self.flood_count     += 1
        self.total_flood     += 1
        self.last_flood_time  = now
        self.is_cooling       = True
        self.penalty          = min(
            float(FLOOD_MAX_PENALTY),
            float(self.flood_count * FLOOD_PENALTY_STEP)
        )

        random_add = random.uniform(FLOOD_RANDOM_MIN, FLOOD_RANDOM_MAX)
        total_wait = float(suggested_wait) + random_add + self.penalty

        self.group_delay_min = min(120.0, DELAY_BETWEEN_GROUP_MIN + (self.flood_count * 5))
        self.group_delay_max = min(180.0, DELAY_BETWEEN_GROUP_MAX + (self.flood_count * 10))

        logging.warning(
            f"🚨 FLOOD #{self.flood_count} terdeteksi!\n"
            f"   ├─ Saran Telegram   : {suggested_wait}s\n"
            f"   ├─ Random tambahan  : {random_add:.1f}s\n"
            f"   ├─ Penalti kumulatif: {self.penalty:.0f}s\n"
            f"   ├─ Total tunggu     : {total_wait:.1f}s\n"
            f"   └─ Delay group baru : {self.group_delay_min:.0f}s - {self.group_delay_max:.0f}s"
        )

        await self.save_state()
        return total_wait

    async def record_success(self) -> None:
        if self.penalty > 0:
            self.penalty = max(0.0, self.penalty - 5.0)
        if self.flood_count > 0:
            self.flood_count = max(0, self.flood_count - 1)
            if self.flood_count == 0:
                self.last_flood_time = None
        if self.group_delay_min > DELAY_BETWEEN_GROUP_MIN:
            self.group_delay_min = max(
                float(DELAY_BETWEEN_GROUP_MIN), self.group_delay_min - 2.0
            )
        if self.group_delay_max > DELAY_BETWEEN_GROUP_MAX:
            self.group_delay_max = max(
                float(DELAY_BETWEEN_GROUP_MAX), self.group_delay_max - 3.0
            )
        self.is_cooling = False
        await self.save_state()

    def get_group_delay(self) -> float:
        return random.uniform(self.group_delay_min, self.group_delay_max)

    def get_status(self) -> str:
        return (
            f"FloodCtrl | Count: {self.flood_count} | "
            f"Total: {self.total_flood} | "
            f"Penalty: {self.penalty:.0f}s | "
            f"Delay: {self.group_delay_min:.0f}-{self.group_delay_max:.0f}s"
        )

    def to_dict(self) -> dict:
        return {
            "flood_count"    : self.flood_count,
            "total_flood"    : self.total_flood,
            "last_flood_time": (
                self.last_flood_time.isoformat()
                if self.last_flood_time else None
            ),
            "penalty"        : self.penalty,
            "is_cooling"     : self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SmartFloodController":
        instance = cls()
        try:
            instance.flood_count     = int(data.get("flood_count", 0))
            instance.total_flood     = int(data.get("total_flood", 0))
            instance.last_flood_time = instance._parse_last_flood_time(
                data.get("last_flood_time")
            )
            instance.penalty         = float(data.get("penalty", 0.0))
            instance.is_cooling      = bool(data.get("is_cooling", False))
            instance.group_delay_min = float(data.get("group_delay_min", DELAY_BETWEEN_GROUP_MIN))
            instance.group_delay_max = float(data.get("group_delay_max", DELAY_BETWEEN_GROUP_MAX))
        except (TypeError, ValueError) as e:
            logging.warning(f"⚠️ Gagal load state flood control: {e}")
            instance = cls()
        return instance

    async def save_state(self) -> None:
        try:
            await r_set_json(KEY_FLOOD_CTRL, self.to_dict())
        except Exception as e:
            logging.error(f"❌ Gagal save flood ctrl state: {e}")

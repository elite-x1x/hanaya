import random
import logging
from datetime import datetime, timezone
from config import (
    FLOOD_PENALTY_STEP,
    FLOOD_MAX_PENALTY,
    FLOOD_RESET_AFTER,
    DELAY_BETWEEN_GROUP_MIN,
    DELAY_BETWEEN_GROUP_MAX,
    FLOOD_RANDOM_MIN,
    FLOOD_RANDOM_MAX,
)
from redis_utils import r_set_json

KEY_FLOOD_CTRL = "hanaya:flood_ctrl"

class SmartFloodController:
    def __init__(self):
        self.flood_count = 0
        self.total_flood = 0
        self.last_flood_time: datetime | None = None
        self.penalty = 0.0
        self.is_cooling = False
        self.group_delay_min = float(DELAY_BETWEEN_GROUP_MIN)
        self.group_delay_max = float(DELAY_BETWEEN_GROUP_MAX)

    async def record_flood(self, suggested_wait: int) -> float:
        now = datetime.now(timezone.utc)

        # Reset jika flood sudah lama tidak terjadi
        if self.last_flood_time is not None:
            elapsed = (now - self.last_flood_time).total_seconds()
            if elapsed > FLOOD_RESET_AFTER:
                logging.info(f"🔄 Flood counter direset (idle {elapsed:.0f}s)")
                self.flood_count = 0
                self.penalty = 0.0

        self.flood_count += 1
        self.total_flood += 1
        self.last_flood_time = now
        self.is_cooling = True
        self.penalty = min(FLOOD_MAX_PENALTY, self.flood_count * FLOOD_PENALTY_STEP)

        random_add = random.uniform(FLOOD_RANDOM_MIN, FLOOD_RANDOM_MAX)
        total_wait = suggested_wait + random_add + self.penalty

        # Delay group meningkat sesuai jumlah flood
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
            self.group_delay_min = max(DELAY_BETWEEN_GROUP_MIN, self.group_delay_min - 2.0)
        if self.group_delay_max > DELAY_BETWEEN_GROUP_MAX:
            self.group_delay_max = max(DELAY_BETWEEN_GROUP_MAX, self.group_delay_max - 3.0)
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
            "flood_count": self.flood_count,
            "total_flood": self.total_flood,
            "last_flood_time": self.last_flood_time.isoformat() if self.last_flood_time else None,
            "penalty": self.penalty,
            "is_cooling": self.is_cooling,
            "group_delay_min": self.group_delay_min,
            "group_delay_max": self.group_delay_max,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SmartFloodController":
        instance = cls()
        try:
            instance.flood_count = int(data.get("flood_count", 0))
            instance.total_flood = int(data.get("total_flood", 0))
            lf = data.get("last_flood_time")
            if lf:
                instance.last_flood_time = datetime.fromisoformat(lf)
            instance.penalty = float(data.get("penalty", 0.0))
            instance.is_cooling = bool(data.get("is_cooling", False))
            instance.group_delay_min = float(data.get("group_delay_min", DELAY_BETWEEN_GROUP_MIN))
            instance.group_delay_max = float(data.get("group_delay_max", DELAY_BETWEEN_GROUP_MAX))
        except Exception as e:
            logging.warning(f"⚠️ Gagal load state flood control: {e}")
        return instance

    async def save_state(self) -> None:
        try:
            await r_set_json(KEY_FLOOD_CTRL, self.to_dict())
        except Exception as e:
            logging.error(f"❌ Gagal save flood ctrl state: {e}")

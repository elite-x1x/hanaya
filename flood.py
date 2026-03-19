import logging, random
from datetime import datetime, timezone
from .config import FLOOD_PENALTY_STEP, FLOOD_MAX_PENALTY, FLOOD_RESET_AFTER

class SmartFloodController:
    def __init__(self):
        self.flood_count=0; self.total_flood=0; self.last_flood_time=None; self.penalty=0
    def record_flood(self,suggested):
        now=datetime.now(timezone.utc)
        if self.last_flood_time and (now-self.last_flood_time).total_seconds()>FLOOD_RESET_AFTER:
            self.flood_count=0; self.penalty=0
        self.flood_count+=1; self.total_flood+=1; self.last_flood_time=now
        self.penalty=min(FLOOD_MAX_PENALTY,self.flood_count*FLOOD_PENALTY_STEP)
        return suggested+random.uniform(10,30)+self.penalty
    def record_success(self): 
        self.penalty=max(0,self.penalty-5); self.flood_count=max(0,self.flood_count-1)
    def get_status(self): return f"FloodCtrl | Count:{self.flood_count} | Total:{self.total_flood} | Penalty:{self.penalty}s"

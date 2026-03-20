import logging
import json
from datetime import datetime, timezone
from hashlib import md5
from redis_utils import r_sismember, r_sadd_with_ttl

# Redis keys
KEY_SENT = "hanaya:sent"

# Local cache untuk sent items
local_sent = {}
MAX_LOCAL_SENT = 5000

DUPLICATES_DROPPED = 0

def make_hash(fingerprint: dict) -> str:
    """Buat hash fingerprint tanpa file_id & timestamp"""
    fp_copy = {k: v for k, v in fingerprint.items() if k not in ("file_id", "timestamp")}
    return md5(json.dumps(fp_copy, sort_keys=True).encode()).hexdigest()

def get_fingerprint(msg) -> dict | None:
    """Ambil fingerprint dari pesan video"""
    if not msg.video:
        return None
    v = msg.video
    return {
        'type': 'video',
        'duration': v.duration,
        'width': v.width,
        'height': v.height,
        'file_size': v.file_size,
        'file_id': v.file_id,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def _local_sent_add(key: str, max_size: int = MAX_LOCAL_SENT) -> None:
    """Tambah ke local_sent dengan eviction FIFO"""
    if key in local_sent:
        return
    local_sent[key] = True
    while len(local_sent) > max_size:
        # popitem(last=False) untuk FIFO
        local_sent.pop(next(iter(local_sent)))

async def is_duplicate(fingerprint: dict, pending_snapshot: list | None = None) -> bool:
    """Cek duplikat dengan 3 lapis: Redis, local_sent, pending list"""
    global DUPLICATES_DROPPED
    if not fingerprint:
        return False

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)

    # Lapis 1: Redis
    try:
        if await r_sismember(KEY_SENT, file_id) or await r_sismember(KEY_SENT, fp_hash):
            logging.debug(f"♻️ Duplikat (Redis): {file_id[:8]}...")
            DUPLICATES_DROPPED += 1
            return True
    except Exception as e:
        logging.warning(f"⚠️ Redis check error: {e}")

    # Lapis 2: local_sent
    if file_id in local_sent or fp_hash in local_sent:
        logging.debug(f"♻️ Duplikat (local_sent): {file_id[:8]}...")
        DUPLICATES_DROPPED += 1
        return True

    # Lapis 3: pending snapshot
    snapshot = pending_snapshot if pending_snapshot is not None else []
    for item in snapshot:
        if isinstance(item, (list, tuple)) and len(item) == 3:
            q_file_id, _, q_fp = item
            if q_file_id == file_id or make_hash(q_fp) == fp_hash:
                logging.debug(f"♻️ Duplikat (pending): {file_id[:8]}...")
                DUPLICATES_DROPPED += 1
                return True

    return False

async def mark_sent(fingerprint: dict) -> None:
    """Tandai video sebagai sudah dikirim (Redis + local fallback)"""
    if not fingerprint:
        return

    file_id = fingerprint.get('file_id', '')
    fp_hash = make_hash(fingerprint)

    try:
        await r_sadd_with_ttl(KEY_SENT, file_id, 60*60*24*30)  # TTL 30 hari
        await r_sadd_with_ttl(KEY_SENT, fp_hash, 60*60*24*30)
        logging.debug(f"✅ Marked sent (Redis): {file_id[:8]}...")
    except Exception as e:
        logging.warning(f"⚠️ Redis mark_sent error, fallback ke local: {e}")
        _local_sent_add(file_id)
        _local_sent_add(fp_hash)

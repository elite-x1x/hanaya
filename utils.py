import hashlib, json
from .config import pending_media, local_sent
from .redis_helpers import r_sismember, r_sadd_with_ttl, redis_client, KEY_SENT

def make_hash(fp): 
    return hashlib.md5(json.dumps({k:v for k,v in fp.items() if k!="file_id"}, sort_keys=True).encode()).hexdigest()

def is_duplicate(fp):
    if not fp: return False
    fid, h = fp.get("file_id",""), make_hash(fp)
    if redis_client and (r_sismember(KEY_SENT,fid) or r_sismember(KEY_SENT,h)): return True
    if fid in local_sent or h in local_sent: return True
    return any(qfid==fid or make_hash(qfp)==h for qfid,_,qfp in pending_media)

def mark_sent(fp):
    fid,h = fp.get("file_id",""), make_hash(fp)
    try:
        r_sadd_with_ttl(KEY_SENT,fid,60*60*24*30)
        r_sadd_with_ttl(KEY_SENT,h,60*60*24*30)
    except: local_sent.update({fid,h})

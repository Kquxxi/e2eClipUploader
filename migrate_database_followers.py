import sys
import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv
import time

sys.stdout.reconfigure(encoding='utf-8')
load_dotenv()  # prefer .env first
load_dotenv('config.env')  # fallback

# --- safe atomic write helper ---
def _safe_write_json(path: str, obj):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
# --- end helper ---

CLIENT_ID     = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

# --- CONFIG ---
REQUEST_TIMEOUT = int(os.getenv('TWITCH_REQUEST_TIMEOUT', '20'))
MIN_FOLLOWERS   = int(os.getenv('TWITCH_MIN_FOLLOWERS', '1000'))
DB_FILE         = os.getenv('TWITCH_DB_FILE') or os.getenv('STREAMERS_DB_FILE') or 'database.json'

# --- DATA DIR CONFIG ---
PROJECT_ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(DATA_DIR, exist_ok=True)

def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

print(f"[migrate] DATA_DIR={DATA_DIR}")
print(f"[migrate] Using DB_FILE={DB_FILE} -> {get_data_path(DB_FILE)}")
print(f"[migrate] Minimum followers threshold: {MIN_FOLLOWERS}")

def get_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    print(f"[migrate] Requesting token from Twitch...")
    try:
        res = requests.post(url, params=params, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            print(f"[migrate] Token request failed: status={res.status_code}, body={res.text[:300]}")
        res.raise_for_status()
        j = res.json()
        token = j.get('access_token')
        if not token:
            raise RuntimeError(f"No access_token in response: {j}")
        print(f"[migrate] Token acquired.")
        return token
    except Exception as e:
        print(f"[migrate] Token request exception: {e}")
        raise

def get_follower_count(broadcaster_id, token):
    """Get follower count for a specific broadcaster"""
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={broadcaster_id}"
    try:
        res = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            print(f"[migrate] Follower count request failed for {broadcaster_id}: status={res.status_code}")
            return 0
        data = res.json()
        total = data.get('total', 0)
        return total
    except Exception as e:
        print(f"[migrate] Exception getting follower count for {broadcaster_id}: {e}")
        return 0

def migrate_database():
    print(f"[migrate] Migration started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load existing database
    db_path = get_data_path(DB_FILE)
    if not os.path.exists(db_path):
        print(f"[migrate] Database file not found: {db_path}")
        return
    
    try:
        with open(db_path, encoding='utf-8') as f:
            db_data = json.load(f)
        streamers = db_data.get('database', [])
    except Exception as e:
        print(f"[migrate] Failed to load database: {e}")
        return
    
    print(f"[migrate] Loaded {len(streamers)} streamers from database")
    
    # Get token
    token = get_token()
    
    # Create backup
    backup_path = get_data_path(f"database_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    _safe_write_json(backup_path, db_data)
    print(f"[migrate] Created backup: {backup_path}")
    
    # Process each streamer
    updated = 0
    removed = 0
    valid_streamers = []
    
    for i, streamer in enumerate(streamers):
        streamer_id = streamer.get('id')
        display_name = streamer.get('display_name', 'Unknown')
        
        if not streamer_id:
            print(f"[migrate] Skipping streamer without ID: {display_name}")
            continue
        
        print(f"[migrate] Processing {i+1}/{len(streamers)}: {display_name} (ID: {streamer_id})")
        
        # Get follower count
        follower_count = get_follower_count(streamer_id, token)
        
        # Add followers field
        streamer['followers'] = follower_count
        
        # Check if meets minimum threshold
        if follower_count >= MIN_FOLLOWERS:
            valid_streamers.append(streamer)
            print(f"[migrate] ✓ {display_name}: {follower_count} followers (kept)")
        else:
            removed += 1
            print(f"[migrate] ✗ {display_name}: {follower_count} followers (removed - below {MIN_FOLLOWERS} threshold)")
        
        updated += 1
        
        # Rate limiting - small delay between requests
        if i % 10 == 0 and i > 0:
            print(f"[migrate] Processed {i} streamers, sleeping for 1 second...")
            time.sleep(1)
    
    # Save updated database
    updated_db = {'database': valid_streamers}
    _safe_write_json(db_path, updated_db)
    
    print(f"[migrate] Migration completed!")
    print(f"[migrate] - Processed: {updated} streamers")
    print(f"[migrate] - Kept: {len(valid_streamers)} streamers")
    print(f"[migrate] - Removed: {removed} streamers (below {MIN_FOLLOWERS} followers)")
    print(f"[migrate] - Backup saved to: {backup_path}")
    print(f"[migrate] Migration finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    migrate_database()
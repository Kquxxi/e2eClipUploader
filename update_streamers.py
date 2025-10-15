import sys
import requests
import os
import json
from datetime import datetime
from dotenv import load_dotenv
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
MAX_PAGES       = int(os.getenv('TWITCH_MAX_PAGES', '5'))
DB_FILE         = os.getenv('TWITCH_DB_FILE') or os.getenv('STREAMERS_DB_FILE') or 'database.json'
MIN_FOLLOWERS   = int(os.getenv('TWITCH_MIN_FOLLOWERS', '1000'))  # Minimum followers to include streamer

# --- DATA DIR CONFIG ---
PROJECT_ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(DATA_DIR, exist_ok=True)

def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

print(f"[update_streamers] DATA_DIR={DATA_DIR}")
print(f"[update_streamers] Using DB_FILE={DB_FILE} -> {get_data_path(DB_FILE)}")


def get_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    print(f"[update_streamers] Requesting token from Twitch (timeout={REQUEST_TIMEOUT}s)...")
    try:
        res = requests.post(url, params=params, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            print(f"[update_streamers] Token request failed: status={res.status_code}, body={res.text[:300]}")
        res.raise_for_status()
        j = res.json()
        token = j.get('access_token')
        if not token:
            raise RuntimeError(f"No access_token in response: {j}")
        print(f"[update_streamers] Token acquired.")
        return token
    except Exception as e:
        print(f"[update_streamers] Token request exception: {e}")
        raise


def get_follower_count(broadcaster_id, token):
    """Get follower count for a specific broadcaster"""
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    url = f"https://api.twitch.tv/helix/channels/followers?broadcaster_id={broadcaster_id}"
    try:
        print(f"[update_streamers] Getting follower count for broadcaster_id={broadcaster_id}")
        res = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            print(f"[update_streamers] Follower count request failed: status={res.status_code}, body={res.text[:300]}")
            return 0
        data = res.json()
        total = data.get('total', 0)
        print(f"[update_streamers] Broadcaster {broadcaster_id} has {total} followers")
        return total
    except Exception as e:
        print(f"[update_streamers] Exception getting follower count for {broadcaster_id}: {e}")
        return 0


def fetch_polish_streamers(token, max_pages=MAX_PAGES):
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    streamers = []
    url    = "https://api.twitch.tv/helix/streams"
    params = {'language': 'pl', 'first': 100}
    print(f"[update_streamers] Fetching Polish streamers (max_pages={max_pages}, timeout={REQUEST_TIMEOUT}s)...")
    for page_idx in range(max_pages):
        try:
            print(f"[update_streamers] Page {page_idx+1}/{max_pages}: requesting...")
            res = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            status = res.status_code
            rl_rem = res.headers.get('Ratelimit-Remaining') or res.headers.get('ratelimit-remaining')
            rl_reset = res.headers.get('Ratelimit-Reset') or res.headers.get('ratelimit-reset')
            if status != 200:
                print(f"[update_streamers] Page {page_idx+1}: non-200 status={status}, body={res.text[:300]}")
            try:
                data_json = res.json()
            except Exception:
                data_json = {'text': res.text}
            data = data_json.get('data', [])
            print(f"[update_streamers] Page {page_idx+1}: got {len(data)} items, rate_remaining={rl_rem}, rate_reset={rl_reset}")
            for s in data:
                user_id = s.get('user_id')
                user_login = s.get('user_login')
                user_name = s.get('user_name')
                
                # Get follower count for this streamer
                follower_count = get_follower_count(user_id, token)
                
                # Only add streamer if they meet minimum follower requirement
                if follower_count >= MIN_FOLLOWERS:
                    streamers.append({
                        'id':           user_id,
                        'login':        user_login,
                        'display_name': user_name,
                        'followers':    follower_count
                    })
                    print(f"[update_streamers] Added {user_name} with {follower_count} followers")
                else:
                    print(f"[update_streamers] Skipped {user_name} with {follower_count} followers (below {MIN_FOLLOWERS} threshold)")
            cursor = (data_json.get('pagination') or {}).get('cursor')
            print(f"[update_streamers] Page {page_idx+1}: cursor={'present' if cursor else 'none'}")
            if not cursor or not data:
                print("[update_streamers] No cursor or empty data -> stopping pagination.")
                break
            params['after'] = cursor
        except Exception as e:
            print(f"[update_streamers] Exception on page {page_idx+1}: {e}")
            break
    print(f"[update_streamers] Total fetched streamers: {len(streamers)}")
    return streamers


def load_existing():
    path = get_data_path(DB_FILE)
    if os.path.exists(path):
        print(f"[update_streamers] Loading existing DB from {path}")
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[update_streamers] Failed to load existing DB: {e}")
            return {'database': []}
    print(f"[update_streamers] DB file not found at {path}, starting with empty DB.")
    return {'database': []}


def save(streamers_data):
    path = get_data_path(DB_FILE)
    print(f"[update_streamers] Saving {len(streamers_data)} streamers to {path}")
    _safe_write_json(path, {'database': streamers_data})


def job():
    print(f"[update_streamers] Job started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[update_streamers] Using minimum followers threshold: {MIN_FOLLOWERS}")
    try:
        token    = get_token()
        new_list = fetch_polish_streamers(token)
        db = load_existing().get('database', [])
        existing = {s.get('id'): s for s in db if s.get('id')}

        added = 0
        updated = 0
        
        for s in new_list:
            streamer_id = s.get('id')
            if not streamer_id:
                continue
                
            if streamer_id in existing:
                # Update existing streamer's follower count
                existing_streamer = existing[streamer_id]
                old_followers = existing_streamer.get('followers', 0)
                existing_streamer['followers'] = s['followers']
                if old_followers != s['followers']:
                    updated += 1
                    print(f"[update_streamers] Updated {s['display_name']}: {old_followers} -> {s['followers']} followers")
            else:
                # Add new streamer (already filtered by MIN_FOLLOWERS in fetch_polish_streamers)
                db.append(s)
                added += 1

        # Filter out existing streamers that no longer meet the minimum followers requirement
        original_count = len(db)
        db = [s for s in db if s.get('followers', 0) >= MIN_FOLLOWERS]
        removed = original_count - len(db)
        
        if removed > 0:
            print(f"[update_streamers] Removed {removed} streamers below {MIN_FOLLOWERS} followers threshold")

        save(db)
        print(f"[update_streamers] Added {added} new streamers, updated {updated} existing streamers, removed {removed} below threshold. Total in DB: {len(db)}")
    except Exception as e:
        print(f"[update_streamers] Job failed: {e}")
    finally:
        print(f"[update_streamers] Job finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    # Pierwsze uruchomienie od razu
    job()

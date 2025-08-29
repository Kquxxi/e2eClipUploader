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
import os, json

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

# --- DATA DIR CONFIG ---
PROJECT_ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(DATA_DIR, exist_ok=True)

def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

def get_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    res = requests.post(url, params=params)
    return res.json()['access_token']

def fetch_polish_streamers(token, max_pages=5):
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    streamers = []
    url    = "https://api.twitch.tv/helix/streams"
    params = {'language': 'pl', 'first': 100}
    for _ in range(max_pages):
        res = requests.get(url, headers=headers, params=params).json()
        data = res.get('data', [])
        for s in data:
            streamers.append({
                'id':           s['user_id'],
                'login':        s['user_login'],
                'display_name': s['user_name']
            })
        cursor = res.get('pagination', {}).get('cursor')
        if not cursor:
            break
        params['after'] = cursor
    return streamers

def load_existing():
    path = get_data_path('database.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    # fallback: jeśli nie istnieje, stwórz pustą bazę
    return {'database': []}

def save(streamers_data):
    _safe_write_json(get_data_path('database.json'), {'database': streamers_data})

def job():
    token    = get_token()
    new_list = fetch_polish_streamers(token)
    db = load_existing()['database']
    existing = {s['id'] for s in db}

    added = 0
    for s in new_list:
        if s['id'] not in existing:
            db.append(s)
            added += 1

    save(db)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Dodano {added} nowych streamerów. Łącznie w bazie: {len(db)}")

if __name__ == "__main__":
    # Pierwsze uruchomienie od razu
    job()

import os, json, sys
from datetime import datetime, timedelta, timezone
from kickapi import KickAPI
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')
load_dotenv()  # prefer .env first
load_dotenv('config.env')  # fallback
api = KickAPI()
base = os.path.dirname(__file__)

# --- DATA DIR CONFIG ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(os.path.join(DATA_DIR, 'kick'), exist_ok=True)

# --- Kick filters from ENV ---
KICK_WINDOW_HOURS = int(os.getenv('KICK_WINDOW_HOURS', '24'))
KICK_MIN_VIEWS = int(os.getenv('KICK_MIN_VIEWS', '20'))

def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

# --- safe atomic write helper ---
import json, os

def _safe_write_json(path: str, obj):
    # ensure dir exists
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
# --- end helper ---

# wybór pliku bazy streamerów: ENV > DATA_DIR/kick/kick_database.json > repo/kick/kick_database.json
db_file = os.getenv('KICK_DATABASE_FILE')
if not db_file:
    candidate = get_data_path('kick', 'kick_database.json')
    if os.path.exists(candidate):
        db_file = candidate
    else:
        db_file = os.path.join(base, 'kick_database.json')

with open(db_file, encoding='utf-8') as f:
    streamers = json.load(f)['database']

now = datetime.now(timezone.utc)
day_ago = now - timedelta(hours=KICK_WINDOW_HOURS)

# --- Wczytaj stare klipy (jeśli plik istnieje)
cache_path = get_data_path('kick', 'kick_clips_cache.json')
if os.path.exists(cache_path):
    with open(cache_path, encoding='utf-8') as f:
        old_clips = json.load(f)
else:
    old_clips = []

# Zamien na dict po url (albo ID jak masz)
clip_dict = {c['url']: c for c in old_clips}

def get_clips_kick(slug):
    try:
        clips = api.channel(slug).clips
        return [
            {
                'broadcaster': s['display_name'],
                'title': c.title,
                'url': f"https://kick.com/{slug}/clips/{c.id}",
                'views': c.views,
                'created_at': c.created_at,
            }
            for c in clips
            if datetime.fromisoformat(c.created_at.replace('Z','+00:00')) > day_ago
        ]
    except Exception as e:
        print(f"[ERROR] {slug}: {e}")
        return []

# --- Update'ujemy stare klipy
for s in streamers:
    for c in get_clips_kick(s['slug']):
        if c['url'] in clip_dict:
            # Update liczby wyświetleń i tytułu jeśli się zmienił
            clip_dict[c['url']]['views'] = c['views']
            clip_dict[c['url']]['title'] = c['title']
        else:
            # Dodaj nowy klip
            clip_dict[c['url']] = c

# Filtrujemy klipy z wybranego okna i progu wyświetleń
final_clips = [
    c for c in clip_dict.values()
    if datetime.fromisoformat(c['created_at'].replace('Z','+00:00')) > day_ago and c.get('views',0) >= KICK_MIN_VIEWS
]

_safe_write_json(cache_path, final_clips)

print(f"[OK] Zapisano {len(final_clips)} klipów w kick_clips_cache.json")

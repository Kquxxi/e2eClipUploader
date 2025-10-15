import os, json, sys
from datetime import datetime, timedelta, timezone
from collections import Counter
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')
base = os.path.dirname(__file__)
load_dotenv()  # prefer .env first
load_dotenv('config.env')  # fallback legacy

# --- DATA DIR CONFIG ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(os.path.join(DATA_DIR, 'kick'), exist_ok=True)

# Kick filters from ENV
KICK_WINDOW_HOURS = int(os.getenv('KICK_WINDOW_HOURS', '24'))
KICK_MIN_VIEWS = int(os.getenv('KICK_MIN_VIEWS', '20'))

def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

# --- safe atomic write helpers ---
import os
import json

def _safe_write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _safe_write_json(path: str, obj):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
# --- end helpers ---

# Wczytaj cache klipów (toleruj BOM)
with open(get_data_path('kick', 'kick_clips_cache.json'), encoding='utf-8-sig') as f:
    all_clips = json.load(f)

now = datetime.now(timezone.utc)
day_ago = now - timedelta(hours=KICK_WINDOW_HOURS)

# Filtruj i sortuj jak dotąd:
filtered = [
    {
        'broadcaster': c['broadcaster'],
        'title': c['title'],
        'url': c['url'],
        'views': c['views'],
        'relative_time': (
            lambda created:
                f"{(now-delta).days}d ago" if (delta := now - datetime.fromisoformat(created.replace('Z','+00:00'))).days > 0
            else f"{delta.seconds//3600}h ago" if delta.seconds >= 3600
            else f"{delta.seconds//60}m ago" if delta.seconds >= 60
            else f"{delta.seconds}s ago"
        )(c['created_at'])
    }
    for c in all_clips
    if datetime.fromisoformat(c['created_at'].replace('Z','+00:00')) > day_ago and c['views'] >= KICK_MIN_VIEWS
]

filtered.sort(key=lambda c: c['views'], reverse=True)

# Statystyki
b_counts = Counter(c['broadcaster'] for c in filtered)
stats = {
    'total_clips': len(filtered),
    'top_streamers': b_counts.most_common(3)
}

# Zapis danych atomowo
_safe_write_json(get_data_path('kick','raport_kick_data.json'), {'clips': filtered, 'stats': stats})

# Render HTML (jak dotąd)
env = Environment(loader=FileSystemLoader(base))
tpl = env.get_template('template_kick.html')
html = tpl.render(clips=filtered, stats=stats)
_safe_write_text(get_data_path('kick', 'raport_kick.html'), html)

print("Kick raport wygenerowany: kick/raport_kick.html")

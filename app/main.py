import sys
import os
import subprocess
import json
from flask import Flask, render_template, jsonify, send_file, request, Response, make_response
from subprocess import CalledProcessError
# --- new imports for local parity ---
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import threading
from contextlib import contextmanager
from werkzeug.exceptions import HTTPException
import shutil
from urllib.parse import urlsplit
import requests
import datetime
import time
import uuid
import re


app = Flask(
    __name__,
    static_folder='../static',
    template_folder='../templates'
)

# --- load env (supports both config.env and .env) ---
load_dotenv()  # prefer .env
load_dotenv('config.env')  # fallback for legacy

# --- configuration: DATA_DIR, HOST/PORT/DEBUG ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.abspath(os.getenv('DATA_DIR', PROJECT_ROOT))
os.makedirs(DATA_DIR, exist_ok=True)

# Globalny handler wyjątków: dla ścieżek API zawsze zwracaj JSON zamiast HTML
@app.errorhandler(Exception)
def _json_errors_for_api(e):
    # Jeśli to HTTPException, zachowaj kod odpowiedzi; inaczej 500
    status_code = 500
    if isinstance(e, HTTPException):
        try:
            status_code = int(getattr(e, 'code', 500) or 500)
        except Exception:
            status_code = 500
    # Dla ścieżek /api/* zwracaj JSON (aby front nie dostawał HTML "<!DOCTYPE ...")
    try:
        path = request.path or ''
    except Exception:
        path = ''
    if isinstance(path, str) and path.startswith('/api/'):
        payload = {
            'ok': False,
            'error': str(e),
            'type': e.__class__.__name__,
        }
        # Dodaj skrócony traceback dla diagnostyki (bez zasypywania odpowiedzi)
        try:
            import traceback
            tb = traceback.format_exc()
            if isinstance(tb, str):
                payload['traceback'] = tb[-2000:]
        except Exception:
            pass
        resp = jsonify(payload)
        try:
            resp.headers['Cache-Control'] = 'no-store'
            resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
        except Exception:
            pass
        return resp, status_code
    # Poza /api/* pozwól działać domyślnemu handlerowi (HTML)
    raise e

# Allow importing pipeline.transcribe.adapter without installing as package
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
try:
    from pipeline.transcribe.adapter import transcribe_srt as _transcribe_srt
except Exception:
    _transcribe_srt = None

# --- global scheduler instance reference (for schedule-info endpoint) ---
scheduler_instance = None
def get_data_path(*parts: str) -> str:
    return os.path.join(DATA_DIR, *parts)

# --- helpers for running scripts with absolute paths ---
def _abs_path(*parts: str) -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', *parts))

# --- simple cross-platform file lock ---
@contextmanager
def file_lock(lock_path: str):
    acquired = False
    # ensure lock dir exists
    os.makedirs(os.path.dirname(lock_path) or '.', exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(str(os.getpid()))
        acquired = True
    except FileExistsError:
        pass
    try:
        yield acquired
    finally:
        if acquired:
            try:
                os.remove(lock_path)
            except OSError:
                pass


def run_script(script_rel_path: str):
    script = _abs_path(script_rel_path)
    # Forward child process output to server stdout/stderr for visibility
    return subprocess.run([sys.executable, script], text=True, check=True)

# --- scheduled jobs ---
def job_update_streamers():
    try:
        run_script('update_streamers.py')
    except CalledProcessError as e:
        print('[scheduler] update_streamers failed:', e.stderr or str(e))


def job_generate_twitch_report():
    # Ensure organized directory exists
    os.makedirs(get_data_path('reports', 'twitch'), exist_ok=True)
    lock_path = get_data_path('reports', 'twitch', 'generate_raport.lock')
    # Guard: remove stale lock (>20 min) to prevent permanent blocking
    try:
        if os.path.exists(lock_path):
            st = os.stat(lock_path)
            age_sec = time.time() - st.st_mtime
            if age_sec > 20 * 60:
                os.remove(lock_path)
                print('[twitch] removed stale lock (>20 min)')
    except Exception:
        pass
    with file_lock(lock_path) as acquired:
        if not acquired:
            print('[lock] generate_raport already running; skipping.')
            return
        # Nie usuwaj starego raportu – nowy plik nadpisze go atomowo.
        # Dzięki temu stary raport pozostaje widoczny, dopóki nie powstanie nowy.
        try:
            run_script('generate_raport.py')
        except CalledProcessError as e:
            print('[scheduler] generate_raport failed:', e.stderr or str(e))


def job_refresh_kick_and_report():
    print('[kick] job_refresh_kick_and_report: start')
    # ensure kick subdir exists inside DATA_DIR
    os.makedirs(get_data_path('kick'), exist_ok=True)
    lock_path = get_data_path('kick', 'raport_kick.lock')
    # Guard: remove stale lock (>30 min) to prevent permanent blocking
    try:
        if os.path.exists(lock_path):
            st = os.stat(lock_path)
            age_sec = time.time() - st.st_mtime
            if age_sec > 30*60:
                os.remove(lock_path)
                print('[kick] removed stale lock (>30 min)')
    except Exception:
        pass
    with file_lock(lock_path) as acquired:
        if not acquired:
            print('[lock] kick scrape/report already running; skipping.')
            return
        print('[kick] lock acquired')
        # przygotuj ścieżki
        progress_path = get_data_path('kick', 'progress.json')
        # inicjalny progres: start scraping
        try:
            _safe_write_json(progress_path, {
                'status': 'scraping',
                'total': 0,
                'processed': 0,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            })
            print('[kick] progress initialized: scraping')
        except Exception:
            print('[kick] progress init failed')
            pass
        # Nie usuwaj starego raportu Kick – nowy zostanie zapisany atomowo.
        # To zapobiega znikaniu raportu w trakcie generowania.
        try:
            print('[kick] running scrape_kick_clips.py')
            run_script(os.path.join('kick', 'scrape_kick_clips.py'))
            print('[kick] scrape completed')
            # po scrape: zaktualizuj total i status
            try:
                cache_path = get_data_path('kick', 'kick_clips_cache.json')
                total = 0
                if os.path.exists(cache_path):
                    with open(cache_path, encoding='utf-8-sig') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            total = len(data)
                _safe_write_json(progress_path, {
                    'status': 'generating',
                    'total': total,
                    'processed': 0,
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                })
                print(f"[kick] scrape total clips: {total}")
            except Exception:
                pass
            print('[kick] running generate_raport_kick.py')
            run_script(os.path.join('kick', 'generate_raport_kick.py'))
            print('[kick] generate completed')
            # po generowaniu: ustaw finished
            try:
                # processed = total jeśli znamy total
                prog = _safe_read_json(progress_path, {
                    'status': 'generating', 'total': 0, 'processed': 0
                })
                total = int(prog.get('total') or 0)
                _safe_write_json(progress_path, {
                    'status': 'finished',
                    'total': total,
                    'processed': total,
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                })
                print('[kick] progress set to finished')
            except Exception:
                pass
        except CalledProcessError as e:
            print('[kick] job failed:', e.stderr or str(e))
            try:
                _safe_write_json(progress_path, {
                    'status': 'error',
                    'total': 0,
                    'processed': 0,
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                pass


# --- safe atomic write/read helpers for JSON ---
def _safe_write_json(path: str, obj):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _safe_read_json(path: str, default=None):
    # Najpierw spróbuj odczytać plik docelowy, jeśli nie wyjdzie — spróbuj plik tymczasowy
    # To zabezpiecza przed rzadkimi przypadkami, gdy replace jest w trakcie i plik końcowy
    # jest chwilowo niekompletny (np. widoczny jako "{").
    for candidate in (path, path + '.tmp'):
        try:
            with open(candidate, encoding='utf-8-sig') as f:
                return json.load(f)
        except Exception:
            continue
    return default

@app.route('/')
def index():
    # cache_buster wymusza odświeżenie zasobów statycznych po zmianach CSS/JS
    resp = make_response(render_template('index.html', cache_buster=int(time.time())))
    try:
        resp.headers['Cache-Control'] = 'no-store'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
    except Exception:
        pass
    return resp

# --- Admin Panel ---
@app.route('/admin')
def admin_panel():
    # Prosty panel administracyjny do edycji preferencji streamerów
    resp = make_response(render_template('admin.html', cache_buster=int(time.time())))
    try:
        resp.headers['Cache-Control'] = 'no-store'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
    except Exception:
        pass
    return resp

@app.route('/api/streamers-prefs', methods=['GET', 'POST'])
def api_streamers_prefs():
    prefs_default = { 'highlighted': [], 'skipped': [], 'tags': {}, 'platforms': {}, 'tag_groups': {} }
    prefs_path = get_data_path('streamers_prefs.json')
    if request.method == 'GET':
        prefs = _safe_read_json(prefs_path, prefs_default) or prefs_default
        # Upewnij się, że brakujące pola są obecne (zgodne wstecz)
        for k,v in prefs_default.items():
            if k not in prefs:
                prefs[k] = v
        return _no_cache(jsonify(prefs))
    # POST: zapisz preferencje
    payload = request.get_json(silent=True) or {}
    highlighted = payload.get('highlighted') or []
    skipped = payload.get('skipped') or []
    tags = payload.get('tags') or {}
    platforms = payload.get('platforms') or {}
    tag_groups = payload.get('tag_groups') or {}
    # prosta walidacja typów
    if not isinstance(highlighted, list) or not isinstance(skipped, list) or not isinstance(tags, dict) or not isinstance(platforms, dict) or not isinstance(tag_groups, dict):
        return jsonify({'error': 'Nieprawidłowe typy pól'}), 400
    # Walidacja i normalizacja tag_groups: dict[str] -> list[str]
    norm_tag_groups = {}
    try:
        for raw_name, raw_tags in (tag_groups or {}).items():
            name = str(raw_name).strip().lower()
            if not name:
                continue
            if not isinstance(raw_tags, list):
                continue
            clean_tags = []
            for t in raw_tags:
                s = str(t).strip()
                if not s:
                    continue
                # Upewnij się, że każdy tag zaczyna się od '#'
                if not s.startswith('#'):
                    s = '#' + s
                clean_tags.append(s)
            # deduplikacja przy zachowaniu kolejności
            seen = set()
            dedup = []
            for t in clean_tags:
                lt = t.lower()
                if lt in seen:
                    continue
                seen.add(lt)
                dedup.append(t)
            norm_tag_groups[name] = dedup
    except Exception:
        # w razie problemów pozostaw pustą strukturę
        norm_tag_groups = {}
    new_prefs = {
        'highlighted': list(dict.fromkeys([str(x).strip() for x in highlighted if str(x).strip()])),
        'skipped': list(dict.fromkeys([str(x).strip() for x in skipped if str(x).strip()])),
        'tags': tags,
        'platforms': platforms,
        'tag_groups': norm_tag_groups,
    }
    _safe_write_json(prefs_path, new_prefs)
    return _no_cache(jsonify({'ok': True}))

@app.route('/api/suggest-streamers')
def api_suggest_streamers():
    q = (request.args.get('q') or '').strip().lower()
    platform = (request.args.get('platform') or 'all').strip().lower()
    suggestions = []
    # Twitch
    try:
        db_twitch = _safe_read_json(get_data_path('database.json'), {'database': []}) or {'database': []}
        for s in db_twitch.get('database') or []:
            name = s.get('display_name') or ''
            if not name:
                continue
            if platform in ('all', 'twitch') and (not q or q in name.lower()):
                suggestions.append({'name': name, 'platform': 'twitch'})
    except Exception:
        pass
    # Kick
    try:
        db_kick = _safe_read_json(get_data_path('kick', 'kick_database.json'), {'database': []}) or {'database': []}
        for s in db_kick.get('database') or []:
            name = s.get('display_name') or s.get('slug') or ''
            if not name:
                continue
            if platform in ('all', 'kick') and (not q or q in name.lower()):
                suggestions.append({'name': name, 'platform': 'kick'})
    except Exception:
        pass
    # deduplikacja według (name, platform)
    seen = set()
    unique = []
    for item in suggestions:
        key = (item['name'], item['platform'])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return _no_cache(jsonify({'suggestions': unique}))

@app.route('/api/update-streamers')
def api_update_streamers():
    try:
        res = run_script('update_streamers.py')
        return jsonify({'message': res.stdout})
    except CalledProcessError as e:
        # zwracamy stderr i code, by łatwiej debugować
        return jsonify({'error': e.stderr or str(e), 'code': e.returncode}), 500

@app.route('/api/generate-raport')
def api_generate_raport():
    # Jeśli raport już się generuje (lock istnieje), nie spawnuj nowego wątku
    lock_path = get_data_path('reports', 'twitch', 'generate_raport.lock')
    try:
        if os.path.exists(lock_path):
            return _no_cache(jsonify({'message': 'Raport już w toku', 'already_running': True})), 409
    except Exception:
        pass
    # uruchamiamy generowanie w tle, kasowanie starego raportu jest w jobie pod lockiem
    threading.Thread(target=job_generate_twitch_report, daemon=True).start()
    return _no_cache(jsonify({'message': 'Generowanie raportu uruchomione', 'already_running': False})), 202

@app.route('/raport')
def raport():
    new_path = get_data_path('reports', 'twitch', 'raport.html')
    return send_file(new_path)

@app.route('/api/report-ready')
def api_report_ready():
    new_path = get_data_path('reports', 'twitch', 'raport.html')
    ready = os.path.exists(new_path)
    return _no_cache(jsonify({'ready': ready}))

@app.route('/api/report-status')
def api_report_status():
    progress_default = {
        'status': 'idle',
        'total': 0,
        'processed': 0,
        'last_completed': None,
        'updated_at': None,
    }
    new_progress_path = get_data_path('reports', 'twitch', 'progress.json')
    progress = _safe_read_json(new_progress_path, progress_default)
    events_path = get_data_path('reports', 'twitch', 'events.log')
    recent_events = []
    active_streamers = []
    try:
        with open(events_path, encoding='utf-8') as f:
            lines = f.readlines()
        last_lines = lines[-200:]
        for line in last_lines:
            try:
                ev = json.loads(line.strip())
                if isinstance(ev, dict) and ev.get('event') in ('start', 'done'):
                    recent_events.append(ev)
            except Exception:
                pass
        counts = {}
        for ev in recent_events:
            name = ev.get('name')
            if not name:
                continue
            counts.setdefault(name, 0)
            if ev.get('event') == 'start':
                counts[name] += 1
            elif ev.get('event') == 'done':
                counts[name] -= 1
        active_streamers = sorted([n for n, c in counts.items() if c > 0])
    except Exception:
        recent_events = []
        active_streamers = []
    workers = int(os.getenv('RAPORT_MAX_WORKERS', '5'))
    return _no_cache(jsonify({
        'progress': progress,
        'active_streamers': active_streamers,
        'recent_events': recent_events[-20:],
        'workers': workers,
    }))

# --- ADMIN: force unlock Twitch report + reset progress ---
@app.route('/api/unlock-twitch', methods=['POST'])
def api_unlock_twitch():
    lock_path = get_data_path('reports', 'twitch', 'generate_raport.lock')
    progress_path = get_data_path('reports', 'twitch', 'progress.json')
    removed = False
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
            removed = True
    except Exception:
        pass
    # reset progress to idle (helps UI leave stuck state)
    try:
        _safe_write_json(progress_path, {
            'status': 'idle',
            'total': 0,
            'processed': 0,
            'last_completed': None,
            'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
    except Exception:
        pass
    return _no_cache(jsonify({'ok': True, 'lock_removed': removed}))

# --- NEW: scheduler info (next run times) ---
@app.route('/api/schedule-info')
def api_schedule_info():
    # Użyj timezone-aware czasu w UTC, aby poprawnie policzyć różnice
    now = datetime.datetime.now(datetime.timezone.utc)
    info = {
        'server_time': now.isoformat() + 'Z',
        'generate_report': None,
        'update_streamers': None,
        'kick_refresh': None,
    }
    try:
        sched = scheduler_instance
        if sched:
            jr = sched.get_job('generate_report')
            if jr and getattr(jr, 'next_run_time', None):
                nrt = jr.next_run_time
                try:
                    # różnica czasu (aware) w sekundach
                    seconds = int((nrt - now).total_seconds())
                except Exception:
                    seconds = None
                info['generate_report'] = {
                    'next_run_time': nrt.isoformat(),
                    'seconds_to_next': max(0, seconds) if isinstance(seconds, int) else None,
                }
            ju = sched.get_job('update_streamers')
            if ju and getattr(ju, 'next_run_time', None):
                nrt = ju.next_run_time
                try:
                    seconds = int((nrt - now).total_seconds())
                except Exception:
                    seconds = None
                info['update_streamers'] = {
                    'next_run_time': nrt.isoformat(),
                    'seconds_to_next': max(0, seconds) if isinstance(seconds, int) else None,
                }
            jk = sched.get_job('kick_refresh')
            if jk and getattr(jk, 'next_run_time', None):
                nrt = jk.next_run_time
                try:
                    seconds = int((nrt - now).total_seconds())
                except Exception:
                    seconds = None
                info['kick_refresh'] = {
                    'next_run_time': nrt.isoformat(),
                    'seconds_to_next': max(0, seconds) if isinstance(seconds, int) else None,
                }
    except Exception as e:
        info['error'] = str(e)
    return jsonify(info)

@app.route('/raport-fragment')
def raport_fragment():
    # wczytujemy JSON z bazy gotowych danych (bezpiecznie, tolerując BOM i częściowe zapisy)
    new_data_path = get_data_path('reports', 'twitch', 'raport_data.json')
    default_data = {'clips': [], 'stats': {'total_clips': 0, 'top_categories': [], 'top_streamers': []}}
    data = _safe_read_json(new_data_path, default_data) or default_data
    # defensywnie upewnij się, że klucze istnieją
    clips = data.get('clips') or []
    stats = data.get('stats') or {'total_clips': 0, 'top_categories': [], 'top_streamers': []}
    # wczytaj preferencje streamerów (wyróżnieni / pomijani / tagi)
    prefs_default = { 'highlighted': [], 'skipped': [], 'tags': {}, 'platforms': {} }
    prefs = _safe_read_json(get_data_path('streamers_prefs.json'), prefs_default) or prefs_default
    # Ujednolicenie wielkości liter dla dopasowania nazw
    highlighted_streamers = set([str(s).lower() for s in (prefs.get('highlighted') or [])])
    skipped_streamers = set([str(s).lower() for s in (prefs.get('skipped') or [])])
    # filtruj klipy dla pomijanych streamerów (proste dopasowanie po display_name)
    if skipped_streamers:
        clips = [c for c in clips if (c.get('broadcaster') or '').lower() not in skipped_streamers]
        # przeliczenie statystyk po filtrze (opcjonalnie)
        try:
            from collections import Counter
            total_clips = len(clips)
            cat_counts = Counter(c.get('category') for c in clips)
            broad_counts = Counter(c.get('broadcaster') for c in clips)
            stats = {
                'total_clips': total_clips,
                'top_categories': cat_counts.most_common(3),
                'top_streamers': broad_counts.most_common(3),
            }
        except Exception:
            pass
    return _no_cache(render_template('raport_fragment.html', clips=clips, stats=stats, highlighted_streamers=highlighted_streamers))

# --- NEW: selection API and editor page ---
@app.route('/api/selection', methods=['POST'])
def api_selection():
    payload = request.get_json(silent=True) or {}
    clips = payload.get('clips', [])
    if not isinstance(clips, list):
        return jsonify({'error': 'Invalid payload: clips must be a list'}), 400
    # minimal sanitize: keep only allowed keys
    allowed_keys = {'url', 'title', 'broadcaster'}
    clean = []
    for c in clips:
        if isinstance(c, dict) and 'url' in c:
            clean.append({k: c.get(k) for k in allowed_keys})
    _safe_write_json(get_data_path('selection.json'), {'clips': clean})
    return jsonify({'ok': True, 'count': len(clean)})

# --- Add clip by direct URL (Twitch/Kick) ---
def _parse_twitch_clip_id(url: str):
    try:
        p = urlsplit(url)
        path = p.path or ''
        segs = [s for s in path.split('/') if s]
        if 'clips.twitch.tv' in (p.netloc or ''):
            return segs[-1] if segs else None
        for i, s in enumerate(segs):
            if s.lower() == 'clip' and i+1 < len(segs):
                return segs[i+1]
    except Exception:
        pass
    return None

def _resolve_twitch_metadata(url: str):
    clip_id = _parse_twitch_clip_id(url)
    if not clip_id:
        return {}
    client_id = os.getenv('TWITCH_CLIENT_ID')
    client_secret = os.getenv('TWITCH_CLIENT_SECRET')
    if not client_id or not client_secret:
        return {'title': None, 'broadcaster': None, 'clip_id': clip_id}
    try:
        tok = requests.post(
            'https://id.twitch.tv/oauth2/token',
            data={'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'client_credentials'},
            timeout=20
        ).json()
        access_token = tok.get('access_token')
        if not access_token:
            return {'title': None, 'broadcaster': None, 'clip_id': clip_id}
        res = requests.get(
            'https://api.twitch.tv/helix/clips',
            headers={'Client-ID': client_id, 'Authorization': f'Bearer {access_token}'},
            params={'id': clip_id},
            timeout=20
        )
        data = {}
        try:
            data = res.json() or {}
        except Exception:
            data = {}
        items = data.get('data') or []
        if items:
            it = items[0]
            return {'title': it.get('title'), 'broadcaster': it.get('broadcaster_name') or it.get('creator_name'), 'clip_id': clip_id}
    except Exception:
        pass
    return {'title': None, 'broadcaster': None, 'clip_id': clip_id}

def _resolve_kick_metadata(url: str):
    try:
        p = urlsplit(url)
        segs = [s for s in (p.path or '').split('/') if s]
        if len(segs) >= 3 and segs[-2].lower() == 'clips':
            slug = segs[-3]
            cid = segs[-1]
        else:
            slug = segs[0] if segs else None
            cid = segs[-1] if segs else None
        broadcaster = slug or None
        title = None
        try:
            from kickapi import KickAPI
            api = KickAPI()
            ch = api.channel(slug)
            for c in getattr(ch, 'clips', []) or []:
                if str(getattr(c, 'id', '')) == str(cid):
                    title = getattr(c, 'title', None)
                    try:
                        bj = getattr(ch, 'json', {}) or {}
                        broadcaster = bj.get('display_name') or broadcaster
                    except Exception:
                        pass
                    break
        except Exception:
            pass
        return {'title': title, 'broadcaster': broadcaster, 'clip_id': cid}
    except Exception:
        return {'title': None, 'broadcaster': None, 'clip_id': None}

def _resolve_clip_metadata(url: str):
    try:
        p = urlsplit(url)
        host = (p.netloc or '').lower()
    except Exception:
        host = ''
    if 'twitch.tv' in host:
        return _resolve_twitch_metadata(url)
    if 'kick.com' in host or 'stream.kick.com' in host:
        return _resolve_kick_metadata(url)
    return {'title': None, 'broadcaster': None}

@app.route('/api/add-clip-by-url', methods=['POST'])
def api_add_clip_by_url():
    data = request.get_json(silent=True) or {}
    url = (data.get('url') or '').strip()
    if not url:
        return jsonify({'ok': False, 'error': 'Brak URL klipu'}), 400
    meta = _resolve_clip_metadata(url)
    title = meta.get('title') or '(bez tytułu)'
    broadcaster = meta.get('broadcaster') or ''
    sel = _safe_read_json(get_data_path('selection.json'), default={'clips': []})
    clips = sel.get('clips', [])
    clips = [c for c in clips if c.get('url') != url]
    clip_obj = {'url': url, 'title': title, 'broadcaster': broadcaster}
    clips.append(clip_obj)
    _safe_write_json(get_data_path('selection.json'), {'clips': clips})
    return jsonify({'ok': True, 'clip': clip_obj, 'count': len(clips)})

@app.route('/editor')
def editor():
    sel = _safe_read_json(get_data_path('selection.json'), default={'clips': []})
    clips = sel.get('clips', [])
    resp = make_response(render_template('editor.html', clips=clips))
    try:
        resp.headers['Cache-Control'] = 'no-store'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
    except Exception:
        pass
    return resp

# --- NEW: helpers for downloader/preview ---
def _clip_id_from_url(url: str) -> str:
    try:
        p = urlsplit(url)
        segs = [s for s in p.path.split('/') if s]
        if segs:
            base = segs[-1].split('?')[0]
            # sanitize
            base = ''.join(ch for ch in base if ch.isalnum() or ch in ('-', '_'))
            return base or 'clip'
    except Exception:
        pass
    return 'clip'

def _ensure_media_dirs():
    os.makedirs(get_data_path('media', 'clips'), exist_ok=True)
    os.makedirs(get_data_path('media', 'previews'), exist_ok=True)

def _download_with_ytdlp(url: str, out_path: str):
    # Spróbuj znaleźć yt-dlp w PATH, a jeśli brak, użyj "python -m yt_dlp"
    ytdlp = shutil.which('yt-dlp') or shutil.which('yt-dlp.exe')
    tmp = out_path + '.part'
    # Przygotuj bazowe polecenie, aby móc dołożyć parametry specyficzne dla domeny
    if ytdlp:
        cmd_base = [ytdlp]
    else:
        # fallback: modułowa forma przez aktualnego Pythona
        cmd_base = [sys.executable, '-m', 'yt_dlp']

    extra_args = []
    try:
        # Kick wprowadził ostrzejszą ochronę (403/Cloudflare) – wymagane cookies i realne nagłówki
        if ('kick.com' in (url or '')) or ('stream.kick.com' in (url or '')):
            # 1) Preferowane: pobierz cookies z przeglądarki (np. chrome / edge / firefox)
            browser = os.getenv('KICK_COOKIES_FROM_BROWSER')
            if browser:
                extra_args += ['--cookies-from-browser', browser]
                print(f"[yt-dlp] Kick: używam cookies z przeglądarki: {browser}")
            else:
                # Alternatywa: ścieżka do pliku cookies w formacie Netscape
                cookies_file = os.getenv('KICK_COOKIES_FILE')
                if cookies_file and os.path.isfile(cookies_file):
                    extra_args += ['--cookies', cookies_file]
                    print(f"[yt-dlp] Kick: używam cookies z pliku: {cookies_file}")

            # 2) Dodaj referer + realny UA i kilka bezpiecznych nagłówków
            referer = url
            ua = os.getenv('KICK_USER_AGENT') or (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/125.0.0.0 Safari/537.36'
            )
            extra_args += [
                '--referer', referer,
                '--add-header', f'User-Agent: {ua}',
                '--add-header', 'Origin: https://kick.com',
                '--add-header', 'Accept-Language: en-US,en;q=0.9'
            ]
    except Exception as _e:
        # Nie przerywaj – najwyżej pobieranie nie powiedzie się jak wcześniej
        print(f"[yt-dlp] Kick headers/cookies build failed: {type(_e).__name__}: {_e}")

    cmd = cmd_base + extra_args + ['-o', tmp, url]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            # jeśli fallback z modułem i nie ma pakietu, podaj czytelny komunikat
            if (cmd_base[:2] == [sys.executable, '-m']) and ('No module named' in (res.stderr or '') and 'yt_dlp' in (res.stderr or '')):
                return False, 'yt-dlp not found. Install with: pip install yt-dlp'
            return False, (res.stderr or res.stdout or 'yt-dlp failed')
        # yt-dlp może zapisać dokładnie tmp lub tmp+ext — obsłuż obie opcje
        if os.path.exists(tmp):
            os.replace(tmp, out_path)
            return True, 'downloaded'
        dirn = os.path.dirname(tmp) or '.'
        pref = os.path.basename(tmp)
        for name in os.listdir(dirn):
            if name.startswith(pref):
                os.replace(os.path.join(dirn, name), out_path)
                return True, 'downloaded'
        return False, 'downloaded file not found'
    except Exception as e:
        return False, str(e)

def _gen_preview_ffmpeg(in_path: str, out_path: str):
    # najpierw PATH, potem fallback przez imageio-ffmpeg
    ffmpeg = shutil.which('ffmpeg') or shutil.which('ffmpeg.exe')
    if not ffmpeg:
        try:
            import imageio_ffmpeg  # type: ignore
            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            return False, 'ffmpeg not found in PATH (or imageio-ffmpeg not installed)'
    cmd = [
        ffmpeg, '-y', '-i', in_path,
        '-vf', 'scale=540:-2',
        '-r', '30',
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-movflags', '+faststart',
        out_path
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            return False, (res.stderr or res.stdout or 'ffmpeg failed')
        return True, 'generated'
    except Exception as e:
        return False, str(e)

@app.route('/api/ensure-cache', methods=['POST'])
def api_ensure_cache():
    # Pobieranie wybranych klipów i generacja prewki
    sel = _safe_read_json(get_data_path('selection.json'), default={'clips': []})
    clips = sel.get('clips', [])
    _ensure_media_dirs()

    results = []
    downloaded = 0
    skipped = 0
    previewed = 0
    errors = []

    # --- progress init ---
    progress_path = get_data_path('ensure_cache_progress.json')
    try:
        _safe_write_json(progress_path, {
            'state': 'running',
            'total': len(clips),
            'done': 0,
            'errors': 0
        })
    except Exception:
        pass

    for idx, c in enumerate(clips, start=1):
        url = c.get('url')
        if not url:
            # aktualizuj progres mimo pustego URL, aby nie zawisnąć
            try:
                _safe_write_json(progress_path, {
                    'state': 'running', 'total': len(clips), 'done': idx, 'errors': len(errors)
                })
            except Exception:
                pass
            continue
        clip_id = _clip_id_from_url(url)
        out_path = get_data_path('media', 'clips', f'{clip_id}.mp4')
        prev_path = get_data_path('media', 'previews', f'{clip_id}.mp4')

        item = {'url': url, 'clip_id': clip_id, 'output': out_path, 'preview': prev_path}

        # Pobieranie jeśli brak w cache
        if os.path.exists(out_path):
            item['download'] = 'cached'
            skipped += 1
        else:
            ok, msg = _download_with_ytdlp(url, out_path)
            item['download'] = 'ok' if ok else f'error: {msg}'
            if ok:
                downloaded += 1
            else:
                errors.append({'clip_id': clip_id, 'stage': 'download', 'message': msg})

        # Prewka: generuj jeśli brak i plik wejściowy istnieje
        if os.path.exists(out_path):
            if os.path.exists(prev_path):
                item['preview'] = 'cached'
            else:
                okp, msgp = _gen_preview_ffmpeg(out_path, prev_path)
                item['preview'] = 'ok' if okp else f'error: {msgp}'
                if okp:
                    previewed += 1
                else:
                    errors.append({'clip_id': clip_id, 'stage': 'preview', 'message': msgp})

        results.append(item)

        # --- progress step ---
        try:
            _safe_write_json(progress_path, {
                'state': 'running',
                'total': len(clips),
                'done': idx,
                'errors': len(errors)
            })
        except Exception:
            pass

    # --- progress done ---
    try:
        _safe_write_json(progress_path, {
            'state': 'done',
            'total': len(clips),
            'done': len(clips),
            'errors': len(errors)
        })
    except Exception:
        pass

    return jsonify({
        'ok': True,
        'total': len(clips),
        'downloaded': downloaded,
        'skipped': skipped,
        'previews_generated': previewed,
        'errors': errors,
        'results': results
    })

# --- MEDIA SERVE ENDPOINTS + CROP API ---------------------

@app.route('/media/previews/<path:name>', methods=['GET','HEAD'])
def serve_preview(name: str):
    path = get_data_path('media', 'previews', name)
    if not os.path.exists(path):
        resp = jsonify({'error': 'Not Found'})
        resp.status_code = 404
        resp.headers['Cache-Control'] = 'no-store'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return resp
    return _send_file_partial(path)

@app.route('/media/clips/<path:name>', methods=['GET','HEAD'])
def serve_clip(name: str):
    path = get_data_path('media', 'clips', name)
    if not os.path.exists(path):
        resp = jsonify({'error': 'Not Found'})
        resp.status_code = 404
        resp.headers['Cache-Control'] = 'no-store'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return resp
    return _send_file_partial(path)

# --- helper: partial content streaming for video ---
def _send_file_partial(path: str):
    try:
        range_header = request.headers.get('Range')
    except Exception:
        range_header = None
    try:
        method = request.method
    except Exception:
        method = 'GET'
    size = os.path.getsize(path)
    try:
        print(f'[media] {method} {path} Range={range_header}')
    except Exception:
        pass
    # Brak nagłówka Range
    if not range_header:
        if method == 'HEAD':
            resp = Response(status=200)
            resp.headers['Content-Type'] = 'video/mp4'
            resp.headers['Content-Length'] = str(size)
            resp.headers['Accept-Ranges'] = 'bytes'
            resp.headers['Cache-Control'] = 'no-transform, public, max-age=0'
            resp.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
            resp.headers['X-Accel-Buffering'] = 'no'
            resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
            return resp
        resp = send_file(path, mimetype='video/mp4')
        resp.headers['Accept-Ranges'] = 'bytes'
        resp.headers['Cache-Control'] = 'no-transform, public, max-age=0'
        resp.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
        resp.headers['X-Accel-Buffering'] = 'no'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return resp
    # Obsługa Range
    m = re.match(r'bytes=(\d+)-(\d*)', range_header)
    if not m:
        # obsługa sufiksowego zakresu: bytes=-N
        m_suffix = re.match(r'bytes=-(\d+)', range_header)
        if not m_suffix:
            # nierozpoznany format Range – fallback
            if method == 'HEAD':
                resp = Response(status=200)
                resp.headers['Content-Type'] = 'video/mp4'
                resp.headers['Content-Length'] = str(size)
                resp.headers['Accept-Ranges'] = 'bytes'
                resp.headers['Cache-Control'] = 'no-transform, public, max-age=0'
                resp.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
                resp.headers['X-Accel-Buffering'] = 'no'
                resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
                return resp
            resp = send_file(path, mimetype='video/mp4')
            resp.headers['Accept-Ranges'] = 'bytes'
            resp.headers['Cache-Control'] = 'no-transform, public, max-age=0'
            resp.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
            resp.headers['X-Accel-Buffering'] = 'no'
            resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
            return resp
        # przetwarzanie sufiksowego zakresu
        suffix_len = int(m_suffix.group(1))
        suffix_len = min(suffix_len, size)
        start = size - suffix_len
        end = size - 1
    else:
        start = int(m.group(1))
        end_s = m.group(2)
        end = int(end_s) if end_s else size - 1
    end = min(end, size - 1)
    if start > end or start >= size:
        # nieprawidłowy zakres: 416 Range Not Satisfiable
        resp = Response(status=416)
        resp.headers['Content-Range'] = f'bytes */{size}'
        resp.headers['Accept-Ranges'] = 'bytes'
        resp.headers['Cache-Control'] = 'no-store, no-transform, max-age=0'
        resp.headers['Content-Type'] = 'video/mp4'
        resp.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
        resp.headers['X-Accel-Buffering'] = 'no'
        resp.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return resp
    length = end - start + 1
    if method == 'HEAD':
        rv = Response(status=206)
        rv.headers['Content-Range'] = f'bytes {start}-{end}/{size}'
        rv.headers['Accept-Ranges'] = 'bytes'
        rv.headers['Content-Length'] = str(length)
        rv.headers['Cache-Control'] = 'no-transform, public, max-age=0'
        rv.headers['Content-Type'] = 'video/mp4'
        rv.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
        rv.headers['X-Accel-Buffering'] = 'no'
        rv.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return rv
    def generate():
        with open(path, 'rb') as f:
            f.seek(start)
            remaining = length
            chunk = 1024 * 1024
            while remaining > 0:
                data = f.read(min(chunk, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data
    rv = Response(generate(), 206, mimetype='video/mp4', direct_passthrough=True)
    rv.headers['Content-Range'] = f'bytes {start}-{end}/{size}'
    rv.headers['Accept-Ranges'] = 'bytes'
    rv.headers['Content-Length'] = str(length)
    rv.headers['Cache-Control'] = 'no-transform, public, max-age=0'
    rv.headers['Content-Disposition'] = 'inline; filename="video.mp4"'
    rv.headers['X-Accel-Buffering'] = 'no'
    rv.headers['X-Robots-Tag'] = 'noindex, nofollow'
    return rv

# NEW: serve rendered exports
@app.route('/media/exports/<path:name>')
def serve_export(name: str):
    path = get_data_path('media', 'exports', name)
    if not os.path.exists(path):
        return jsonify({'error': 'Not Found'}), 404
    return send_file(path)

# NEW: serve subtitles (SRT)
@app.route('/media/subtitles/<path:name>')
def serve_subtitles(name: str):
    path = get_data_path('media', 'subtitles', name)
    if not os.path.exists(path):
        return jsonify({'error': 'Not Found'}), 404
    return send_file(path)

# --- TRANSCRIPTION API (SRT) -------------------------------
_transcribe_threads = {}
_transcribe_lock = threading.Lock()


def _ensure_subtitles_dir():
    os.makedirs(get_data_path('media', 'subtitles'), exist_ok=True)
    os.makedirs(get_data_path('transcribe'), exist_ok=True)


def _status_path_for(clip_id: str) -> str:
    return get_data_path('transcribe', f'{clip_id}.json')


def _write_status(clip_id: str, state: str, **extra):
    payload = {'clip_id': clip_id, 'state': state}
    payload.update(extra or {})
    _safe_write_json(_status_path_for(clip_id), payload)


def _read_status(clip_id: str):
    return _safe_read_json(_status_path_for(clip_id), default=None)

# --- RENDER STATUS HELPERS ---------------------------------
def _render_status_path_for(clip_id: str) -> str:
    return get_data_path('render', f'{clip_id}.json')

def _write_render_status(clip_id: str, state: str, **extra):
    try:
        os.makedirs(get_data_path('render'), exist_ok=True)
    except Exception:
        pass
    payload = {'clip_id': clip_id, 'state': state}
    payload.update(extra or {})
    _safe_write_json(_render_status_path_for(clip_id), payload)

def _read_render_status(clip_id: str):
    return _safe_read_json(_render_status_path_for(clip_id), default=None)


def _transcribe_worker(clip_id: str, language: str, model: str, diarize: bool):
    try:
        _ensure_subtitles_dir()
        in_path = get_data_path('media', 'clips', f'{clip_id}.mp4')
        out_name = f'{clip_id}.srt'
        out_path = get_data_path('media', 'subtitles', out_name)
        url = f'/media/subtitles/{out_name}'
        if not os.path.exists(in_path):
            _write_status(clip_id, 'error', error=f'clip file not found: {clip_id}.mp4')
            return
        _write_status(clip_id, 'running')
        if _transcribe_srt is None:
            _write_status(clip_id, 'error', error='transcribe adapter not available')
            return
        hf_token = os.getenv('HF_TOKEN')
        pyexe = os.getenv('TRANSCRIBE_PYTHON')
        if not pyexe:
            # Default to Python 3.11 venv created at venv311
            candidate = os.path.join(PROJECT_ROOT, 'venv311', 'Scripts', 'python.exe')
            if os.path.isfile(candidate):
                pyexe = candidate
            else:
                pyexe = sys.executable
        timeout = None
        try:
            if os.getenv('TRANSCRIBE_TIMEOUT_SEC'):
                timeout = int(os.getenv('TRANSCRIBE_TIMEOUT_SEC'))
        except Exception:
            timeout = None

        # Preflight: sprawdź krytyczne moduły w docelowym interpreterze
        try:
            pre_code = (
                "import importlib,sys;\n"
                "mods=['numpy','moviepy','PIL']; errs=[]\n"
                "for m in mods:\n"
                "    try:\n"
                "        importlib.import_module(m)\n"
                "    except Exception as e:\n"
                "        errs.append(f'{m}: {e}')\n"
                "try:\n"
                "    importlib.import_module('moviepy.editor')\n"
                "except Exception as e:\n"
                "    errs.append(f'moviepy.editor: {e}')\n"
                "print('OK' if not errs else 'MISSING: ' + ' | '.join(errs))\n"
            )
            pre = subprocess.run([pyexe, '-c', pre_code], capture_output=True, text=True)
            pre_out = (pre.stdout or '').strip()
            pre_err = (pre.stderr or '').strip()
            if pre.returncode != 0 or (pre_out and pre_out.startswith('MISSING')):
                details = (pre_out + ('\n' + pre_err if pre_err else '')).strip()
                _write_status(clip_id, 'error', error=f'Python env check failed. {details}. exe={pyexe}')
                return
        except Exception:
            # preflight nie może blokować właściwego uruchomienia, jeśli sam się wysypie
            pass

        rc = _transcribe_srt(
            input_path=in_path,
            subtitle_path=out_path,
            language=language,
            model=model,
            diarize=diarize,
            python_exe=pyexe,
            hf_token=hf_token,
            cwd=None,
            timeout_sec=timeout,
        )
        if rc == 0 and os.path.exists(out_path):
            _write_status(clip_id, 'done', url=url)
        else:
            _write_status(clip_id, 'error', error=f'process failed with code {rc}. exe={pyexe}')
    except Exception as e:
        _write_status(clip_id, 'error', error=str(e))
    finally:
        with _transcribe_lock:
            _transcribe_threads.pop(clip_id, None)


@app.route('/api/transcribe', methods=['POST'])
def api_transcribe():
    data = request.get_json(silent=True) or {}
    clip_id = (data.get('clip_id') or '').strip()
    if not clip_id:
        return jsonify({'ok': False, 'error': 'clip_id required'}), 400
    in_path = get_data_path('media', 'clips', f'{clip_id}.mp4')
    if not os.path.exists(in_path):
        return jsonify({'ok': False, 'error': f'clip file not found: {clip_id}.mp4'}), 404

    language = (data.get('language') or os.getenv('TRANSCRIBE_LANGUAGE') or 'pl')
    model = (data.get('model') or os.getenv('TRANSCRIBE_MODEL') or 'medium')
    diarize = data.get('diarize')
    if diarize is None:
        diarize = (os.getenv('TRANSCRIBE_DIAR', 'true').lower() != 'false')

    _ensure_subtitles_dir()
    out_name = f'{clip_id}.srt'
    out_path = get_data_path('media', 'subtitles', out_name)
    url = f'/media/subtitles/{out_name}'

    if os.path.exists(out_path):
        _write_status(clip_id, 'done', url=url)
        return jsonify({'ok': True, 'state': 'done', 'url': url})

    with _transcribe_lock:
        if clip_id in _transcribe_threads and _transcribe_threads[clip_id].is_alive():
            return jsonify({'ok': True, 'state': 'running'})
        t = threading.Thread(target=_transcribe_worker, args=(clip_id, language, model, bool(diarize)), daemon=True)
        _transcribe_threads[clip_id] = t
        t.start()
    return jsonify({'ok': True, 'state': 'running'}), 202


@app.route('/api/transcribe/status')
def api_transcribe_status():
    clip_id = (request.args.get('clip_id') or '').strip()
    if not clip_id:
        return jsonify({'error': 'clip_id required'}), 400
    st = _read_status(clip_id)
    if st:
        return jsonify(st)
    out_name = f'{clip_id}.srt'
    out_path = get_data_path('media', 'subtitles', out_name)
    url = f'/media/subtitles/{out_name}'
    if os.path.exists(out_path):
        return jsonify({'clip_id': clip_id, 'state': 'done', 'url': url})
    with _transcribe_lock:
        if clip_id in _transcribe_threads and _transcribe_threads[clip_id].is_alive():
            return jsonify({'clip_id': clip_id, 'state': 'running'})
    return jsonify({'clip_id': clip_id, 'state': 'idle'})
@app.route('/api/crop', methods=['POST'])
def api_crop():
    data = request.get_json(silent=True) or {}
    clip_id = data.get('clip_id')
    rect = data.get('rect')  # expected dict with x,y,w,h in [0..1]
    kind = (data.get('kind') or 'game').lower()
    if kind not in ('game', 'camera'):
        return jsonify({'error': "invalid kind; expected 'game' or 'camera'"}), 400
    if not clip_id or not isinstance(rect, dict):
        return jsonify({'error': 'clip_id and rect required'}), 400
    crops_path = get_data_path('crops.json')
    crops = _safe_read_json(crops_path, default={}) or {}
    # sanitize numbers
    try:
        x = max(0.0, min(1.0, float(rect.get('x', 0))))
        y = max(0.0, min(1.0, float(rect.get('y', 0))))
        w = max(0.0, min(1.0, float(rect.get('w', 1))))
        h = max(0.0, min(1.0, float(rect.get('h', 1))))
    except Exception:
        return jsonify({'error': 'invalid rect values'}), 400

    # Backward compatibility: old schema stored a single rect directly under clip_id
    existing = crops.get(clip_id)
    if isinstance(existing, dict) and {'x', 'y', 'w', 'h'}.issubset(existing.keys()):
        merged = {'game': existing, 'camera': None}
    elif isinstance(existing, dict):
        # new schema or unexpected
        merged = {'game': existing.get('game'), 'camera': existing.get('camera')}
    else:
        merged = {'game': None, 'camera': None}

    merged[kind] = {'x': x, 'y': y, 'w': w, 'h': h}
    crops[clip_id] = merged
    _safe_write_json(crops_path, crops)
    return jsonify({'ok': True, 'clip_id': clip_id, 'game': merged['game'], 'camera': merged['camera']})


# --- NEW: render endpoint (1080x1920 vertical, 30fps, H.264 CRF 18, AAC 192k)
@app.route('/api/render', methods=['POST'])
def api_render():
    data = request.get_json(silent=True) or {}
    clip_id = (data.get('clip_id') or '').strip()
    game = data.get('game') or {}
    camera = data.get('camera') or {}
    # Bezpieczne parsowanie booleanów z różnych typów (bool/int/str)
    def _parse_bool(val, default=False):
        try:
            if val is None:
                return default
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                return (val != 0)
            s = str(val).strip().lower()
            if s in ('1', 'true', 'yes', 'y', 'on', 'enable', 'enabled'): return True
            if s in ('0', 'false', 'no', 'n', 'off', 'disable', 'disabled'): return False
        except Exception:
            pass
        return default

    karaoke_debug = _parse_bool(data.get('karaoke_debug', False), False)
    # NEW: allow disabling subtitles (karaoke/SRT) from the client
    include_subtitles = _parse_bool(data.get('include_subtitles', True), True)
    karaoke_debug_info = {}
    try:
        g_ratio = float(data.get('game_ratio', 0.7))
    except Exception:
        g_ratio = 0.7
    g_ratio = max(0.0, min(1.0, g_ratio))
    auto_split = _parse_bool(data.get('auto_split', False), False)

    # Nowy parametr: sposób dopasowania do paneli (contain vs cover)
    fit_mode = str(data.get('fit_mode', 'contain')).lower()
    if fit_mode not in ('contain', 'cover'):
        fit_mode = 'contain'

    # Tryb jednego kadru
    single_frame = _parse_bool(data.get('single_frame', False), False)
    try:
        single_height_ratio = float(data.get('single_height_ratio', 0.4))
    except Exception:
        single_height_ratio = 0.4
    single_height_ratio = max(0.05, min(0.95, single_height_ratio))
    panel_h = max(2, int(round(1920 * single_height_ratio)))
    if panel_h % 2:
        panel_h += 1

    if not clip_id:
        return jsonify({'ok': False, 'error': 'clip_id required'}), 400

    in_path = get_data_path('media', 'clips', f'{clip_id}.mp4')
    if not os.path.exists(in_path):
        return jsonify({'ok': False, 'error': f'clip file not found: {clip_id}.mp4'}), 404

    def _num(d, k, default=0.0):
        try:
            return float(d.get(k, default))
        except Exception:
            return float(default)

    gx, gy, gw, gh = _num(game, 'x'), _num(game, 'y'), _num(game, 'w', 1.0), _num(game, 'h', 1.0)
    cx, cy, cw, ch = _num(camera, 'x'), _num(camera, 'y'), _num(camera, 'w', 1.0), _num(camera, 'h', 1.0)

    # clamp to [0,1]
    gx, gy, gw, gh = [max(0.0, min(1.0, v)) for v in (gx, gy, gw, gh)]
    cx, cy, cw, ch = [max(0.0, min(1.0, v)) for v in (cx, cy, cw, ch)]
    # dodatkowe klamrowanie względem przesunięcia, aby crop mieścił się w [0,1]
    gw = max(1e-6, min(gw, 1.0 - gx))
    gh = max(1e-6, min(gh, 1.0 - gy))
    cw = max(1e-6, min(cw, 1.0 - cx))
    ch = max(1e-6, min(ch, 1.0 - cy))

    # output target
    out_w, out_h = 1080, 1920
    # Oblicz wysokości sekcji: tryb ręczny (suwak) lub Auto (z aspektów zaznaczeń)
    if auto_split and cw > 0 and ch > 0 and gw > 0 and gh > 0:
        # a = w/h (współczynnik kadru). Wysokość sekcji ~ 1/a (im węższy kadr, tym większa wysokość potrzebna przy contain)
        a_cam = cw / ch
        a_game = gw / gh
        a_cam = 1e-6 if a_cam <= 0 else a_cam
        a_game = 1e-6 if a_game <= 0 else a_game
        p_cam = 1.0 / a_cam
        p_game = 1.0 / a_game
        s = p_cam + p_game
        top_h = max(2, int(round(out_h * (p_cam / s))))  # camera
        bot_h = max(2, out_h - top_h)  # game
    else:
        top_h = max(2, int(round(out_h * (1.0 - g_ratio))))  # camera
        bot_h = max(2, out_h - top_h)  # game
    # wymuś parzystość, aby uniknąć artefaktów skalowania/cropu
    if top_h % 2: top_h += 1
    if bot_h % 2: bot_h -= 1
    if bot_h <= 0: bot_h = 2
    if top_h + bot_h != out_h:
        # dopasuj przez korektę do sumy out_h
        bot_h = out_h - top_h

    # locate ffmpeg
    ffmpeg = shutil.which('ffmpeg') or shutil.which('ffmpeg.exe')
    if not ffmpeg:
        try:
            import imageio_ffmpeg  # type: ignore
            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            return jsonify({'ok': False, 'error': 'ffmpeg not found in PATH (or imageio-ffmpeg not installed)'}), 500

    # Zapisz status rozpoczęcia renderu z podstawowymi parametrami
    try:
        _write_render_status(
            clip_id,
            'running',
            params={
                'game': game,
                'camera': camera,
                'game_ratio': g_ratio,
                'auto_split': auto_split,
                'single_frame': single_frame,
                'single_height_ratio': single_height_ratio,
                'fit_mode': fit_mode,
                'start': ss,
                'end': to,
            }
        )
    except Exception:
        pass

    # build filter: crop normalized -> scale (cover) -> center crop -> vstack lub single-frame overlay
    # use limited precision to keep command readable
    fmt = lambda v: f'{v:.6f}'.rstrip('0').rstrip('.') if isinstance(v, float) else str(v)
    crop_game = f"crop=floor(iw*{fmt(gw)}):floor(ih*{fmt(gh)}):floor(iw*{fmt(gx)}):floor(ih*{fmt(gy)})"
    crop_cam  = f"crop=floor(iw*{fmt(cw)}):floor(ih*{fmt(ch)}):floor(iw*{fmt(cx)}):floor(ih*{fmt(cy)})"

    if single_frame:
        # Tło: ten sam klip (gameRect) pokrywa cały kadr, następnie mocne rozmycie i lekkie przyciemnienie
        scale_bg    = f"scale=w='if(gt(a,{out_w}/{out_h}),-2,{out_w})':h='if(gt(a,{out_w}/{out_h}),{out_h},-2)'"
        center_bg   = f"crop={out_w}:{out_h}:floor((iw-{out_w})/2):floor((ih-{out_h})/2)"
        # Panel: pełna szerokość, wysokość = panel_h, cover + center crop
        scale_panel = f"scale=w='if(gt(a,{out_w}/{panel_h}),-2,{out_w})':h='if(gt(a,{out_w}/{panel_h}),{panel_h},-2)'"
        center_panel= f"crop={out_w}:{panel_h}:floor((iw-{out_w})/2):floor((ih-{panel_h})/2)"
        filter_complex = (
            f"[0:v]{crop_game},{scale_bg},{center_bg},gblur=sigma=16,eq=brightness=-0.08[bg];"
            f"[0:v]{crop_game},{scale_panel},{center_panel}[panel];"
            f"[bg][panel]overlay=x=0:y=(H-h)/2[outv]"
        )
    elif fit_mode == 'contain':
        # Dopasowanie bez crop: skaluj w granicach panelu, zachowując proporcje i parzyste wymiary, potem wyrównaj padami
        scale_cam  = f"scale={out_w}:{top_h}:force_original_aspect_ratio=decrease:force_divisible_by=2"
        scale_game = f"scale={out_w}:{bot_h}:force_original_aspect_ratio=decrease:force_divisible_by=2"
        pad_cam    = f"pad={out_w}:{top_h}:(ow-iw)/2:(oh-ih)/2"
        pad_game   = f"pad={out_w}:{bot_h}:(ow-iw)/2:(oh-ih)/2"
        filter_complex = (
            f"[0:v]{crop_cam},{scale_cam},{pad_cam}[cam];"
            f"[0:v]{crop_game},{scale_game},{pad_game}[game];"
            f"[cam][game]vstack=inputs=2[outv]"
        )
    else:
        # Pokrycie panelu (cover strict): skaluj tak, aby panel był w pełni wypełniony, następnie przytnij do docelowych wymiarów; bez padów i bez soften
        scale_cam  = f"scale=w='if(gt(a,{out_w}/{top_h}),-2,{out_w})':h='if(gt(a,{out_w}/{top_h}),{top_h},-2)'"
        scale_game = f"scale=w='if(gt(a,{out_w}/{bot_h}),-2,{out_w})':h='if(gt(a,{out_w}/{bot_h}),{bot_h},-2)'"
        center_cam   = f"crop={out_w}:{top_h}:floor((iw-{out_w})/2):floor((ih-{top_h})/2)"
        center_game  = f"crop={out_w}:{bot_h}:floor((iw-{out_w})/2):floor((ih-{bot_h})/2)"
        filter_complex = (
            f"[0:v]{crop_cam},{scale_cam},{center_cam}[cam];"
            f"[0:v]{crop_game},{scale_game},{center_game}[game];"
            f"[cam][game]vstack=inputs=2[outv]"
        )

    # Ścieżki napisów; nie wypalamy SRT w pierwszym przebiegu – najpierw spróbujemy karaoke, a SRT będzie tylko fallbackiem
    srt_path = get_data_path('media', 'subtitles', f'{clip_id}.srt')
    json_word_path = get_data_path('media', 'subtitles', f'{clip_id}.json')
    out_label = 'outv'
    ffmpeg_srt_burned = False

    # trim range (accurate: place after -i)
    start = data.get('start', None)
    end = data.get('end', None)
    ss = None; to = None
    try:
        if start is not None:
            s = float(start)
            if s >= 0:
                ss = s
    except Exception:
        ss = None
    try:
        if end is not None:
            e = float(end)
            if e > 0:
                to = e
    except Exception:
        to = None

    os.makedirs(get_data_path('media', 'exports'), exist_ok=True)
    out_name = f"{clip_id}_1080x1920.mp4"
    out_path = get_data_path('media', 'exports', out_name)

    cmd = [
        ffmpeg, '-y', '-i', in_path,
    ]
    if ss is not None:
        cmd += ['-ss', f'{ss}']
    if to is not None:
        cmd += ['-to', f'{to}']
    cmd += [
        '-filter_complex', filter_complex,
        '-map', f'[{out_label}]', '-map', '0:a:0?',
        '-r', '30',
        '-c:v', 'libx264', '-preset', 'medium', '-crf', '18',
        '-pix_fmt', 'yuv420p',
        '-c:a', 'aac', '-b:a', '192k',
        '-shortest',
        '-movflags', '+faststart',
        out_path
    ]

    # Szczegółowe logowanie komendy ffmpeg
    try:
        print(f"[RENDER] ffmpeg exe: {ffmpeg}")
        print(f"[RENDER] cmd: {' '.join(cmd)}")
    except Exception:
        pass

    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            err_text = (res.stderr or '')[-2000:] or (res.stdout or '')[-2000:]
            # Usuń nieudany plik wyjściowy, aby można było ponowić
            try:
                if os.path.exists(out_path):
                    os.remove(out_path)
            except Exception:
                pass
            try:
                _write_render_status(
                    clip_id,
                    'error',
                    error=err_text,
                    cmd=' '.join(cmd),
                    ffmpeg=ffmpeg,
                    ffmpeg_stderr=(res.stderr or '')[-2000:],
                    ffmpeg_stdout=(res.stdout or '')[-2000:]
                )
            except Exception:
                pass
            return jsonify({
                'ok': False,
                'error': err_text,
                'stderr': (res.stderr or '')[-2000:],
                'stdout': (res.stdout or '')[-2000:],
                'cmd': ' '.join(cmd),
                'ffmpeg': ffmpeg,
            }), 500
    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        try:
            _write_render_status(
                clip_id,
                'error',
                error=str(e),
                cmd=' '.join(cmd),
                ffmpeg=ffmpeg
            )
        except Exception:
            pass
        return jsonify({'ok': False, 'error': str(e), 'cmd': ' '.join(cmd), 'ffmpeg': ffmpeg}), 500

    # --- Karaoke overlay (MoviePy) automatycznie, jeśli mamy word-level JSON ---
    karaoke_status = 'skipped'
    def _apply_karaoke_if_available():
        nonlocal karaoke_status
        try:
            json_path = get_data_path('media', 'subtitles', f'{clip_id}.json')
            if not os.path.exists(json_path):
                # Spróbuj wygenerować JSON (word-level) automatycznie – niezależnie od tego, czy SRT już istnieje
                try:
                    srt_path = get_data_path('media', 'subtitles', f'{clip_id}.srt')
                    in_mp4 = get_data_path('media', 'clips', f'{clip_id}.mp4')
                    if os.path.exists(in_mp4) and (_transcribe_srt is not None):
                        # konfiguracja jak w _transcribe_worker
                        language = (os.getenv('TRANSCRIBE_LANGUAGE') or 'pl')
                        model = (os.getenv('TRANSCRIBE_MODEL') or 'medium')
                        diarize = (os.getenv('TRANSCRIBE_DIAR', 'true').lower() != 'false')
                        pyexe = os.getenv('TRANSCRIBE_PYTHON')
                        if not pyexe:
                            candidate = os.path.join(PROJECT_ROOT, 'venv311', 'Scripts', 'python.exe')
                            pyexe = candidate if os.path.isfile(candidate) else sys.executable
                        hf_token = os.getenv('HF_TOKEN')
                        rc = None
                        try:
                            rc = _transcribe_srt(
                                input_path=in_mp4,
                                subtitle_path=srt_path,
                                language=language,
                                model=model,
                                diarize=diarize,
                                python_exe=pyexe,
                                hf_token=hf_token,
                                cwd=None,
                                timeout_sec=None,
                            )
                        except Exception:
                            rc = -1
                        # jeśli nie udało się (błąd/nie-0) spróbuj ponownie bez diarization (bardziej niezawodne)
                        if (rc is None) or (rc != 0):
                            try:
                                _ = _transcribe_srt(
                                    input_path=in_mp4,
                                    subtitle_path=srt_path,
                                    language=language,
                                    model=model,
                                    diarize=False,
                                    python_exe=pyexe,
                                    hf_token=hf_token,
                                    cwd=None,
                                    timeout_sec=None,
                                )
                            except Exception:
                                pass
                    # po próbie generacji sprawdź ponownie
                    if not os.path.exists(json_path):
                        karaoke_status = 'skipped: json not found'
                        return
                except Exception as e:
                    karaoke_status = f'skipped: json not found and generate failed: {e}'
                    karaoke_debug_info['generate_exception'] = str(e)
                    return

            # W tym miejscu wykonujemy overlay karaoke w osobnym procesie (venv311)
            try:
                pyexe = os.getenv('TRANSCRIBE_PYTHON')
                if not pyexe:
                    candidate = os.path.join(PROJECT_ROOT, 'venv311', 'Scripts', 'python.exe')
                    pyexe = candidate if os.path.isfile(candidate) else sys.executable
                args = [
                    pyexe, '-m', 'pipeline.transcribe.script',
                    '--apply-karaoke',
                    '--input', out_path,
                    '--json', json_path,
                    '--offset', str(float(ss) if ss is not None else 0.0),
                    '--height', '1920',
                    '--fps', '30'
                ]
                font_path = os.getenv('KARAOKE_FONT_PATH')
                if font_path:
                    args += ['--font', font_path]
                karaoke_debug_info['karaoke_cmd'] = ' '.join(args)
                res = subprocess.run(args, capture_output=True, text=True, cwd=PROJECT_ROOT)
                karaoke_debug_info['karaoke_rc'] = int(res.returncode)
                if res.stdout:
                    karaoke_debug_info['karaoke_stdout'] = res.stdout[-2000:]
                if res.stderr:
                    karaoke_debug_info['karaoke_stderr'] = res.stderr[-2000:]
                if res.returncode == 0:
                    karaoke_status = 'applied'
                    return
                else:
                    karaoke_status = f'skipped: karaoke cli failed rc={res.returncode}'
                    return
            except Exception as e:
                karaoke_status = f'skipped: karaoke cli exception: {e}'
                karaoke_debug_info['karaoke_cli_exception'] = str(e)
                return

            try:
                from moviepy.editor import VideoFileClip
                from moviepy.video.compositing.CompositeVideoClip import CompositeVideoClip
                karaoke_debug_info['moviepy_import_ok'] = True
                # dodatkowa diagnostyka środowiska
                try:
                    import moviepy, sys as _sys
                    karaoke_debug_info['moviepy_version'] = getattr(moviepy, '__version__', 'unknown')
                    karaoke_debug_info['moviepy_file'] = getattr(moviepy, '__file__', 'unknown')
                    karaoke_debug_info['python_executable'] = getattr(_sys, 'executable', 'unknown')
                except Exception:
                    pass
            except Exception as e:
                karaoke_status = f'skipped: moviepy import failed: {e}'
                karaoke_debug_info['moviepy_import_error'] = str(e)
                try:
                    import sys as _sys
                    karaoke_debug_info['python_executable'] = getattr(_sys, 'executable', 'unknown')
                except Exception:
                    pass
                return
            try:
                # Importujemy funkcje z oryginalnego skryptu karaoke
                from pipeline.transcribe.script import make_karaoke_clip, get_speaker_color, load_badwords
                karaoke_debug_info['karaoke_helpers_import_ok'] = True
            except Exception as e:
                karaoke_status = f'skipped: cannot import karaoke helpers: {e}'
                karaoke_debug_info['karaoke_helpers_import_error'] = str(e)
                return
            # Wczytaj dane
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data_json = json.load(f)
                karaoke_debug_info['json_read_ok'] = True
            except Exception as e:
                karaoke_status = f'skipped: json read failed: {e}'
                karaoke_debug_info['json_read_error'] = str(e)
                return

            segments = data_json.get('segments') or []
            karaoke_debug_info['segments_count'] = len(segments)
            if not segments:
                karaoke_status = 'skipped: no segments in json'
                return

            # otwórz wyrenderowane wideo jako bazę
            base = VideoFileClip(out_path)
            duration = float(getattr(base, 'duration', 0.0) or 0.0)
            karaoke_debug_info['base_duration'] = duration
            if duration <= 0:
                karaoke_status = 'skipped: invalid base duration'
                base.close()
                return

            # offset względem -ss
            offset = float(ss) if ss is not None else 0.0
            end_limit = float(to - ss) if (to is not None and ss is not None) else (float(to) if to is not None else None)
            karaoke_debug_info['offset'] = offset
            karaoke_debug_info['end_limit'] = end_limit

            # przygotuj listę słów w oknie [0, duration]
            def _iter_words():
                for seg in segments:
                    ws = seg.get('words') or []
                    for w in ws:
                        try:
                            s = float(w.get('start', 0.0)) - offset
                            e = float(w.get('end', 0.0)) - offset
                        except Exception:
                            continue
                        if end_limit is not None and s >= end_limit:
                            continue
                        if e <= 0:
                            continue
                        s2 = max(0.0, s)
                        e2 = min(duration, e)
                        if e2 - s2 <= 0:
                            continue
                        ww = dict(w)
                        ww['start'] = s2
                        ww['end'] = e2
                        yield ww

            words_window = list(_iter_words())
            karaoke_debug_info['words_window_count'] = len(words_window)
            if not words_window:
                karaoke_status = 'skipped: no words in time window'
                base.close()
                return

            def split_on_punct(words_list):
                subs, cur = [], []
                for w in words_list:
                    cur.append(w)
                    if str(w.get('word', '')).strip().endswith(('.', ',', '?', '!')):
                        subs.append(cur)
                        cur = []
                if cur:
                    subs.append(cur)
                return subs

            bw_path = os.path.join(PROJECT_ROOT, 'pipeline', 'transcribe', 'badwords.json')
            try:
                badwords = load_badwords(bw_path) if os.path.exists(bw_path) else set()
            except Exception:
                badwords = set()

            subtitle_clips = []
            clip_errors = []
            subs_list = split_on_punct(words_window)
            karaoke_debug_info['subs_count'] = len(subs_list)
            for sub in subs_list:
                if not sub:
                    continue
                seg_start = sub[0]['start']
                seg_end   = sub[-1]['end']
                speakers = [w.get('speaker', 'SPEAKER_00') for w in sub]
                main_speaker = max(set(speakers), key=speakers.count) if speakers else 'SPEAKER_00'
                color = get_speaker_color(main_speaker)
                try:
                    clip = make_karaoke_clip(
                        sub, seg_start, seg_end, color,
                        badwords=badwords, font_path=None, fontsize=65, height=1920, wrap_width=25
                    )
                    subtitle_clips.append(clip)
                except Exception as e:
                    clip_errors.append(str(e))
                    # jeśli jeden sub się nie uda, pomiń i kontynuuj
                    continue
            karaoke_debug_info['subtitle_clips_count'] = len(subtitle_clips)
            if clip_errors:
                karaoke_debug_info['clip_errors'] = clip_errors

            if not subtitle_clips:
                karaoke_status = 'skipped: no subtitle clips built'
                base.close()
                return

            final_video = CompositeVideoClip([base, *subtitle_clips])
            tmp_out = out_path + '.karaoke.tmp.mp4'
            try:
                final_video.write_videofile(
                    tmp_out,
                    fps=30,
                    codec='libx264',
                    audio_codec='aac',
                    audio_bitrate='192k',
                    preset='medium',
                    ffmpeg_params=['-crf', '18', '-movflags', '+faststart']
                )
            except Exception as e:
                karaoke_debug_info['write_error'] = str(e)
                raise
            finally:
                try:
                    final_video.close()
                except Exception:
                    pass
                try:
                    base.close()
                except Exception:
                    pass

            # podmień wynik
            try:
                os.replace(tmp_out, out_path)
            except Exception as e:
                karaoke_status = f'skipped: replace failed: {e}'
                try:
                    if os.path.exists(tmp_out):
                        os.remove(tmp_out)
                except Exception:
                    pass
                return

            karaoke_status = 'applied'

        except Exception as e:
            karaoke_status = f'exception: {e}'
            try:
                karaoke_debug_info['unexpected_exception'] = str(e)
            except Exception:
                pass

    if include_subtitles:
        _apply_karaoke_if_available()
    else:
        karaoke_status = 'disabled'

    # --- Jeżeli karaoke nie zostało zastosowane, spróbuj wypalić SRT jako fallback ---
    if include_subtitles and (not ffmpeg_srt_burned) and (karaoke_status != 'applied') and os.path.exists(srt_path):
        try:
            # Jeśli zastosowano przycięcie (-ss), przesuń czasy w SRT o -ss, aby dopasować do wyrenderowanego klipu
            srt_use_path = srt_path
            if ss is not None:
                try:
                    import re
                    shift_ms = int(float(ss) * 1000)
                    tmp_shift_path = srt_path + f".shift_{shift_ms}.srt"
                    with open(srt_path, 'r', encoding='utf-8', errors='ignore') as fin, open(tmp_shift_path, 'w', encoding='utf-8') as fout:
                        for line in fin:
                            if ' --> ' in line:
                                try:
                                    left, right = line.strip().split(' --> ')
                                    def _to_ms(t):
                                        hh, mm, ssms = t.split(':')
                                        ss2, ms2 = ssms.split(',')
                                        return (int(hh)*3600 + int(mm)*60 + int(ss2))*1000 + int(ms2)
                                    def _fmt_ms(ms):
                                        if ms < 0: ms = 0
                                        hh = ms // 3600000; ms %= 3600000
                                        mm = ms // 60000; ms %= 60000
                                        ss3 = ms // 1000; ms %= 1000
                                        return f"{hh:02d}:{mm:02d}:{ss3:02d},{ms:03d}"
                                    lms = _to_ms(left) - shift_ms
                                    rms = _to_ms(right) - shift_ms
                                    fout.write(f"{_fmt_ms(lms)} --> {_fmt_ms(rms)}\n")
                                except Exception:
                                    fout.write(line)
                            else:
                                fout.write(line)
                    srt_use_path = tmp_shift_path
                except Exception:
                    srt_use_path = srt_path

            # Użyj pliku (ew. przesuniętego) do wypalania
            srt_norm2 = (srt_use_path or srt_path).replace('\\', '/')
            srt_escaped2 = srt_norm2.replace(':', '\\:').replace("'", "\\'")
            ass_style2 = "FontName=Montserrat,FontSize=42,Outline=3,Shadow=1,PrimaryColour=&H00FFFFFF&,BackColour=&H80000000&,BorderStyle=3,Alignment=2,MarginV=240"
            tmp_out2 = out_path + '.srt.tmp.mp4'
            cmd2 = [
                ffmpeg, '-y', '-i', out_path,
                '-filter_complex', f"[0:v]subtitles=filename='{srt_escaped2}':charenc=UTF-8:force_style='{ass_style2}'[v]",
                '-map', '[v]', '-map', '0:a:0?',
                '-r', '30',
                '-c:v', 'libx264', '-preset', 'medium', '-crf', '18',
                '-c:a', 'aac', '-b:a', '192k',
                '-movflags', '+faststart',
                tmp_out2
            ]
            res2 = subprocess.run(cmd2, capture_output=True, text=True)
            if res2.returncode == 0:
                try:
                    # Spróbuj podmienić wynik na główny plik. Na Windows zdarza się lock (WinError 5).
                    # Dodaj niewielkie ponawianie.
                    replaced = False
                    for _ in range(5):
                        try:
                            os.replace(tmp_out2, out_path)
                            replaced = True
                            break
                        except Exception:
                            try:
                                import time as _t
                                _t.sleep(0.3)
                            except Exception:
                                pass
                    if not replaced:
                        # Użyj alternatywnej nazwy, aby nie tracić wyniku napisów
                        alt_name = f"{clip_id}_1080x1920.subs.mp4"
                        alt_path = get_data_path('media', 'exports', alt_name)
                        try:
                            os.replace(tmp_out2, alt_path)
                            # zaktualizuj docelowy URL, aby wskazywał na plik z napisami
                            prev_status = karaoke_status
                            karaoke_status = f"fallback_srt_applied_alt (prev: {prev_status})"
                            url = f"/media/exports/{alt_name}"
                            # posprzątaj tymczasowy SRT
                            try:
                                if srt_use_path and srt_use_path != srt_path and os.path.exists(srt_use_path):
                                    os.remove(srt_use_path)
                            except Exception:
                                pass
                            # Zapisz status i wróć
                            try:
                                _write_render_status(clip_id, 'done', url=url, karaoke=karaoke_status)
                            except Exception:
                                pass
                            return jsonify({'ok': True, 'url': url, 'karaoke': karaoke_status, 'karaoke_debug': (karaoke_debug_info if karaoke_debug else None)})
                        except Exception as e2:
                            # ostatnia deska: nie udało się także alternatywne os.replace
                            raise e2
                    prev_status = karaoke_status
                    karaoke_status = f"fallback_srt_applied (prev: {prev_status})"
                    # posprzątaj tymczasowy SRT
                    try:
                        if srt_use_path and srt_use_path != srt_path and os.path.exists(srt_use_path):
                            os.remove(srt_use_path)
                    except Exception:
                        pass
                except Exception as e:
                    karaoke_status = f'fallback_srt_replace_failed: {e}'
                    try:
                        if os.path.exists(tmp_out2):
                            os.remove(tmp_out2)
                    except Exception:
                        pass
            else:
                karaoke_status = f'fallback_srt_failed: {res2.stderr or res2.stdout or "ffmpeg failed"}'
                # również spróbuj usunąć tymczasowy SRT w przypadku błędu
                try:
                    if srt_use_path and srt_use_path != srt_path and os.path.exists(srt_use_path):
                        os.remove(srt_use_path)
                except Exception:
                    pass
        except Exception as e:
            karaoke_status = f'fallback_srt_exception: {e}'

    url = f"/media/exports/{out_name}"
    try:
        _write_render_status(
            clip_id,
            'done',
            url=url,
            karaoke=karaoke_status,
            karaoke_debug=(karaoke_debug_info if karaoke_debug else None),
            ffmpeg_cmd=' '.join(cmd),
            params={
                'game': game,
                'camera': camera,
                'game_ratio': g_ratio,
                'auto_split': auto_split,
                'single_frame': single_frame,
                'single_height_ratio': single_height_ratio,
                'fit_mode': fit_mode,
                'start': ss,
                'end': to,
                'include_subtitles': include_subtitles
            }
        )
    except Exception:
        pass
    return jsonify({'ok': True, 'url': url, 'karaoke': karaoke_status, 'karaoke_debug': (karaoke_debug_info if karaoke_debug else None)})

@app.route('/api/render/status')
def api_render_status():
    clip_id = (request.args.get('clip_id') or '').strip()
    if not clip_id:
        return jsonify({'error': 'clip_id required'}), 400
    out_name = f"{clip_id}_1080x1920.mp4"
    out_path = get_data_path('media', 'exports', out_name)
    url = f"/media/exports/{out_name}"
    st = _read_render_status(clip_id)
    # Tryb verbose pozwala zwrócić dodatkowe pola diagnostyczne
    def _parse_bool(val, default=False):
        try:
            if val is None:
                return default
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                return (val != 0)
            s = str(val).strip().lower()
            if s in ('1', 'true', 'yes', 'y', 'on', 'enable', 'enabled'): return True
            if s in ('0', 'false', 'no', 'n', 'off', 'disable', 'disabled'): return False
        except Exception:
            pass
        return default
    verbose = _parse_bool(request.args.get('verbose'), False)
    if os.path.exists(out_path):
        resp = {'clip_id': clip_id, 'state': 'done', 'url': url}
        try:
            if isinstance(st, dict) and 'karaoke' in st:
                resp['karaoke'] = st.get('karaoke')
            if verbose and isinstance(st, dict):
                # Dołącz ograniczoną diagnostykę z pliku statusu
                for key in ('params','ffmpeg_cmd','ffmpeg','ffmpeg_stderr','ffmpeg_stdout','error','karaoke_debug'):
                    if key in st:
                        resp[key] = st.get(key)
        except Exception:
            pass
        return jsonify(resp)
    if st:
        if verbose and isinstance(st, dict):
            return jsonify(st)
        # Bez verbose: zwróć tylko podstawowe pola
        base = {k: st.get(k) for k in ('clip_id','state','error','url','karaoke') if isinstance(st, dict)} if isinstance(st, dict) else st
        return jsonify(base)
    return jsonify({'clip_id': clip_id, 'state': 'idle'})

@app.route('/api/crop/<clip_id>')
def api_get_crop(clip_id: str):
    crops = _safe_read_json(get_data_path('crops.json'), default={}) or {}
    existing = crops.get(clip_id)
    if isinstance(existing, dict) and {'x', 'y', 'w', 'h'}.issubset((existing.keys())):
        # legacy -> map to game by default
        game = existing
        camera = None
    elif isinstance(existing, dict):
        game = existing.get('game')
        camera = existing.get('camera')
    else:
        game = None
        camera = None
    return jsonify({'clip_id': clip_id, 'game': game, 'camera': camera})

# --- PUBLISH VIA PUBLER ----------------------------------

def _exports_dir():
    return get_data_path('media', 'exports')


def _export_filename_for(clip_id: str) -> str:
    # zgodnie z api_render: "{clip_id}_1080x1920.mp4"
    return f"{clip_id}_1080x1920.mp4"


def _public_base_url() -> str | None:
    base = os.getenv('PUBLIC_BASE_URL')
    if base:
        return base.rstrip('/')
    return None


def _build_public_export_url(filename: str) -> str | None:
    base = _public_base_url()
    if not base:
        return None
    return f"{base}/media/exports/{filename}"


def _publer_headers(api_key: str, workspace_id: str) -> dict:
    return {
        'Authorization': f'Bearer-API {api_key}',
        'Publer-Workspace-Id': workspace_id,
    }


def _publer_fetch_thumbnails(api_key: str, workspace_id: str, media_id: str, max_wait_seconds: int = 90) -> list | None:
    """Polluje API Publera o metadane media (w tym thumbnails) aż będą dostępne lub upłynie timeout.
    Zwraca listę miniaturek (thumbnails) lub None, jeśli nie udało się pobrać.
    Domyślny czas oczekiwania to 90 sekund, aby dać Publerowi czas na wygenerowanie miniaturek.
    """
    headers = _publer_headers(api_key, workspace_id)
    deadline = time.time() + max_wait_seconds
    last_exc = None
    attempts = 0
    
    print(f"[DEBUG] Rozpoczynam pobieranie miniaturek dla media_id={media_id}, max_wait={max_wait_seconds}s")
    
    while time.time() < deadline:
        attempts += 1
        try:
            # Preferowany endpoint wg API: GET /api/v1/media z filtrem ids[]
            resp = requests.get(
                'https://app.publer.com/api/v1/media',
                headers=headers,
                params={
                    'ids[]': media_id
                },
                timeout=15
            )
            try:
                j = resp.json()
            except Exception:
                j = {'text': resp.text}
                
            # Logowanie odpowiedzi dla debugowania
            print(f"[DEBUG] Próba {attempts}: Odpowiedź API Publer (status={resp.status_code})")
            
            # Różne kształty: { media: [...] } albo { data: { media: [...] } }
            items = None
            if isinstance(j, dict):
                if isinstance(j.get('media'), list):
                    items = j.get('media')
                elif isinstance(j.get('data'), dict) and isinstance(j['data'].get('media'), list):
                    items = j['data']['media']
                    
            if items and len(items) > 0 and isinstance(items[0], dict):
                thumbs = items[0].get('thumbnails')
                if isinstance(thumbs, list) and len(thumbs) > 0:
                    print(f"[DEBUG] Znaleziono {len(thumbs)} miniaturek po {attempts} próbach")
                    return thumbs
                else:
                    print(f"[DEBUG] Brak miniaturek w odpowiedzi (próba {attempts})")
            
            if not resp.ok:
                # Przy 4xx/5xx nie ma sensu natychmiast spamować
                print(f"[DEBUG] Błąd API: {resp.status_code}, czekam 2s")
                time.sleep(2)
            else:
                # OK, ale jeszcze brak miniaturek – Publer może je generować asynchronicznie
                wait_time = min(3, max(1, (max_wait_seconds - (time.time() - (deadline - max_wait_seconds))) / 20))
                print(f"[DEBUG] Brak miniaturek, czekam {wait_time:.1f}s (pozostało {deadline - time.time():.1f}s)")
                time.sleep(wait_time)
        except Exception as e:
            last_exc = e
            print(f"[DEBUG] Wyjątek podczas pobierania miniaturek: {str(e)}")
            time.sleep(2)
    
    # timeout
    print(f"[DEBUG] Timeout po {attempts} próbach, nie udało się pobrać miniaturek")
    return None


def _write_publish_log_publer(clip_id: str, payload: dict, response: dict | None, status: str):
    os.makedirs(get_data_path('publish_logs'), exist_ok=True)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    fname = get_data_path('publish_logs', f"publer_{clip_id}_{ts}.json")
    entry = {
        'clip_id': clip_id,
        'timestamp': ts,
        'status': status,
        'payload': payload,
        'response': response,
    }
    try:
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(entry, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


@app.route('/publish/<clip_id>', methods=['POST'])
def api_publish_publer(clip_id: str):
    app.logger.info(f"DEBUG: Starting publish function for {clip_id}")
    # Konfiguracja z ENV lub body
    data = request.get_json(silent=True) or {}
    api_key = (data.get('publer_api_key') or os.getenv('PUBLER_API_KEY') or '').strip()
    workspace_id = (data.get('publer_workspace_id') or os.getenv('PUBLER_WORKSPACE_ID') or '').strip()
    # accounts: allow multiple via 'publer_account_ids' or 'account_ids' array; fallback to env defaults or single
    account_ids = data.get('publer_account_ids') or data.get('account_ids')
    if isinstance(account_ids, str):
        account_ids = [s.strip() for s in account_ids.split(',') if s.strip()]
    # try env multi first
    if not account_ids:
        env_multi = os.getenv('PUBLER_ACCOUNT_IDS')
        if env_multi:
            account_ids = [s.strip() for s in env_multi.split(',') if s.strip()]
    # fallback to single, but allow comma-separated in single var too
    if not account_ids:
        single_account = (data.get('publer_account_id') or os.getenv('PUBLER_ACCOUNT_ID') or '').strip()
        if single_account:
            if ',' in single_account:
                account_ids = [s.strip() for s in single_account.split(',') if s.strip()]
            else:
                account_ids = [single_account]
    publish_now = bool(data.get('publish_now', True))  # true -> /posts/schedule/publish, false -> /posts/schedule
    caption = (data.get('caption') or os.getenv('DEFAULT_CAPTION') or '').strip()

    # znajdź plik exportu
    filename = _export_filename_for(clip_id)
    local_path = get_data_path('media', 'exports', filename)
    app.logger.info(f"DEBUG: Looking for file at path: {local_path}")
    app.logger.info(f"DEBUG: File exists: {os.path.exists(local_path)}")

    if not os.path.exists(local_path):
        return jsonify({'ok': False, 'error': f'export not found: {filename}', 'hint': 'Run /api/render first', 'source': 'app'}), 404

    dry_run = False
    if not api_key or not workspace_id or not account_ids:
        dry_run = True

    # Etap 1: upload media do Publera (preferujemy upload pliku lokalnego)
    media_id = None
    upload_payload_preview = {'file': filename, 'direct_upload': True, 'in_library': False}

    if dry_run:
        # w dry-run nie wysyłamy nic do Publera – tylko logujemy plan
        plan = {
            'step': 'upload+publish',
            'media_upload': upload_payload_preview,
            'post': {
                'accounts': account_ids or ['MISSING'],
                'networks': {
                    'default': {
                        'type': 'video',
                        'text': caption,
                        'media': [{'id': 'MEDIA_ID'}]
                    }
                },
                'publish_now': publish_now
            }
        }
        _write_publish_log_publer(clip_id, plan, None, 'dry-run')
        return jsonify({'ok': True, 'dry_run': True, 'plan': plan})

    try:
        # Upload: POST /api/v1/media (multipart/form-data)
        with open(local_path, 'rb') as f:
            files = {'file': (filename, f, 'video/mp4')}
            form = {'direct_upload': 'true', 'in_library': 'false'}
            up = requests.post(
                'https://app.publer.com/api/v1/media',
                headers=_publer_headers(api_key, workspace_id),
                files=files,
                data=form,
                timeout=120
            )
        try:
            up_json = up.json()
        except Exception:
            up_json = {'text': up.text}
        if not up.ok:
            _write_publish_log_publer(clip_id, {'upload_form': form}, up_json, 'upload_error')
            return jsonify({'ok': False, 'error': 'publer upload failed', 'status_code': up.status_code, 'response': up_json, 'source': 'publer'}), 502
        # Robust extraction of media fields from upload response
        media_id = None
        media_path = None
        upload_thumbnails = None
        if isinstance(up_json, dict):
            media_id = up_json.get('id')
            media_path = up_json.get('path')
            if isinstance(up_json.get('thumbnails'), list):
                upload_thumbnails = up_json.get('thumbnails')
            data_obj = up_json.get('data') if isinstance(up_json.get('data'), dict) else None
            if data_obj:
                media_id = media_id or data_obj.get('id')
                media_path = media_path or data_obj.get('path')
                if upload_thumbnails is None and isinstance(data_obj.get('thumbnails'), list):
                    upload_thumbnails = data_obj.get('thumbnails')
                result_obj = data_obj.get('result') if isinstance(data_obj.get('result'), dict) else None
                if result_obj and isinstance(result_obj.get('media'), list) and result_obj['media']:
                    first_media = result_obj['media'][0]
                    if isinstance(first_media, dict):
                        media_id = media_id or first_media.get('id')
                        media_path = media_path or first_media.get('path')
                        if upload_thumbnails is None and isinstance(first_media.get('thumbnails'), list):
                            upload_thumbnails = first_media.get('thumbnails')
        # Debug: loguj odpowiedź uploadu
        app.logger.info(f"DEBUG: About to log upload debug for {clip_id}, media_id={media_id}, media_path={media_path}, thumbnails={upload_thumbnails}")
        _write_publish_log_publer(clip_id, {'upload_debug': {'up_json': up_json, 'media_id': media_id, 'media_path': media_path, 'upload_thumbnails': upload_thumbnails}}, None, 'upload_debug')
        if not media_id:
            _write_publish_log_publer(clip_id, {'upload_response': up_json}, up_json, 'upload_missing_id')
            return jsonify({'ok': False, 'error': 'missing media id from Publer upload', 'response': up_json, 'source': 'publer'}), 502
    except Exception as e:
        _write_publish_log_publer(clip_id, {'exception': str(e)}, None, 'upload_exception')
        return jsonify({'ok': False, 'error': f'upload exception: {e}', 'source': 'publer'}), 502

    # Po uploadzie: poczekaj na miniatury, aby móc ustawić default_thumbnail
    app.logger.info(f"DEBUG: Starting thumbnails section for {clip_id}")
    thumbs = None
    default_thumb_index = None
    try:
        # Wymuszamy istnienie miniaturek - zwiększony timeout do 90s
        thumbs = upload_thumbnails if (isinstance(upload_thumbnails, list) and upload_thumbnails) else _publer_fetch_thumbnails(api_key, workspace_id, media_id, max_wait_seconds=90)
        
        # Walidacja miniaturek - przerywamy publikację, jeśli nie są dostępne
        if not (isinstance(thumbs, list) and thumbs):
            _write_publish_log_publer(clip_id, {'reason': 'no_thumbnails_yet'}, None, 'abort_no_thumbnails')
            return jsonify({'ok': False, 'error': 'Publer thumbnails not ready yet; retry later', 'source': 'publer'}), 409
            
        # Publer używa 1-based index dla default_thumbnail (np. Reels)
        default_thumb_index = 1
        
        # Debug: loguj pobrane miniatury
        _write_publish_log_publer(clip_id, {'debug_thumbnails': {'upload_thumbnails': upload_thumbnails, 'fetched_thumbs': thumbs, 'default_thumb_index': default_thumb_index}}, None, 'debug_thumbnails')
        print(f"[DEBUG] Miniatury gotowe: {len(thumbs)} miniatur, default_thumb_index={default_thumb_index}")
    except Exception as e:
        _write_publish_log_publer(clip_id, {'thumbnails_exception': str(e)}, None, 'thumbnails_exception')
        return jsonify({'ok': False, 'error': f'Error fetching thumbnails: {str(e)}', 'source': 'publer'}), 502
        _write_publish_log_publer(clip_id, {'debug_thumbnails_error': str(e)}, None, 'debug_thumbnails_error')

    # Etap 2: publikacja
    # Mapowanie account_id -> provider (aktualne konta z .env)
    account_provider_map = {
        '68aec04c16f59bff1278915c': 'instagram',  # YummiShoty
        '68aec038f4b288fd1444392b': 'tiktok'      # yummi_is_afk
    }
    
    # Buduj networks na podstawie używanych kont
    networks = {}
    for aid in account_ids:
        provider = account_provider_map.get(aid)
        if not provider:
            # Jeśli nie znamy mapowania dla danego konta, zwróć błąd
            _write_publish_log_publer(clip_id, {'unknown_account_id': aid}, None, 'unknown_account_id')
            return jsonify({'ok': False, 'error': f'Unknown account_id: {aid}', 'source': 'app'}), 400
            
        # Tworzenie media_obj bez 'type' (type jest tylko na poziomie networks)
        media_obj = {'id': media_id}
        if media_path:
            media_obj['path'] = media_path
        # Miniatury są już zwalidowane wcześniej, więc zawsze będą dostępne
        media_obj['thumbnails'] = thumbs
        media_obj['default_thumbnail'] = default_thumb_index
        
        if provider == 'instagram':
            # Instagram Reels: w sieci musi być type, text, media + details.type = reel
            networks[provider] = {
                'type': 'video',
                'text': caption,
                'media': [media_obj],
                'details': {'type': 'reel'}
            }
        else:
            # TikTok i inne platformy używają 'video' z text + media
            networks[provider] = {
                'type': 'video',
                'text': caption,
                'media': [media_obj]
            }
    
    # Walidacja czy mamy jakiekolwiek networks
    if not networks:
        _write_publish_log_publer(clip_id, {'reason': 'no_networks'}, None, 'no_networks')
        return jsonify({'ok': False, 'error': 'No valid networks configured for provided account_ids', 'source': 'app'}), 400
        
    # Logowanie finalnego payloadu dla debugowania
    print(f"[DEBUG] Finalny payload networks: {networks}")
    _write_publish_log_publer(clip_id, {'final_networks': networks}, None, 'debug_networks')
    
    post_payload = {
        'bulk': {
            'state': 'scheduled',
            'posts': [
                {
                    'networks': networks,
                    'accounts': [
                        { 'id': aid } for aid in account_ids
                    ]
                }
            ]
        }
    }

    endpoint = 'https://app.publer.com/api/v1/posts/schedule/publish' if publish_now else 'https://app.publer.com/api/v1/posts/schedule'

    try:
        res = requests.post(
            endpoint,
            headers={**_publer_headers(api_key, workspace_id), 'Content-Type': 'application/json'},
            json=post_payload,
            timeout=60
        )
        try:
            res_json = res.json()
        except Exception:
            res_json = {'text': res.text}
        if not res.ok:
            _write_publish_log_publer(clip_id, post_payload, res_json, 'publish_error')
            return jsonify({'ok': False, 'error': 'publer publish failed', 'status_code': res.status_code, 'response': res_json, 'source': 'publer'}), 502
        _write_publish_log_publer(clip_id, post_payload, res_json, 'published')
        # Polling job_status (jeśli dostępny job_id), aby wykryć ewentualne błędy i je zalogować
        job_id = res_json.get('job_id') if isinstance(res_json, dict) else None
        job_status = None
        if job_id:
            for _ in range(8):
                try:
                    jr = requests.get(
                        f'https://app.publer.com/api/v1/job_status/{job_id}',
                        headers=_publer_headers(api_key, workspace_id),
                        timeout=15
                    )
                    try:
                        job_status = jr.json()
                    except Exception:
                        job_status = {'text': jr.text}
                    st = ''
                    if isinstance(job_status, dict):
                        st = str(job_status.get('status') or job_status.get('state') or '').lower()
                    if jr.ok and st not in ('', 'pending', 'in_progress', 'running', 'processing'):
                        break
                    if not jr.ok:
                        break
                except Exception as e:
                    job_status = {'exception': str(e)}
                    break
                time.sleep(1)
            _write_publish_log_publer(clip_id, {'post_payload': post_payload, 'job_id': job_id}, job_status, 'job_status')
            # Evaluate job_status to determine final publish result per account (dokładny feedback)
            published_flag = True
            errors = []
            per_accounts = []
            source = 'publer'
            try:
                if isinstance(job_status, dict):
                    payload = job_status.get('payload')
                    if isinstance(payload, list):
                        for item in payload:
                            item_status = str(item.get('status', '')).lower()
                            item_type = str(item.get('type', '')).lower()
                            failure = item.get('failure') or {}
                            acc_id = failure.get('account_id') or item.get('account_id')
                            acc_name = failure.get('account_name') or item.get('account_name')
                            provider = failure.get('provider') or item.get('provider')
                            message = failure.get('message') or item.get('message')
                            # zbuduj podsumowanie per konto
                            per_accounts.append({
                                'provider': provider,
                                'account_id': acc_id,
                                'account_name': acc_name,
                                'status': item_status or (item_type if item_type in ('error','success') else ''),
                                **({'message': message} if message else {})
                            })
                            if item_type == 'error' or item_status in ('failed', 'error'):
                                published_flag = False
                                errors.append({
                                    'provider': provider,
                                    'account_id': acc_id,
                                    'account_name': acc_name,
                                    'message': message or 'Unknown Publer error'
                                })
                    top_status = str(job_status.get('status') or job_status.get('state') or '').lower()
                    if top_status in ('failed', 'error'):
                        published_flag = False
            except Exception as e:
                errors.append({'message': f'job_status evaluation error: {e}'})
            # Określ źródło błędu: Publer vs aplikacja (tu: Publer, bo błąd po stronie job_status)
            err_source = None
            if not published_flag:
                err_source = 'publer'
            summary = {
                'ok': published_flag,
                'published': published_flag,
                'response': res_json,
                'job_id': job_id,
                'job_status': job_status,
                'per_accounts': per_accounts,
                **({'errors': errors} if errors else {}),
                **({'source': err_source} if err_source else {})
            }
            _write_publish_log_publer(clip_id, {'summary': summary}, None, 'publish_summary')
            return jsonify(summary)
        return jsonify({'ok': True, 'published': True, 'response': res_json})
    except Exception as e:
        _write_publish_log_publer(clip_id, post_payload, {'exception': str(e)}, 'publish_exception')
        return jsonify({'ok': False, 'error': f'publish exception: {e}', 'source': 'publer'}), 502

@app.route('/api/publer/workspaces', methods=['GET'])
def api_publer_workspaces():
    """Pobierz listę workspace'ów z Publera. API key z ?api_key= lub z .env (PUBLER_API_KEY)."""
    api_key = (request.args.get('api_key') or os.getenv('PUBLER_API_KEY') or '').strip()
    if not api_key:
        return jsonify({'ok': False, 'error': 'Missing PUBLER_API_KEY. Provide via ?api_key= or set in .env'}), 400
    try:
        res = requests.get(
            'https://app.publer.com/api/v1/workspaces',
            headers={'Authorization': f'Bearer-API {api_key}'},
            timeout=30
        )
        try:
            data = res.json()
        except Exception:
            data = {'text': res.text}
        return jsonify({'ok': res.ok, 'status_code': res.status_code, 'data': data}), res.status_code
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Exception: {e}'}), 502


@app.route('/api/publer/accounts', methods=['GET'])
def api_publer_accounts():
    """Pobierz listę kont w Publerze dla podanego workspace. API key i workspace z query lub .env."""
    api_key = (request.args.get('api_key') or os.getenv('PUBLER_API_KEY') or '').strip()
    workspace_id = (request.args.get('workspace_id') or os.getenv('PUBLER_WORKSPACE_ID') or '').strip()
    if not api_key:
        return jsonify({'ok': False, 'error': 'Missing PUBLER_API_KEY. Provide via ?api_key= or set in .env'}), 400
    if not workspace_id:
        return jsonify({'ok': False, 'error': 'Missing workspace_id. Provide via ?workspace_id= or set PUBLER_WORKSPACE_ID in .env'}), 400
    try:
        res = requests.get(
            'https://app.publer.com/api/v1/accounts',
            headers=_publer_headers(api_key, workspace_id),
            timeout=30
        )
        try:
            data = res.json()
        except Exception:
            data = {'text': res.text}
        return jsonify({'ok': res.ok, 'status_code': res.status_code, 'data': data}), res.status_code
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Exception: {e}'}), 502

# Dodatkowy endpoint: weryfikacja statusu publikacji po job_id (dokładny feedback per konto)
@app.route('/api/publer/post-status', methods=['GET'])
def api_publer_post_status():
    job_id = (request.args.get('job_id') or '').strip()
    api_key = (request.args.get('api_key') or os.getenv('PUBLER_API_KEY') or '').strip()
    workspace_id = (request.args.get('workspace_id') or os.getenv('PUBLER_WORKSPACE_ID') or '').strip()
    if not job_id:
        return jsonify({'ok': False, 'error': 'Missing job_id'}), 400
    if not api_key:
        return jsonify({'ok': False, 'error': 'Missing PUBLER_API_KEY'}), 400
    if not workspace_id:
        return jsonify({'ok': False, 'error': 'Missing PUBLER_WORKSPACE_ID'}), 400
    try:
        jr = requests.get(
            f'https://app.publer.com/api/v1/job_status/{job_id}',
            headers=_publer_headers(api_key, workspace_id),
            timeout=30
        )
        try:
            job_status = jr.json()
        except Exception:
            job_status = {'text': jr.text}
        published_flag = jr.ok
        errors = []
        per_accounts = []
        try:
            if isinstance(job_status, dict):
                payload = job_status.get('payload')
                if isinstance(payload, list):
                    for item in payload:
                        item_status = str(item.get('status', '')).lower()
                        item_type = str(item.get('type', '')).lower()
                        failure = item.get('failure') or {}
                        acc_id = failure.get('account_id') or item.get('account_id')
                        acc_name = failure.get('account_name') or item.get('account_name')
                        provider = failure.get('provider') or item.get('provider')
                        message = failure.get('message') or item.get('message')
                        per_accounts.append({
                            'provider': provider,
                            'account_id': acc_id,
                            'account_name': acc_name,
                            'status': item_status or (item_type if item_type in ('error','success') else ''),
                            **({'message': message} if message else {})
                        })
                        if item_type == 'error' or item_status in ('failed', 'error'):
                            published_flag = False
                            errors.append({
                                'provider': provider,
                                'account_id': acc_id,
                                'account_name': acc_name,
                                'message': message or 'Unknown Publer error'
                            })
                top_status = str(job_status.get('status') or job_status.get('state') or '').lower()
                if top_status in ('failed', 'error'):
                    published_flag = False
        except Exception as e:
            errors.append({'message': f'job_status evaluation error: {e}'})
        err_source = None
        if not published_flag:
            err_source = 'publer'
        summary = {
            'ok': published_flag,
            'published': published_flag,
            'job_id': job_id,
            'job_status': job_status,
            'per_accounts': per_accounts,
            **({'errors': errors} if errors else {}),
            **({'source': err_source} if err_source else {})
        }
        return jsonify(summary), 200 if published_flag else 502
    except Exception as e:
        return jsonify({'ok': False, 'error': f'Exception: {e}'}), 502

# --- KICK REPORT ENDPOINTS --------------------------------
def _no_cache(resp):
    try:
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
    except Exception:
        pass
    return resp

@app.route('/api/generate-raport-kick')
def api_generate_raport_kick():
    # uruchamiamy scrape+raport w tle; kasowanie starego HTML jest w jobie pod lockiem
    print('[kick] api_generate_raport_kick: spawn background thread')
    threading.Thread(target=job_refresh_kick_and_report, daemon=True).start()
    return _no_cache(jsonify({'message': 'Scrape + generowanie Kick uruchomione'})), 202

@app.route('/raport-kick')
def raport_kick():
    return _no_cache(send_file(get_data_path('kick', 'raport_kick.html')))

@app.route('/api/report-kick-ready')
def api_report_kick_ready():
    # Raport Kick uznaj za gotowy tylko, gdy istnieje plik HTML i bieżący cykl nie jest w trakcie
    progress_default = {
        'status': 'idle',
        'total': 0,
        'processed': 0,
        'updated_at': None,
    }
    try:
        p = _safe_read_json(get_data_path('kick', 'progress.json'), progress_default) or progress_default
    except Exception:
        p = progress_default
    st = str((p or {}).get('status') or '').lower()
    exists = os.path.exists(get_data_path('kick', 'raport_kick.html'))
    # Traktuj 'finished' i 'idle' jako gotowe; unikaj zwracania gotowości podczas 'scraping'/'generating'
    fresh = st in ('finished', 'idle', '')
    return _no_cache(jsonify({'ready': bool(exists and fresh), 'status': st}))

@app.route('/raport-kick-fragment')
def raport_kick_fragment():
    # bezpieczny odczyt danych raportu Kick (eliminuje JSONDecodeError przy BOM/partial)
    data_path = get_data_path('kick', 'raport_kick_data.json')
    default_data = {'clips': [], 'stats': {'total_clips': 0}}
    data = _safe_read_json(data_path, default_data) or default_data
    clips = data.get('clips') or []
    stats = data.get('stats') or {'total_clips': 0}
    # wczytaj preferencje streamerów (wyróżnieni / pomijani / tagi)
    prefs_default = { 'highlighted': [], 'skipped': [], 'tags': {}, 'platforms': {} }
    prefs = _safe_read_json(get_data_path('streamers_prefs.json'), prefs_default) or prefs_default
    # Ujednolicenie wielkości liter dla dopasowania nazw
    highlighted_streamers = set([str(s).lower() for s in (prefs.get('highlighted') or [])])
    skipped_streamers = set([str(s).lower() for s in (prefs.get('skipped') or [])])
    # filtr pomijanych
    if skipped_streamers:
        clips = [c for c in clips if (c.get('broadcaster') or '').lower() not in skipped_streamers]
        try:
            from collections import Counter
            total_clips = len(clips)
            broad_counts = Counter(c.get('broadcaster') for c in clips)
            stats = {
                'total_clips': total_clips,
                'top_streamers': broad_counts.most_common(3),
            }
        except Exception:
            pass
    return _no_cache(render_template('raport_kick_fragment.html', clips=clips, stats=stats, highlighted_streamers=highlighted_streamers))

@app.route('/api/report-kick-status')
def api_report_kick_status():
    progress_default = {
        'status': 'idle',
        'total': 0,
        'processed': 0,
        'updated_at': None,
    }
    p = _safe_read_json(get_data_path('kick', 'progress.json'), progress_default)
    return _no_cache(jsonify({'progress': p}))

# --- ADMIN: force unlock Kick report + reset progress ---
@app.route('/api/unlock-kick', methods=['POST'])
def api_unlock_kick():
    lock_path = get_data_path('kick', 'raport_kick.lock')
    progress_path = get_data_path('kick', 'progress.json')
    removed = False
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
            removed = True
    except Exception:
        pass
    try:
        _safe_write_json(progress_path, {
            'status': 'idle',
            'total': 0,
            'processed': 0,
            'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
    except Exception:
        pass
    return _no_cache(jsonify({'ok': True, 'lock_removed': removed}))

    

# --- HEALTHCHECK -----------------------------------------
@app.route('/health')
def health():
    twitch_ready = os.path.exists(get_data_path('reports', 'twitch', 'raport.html'))
    kick_ready = os.path.exists(get_data_path('kick','raport_kick.html'))

    # rozszerzona diagnostyka środowiska i MoviePy
    diag = {
        'python_executable': None,
        'python_version': None,
        'site_packages': [],
        'moviepy_available': False,
        'moviepy_version': None,
        'moviepy_file': None,
        'moviepy_tools_file': None,
        'deprecated_version_of_signature': None,
        'venv311_python': None,
        'env_TRANSCRIBE_PYTHON': bool(os.getenv('TRANSCRIBE_PYTHON')),
    }
    try:
        import sys as _sys, inspect as _inspect
        diag['python_executable'] = getattr(_sys, 'executable', None)
        diag['python_version'] = getattr(_sys, 'version', None)
        # pokaż ścieżki site-packages dla lepszej diagnostyki
        try:
            diag['site_packages'] = [p for p in getattr(_sys, 'path', []) if 'site-packages' in p]
        except Exception:
            pass
        # venv311 python
        try:
            candidate = os.path.join(PROJECT_ROOT, 'venv311', 'Scripts', 'python.exe')
            diag['venv311_python'] = candidate if os.path.isfile(candidate) else None
        except Exception:
            pass
        try:
            import moviepy, moviepy.tools as _t
            diag['moviepy_available'] = True
            diag['moviepy_version'] = getattr(moviepy, '__version__', None)
            diag['moviepy_file'] = getattr(moviepy, '__file__', None)
            diag['moviepy_tools_file'] = getattr(_t, '__file__', None)
            try:
                sig = _inspect.signature(_t.deprecated_version_of)
                diag['deprecated_version_of_signature'] = str(sig)
            except Exception as _e:
                diag['deprecated_version_of_signature'] = f'err: {type(_e).__name__}: {_e}'
        except Exception as _e:
            diag['moviepy_available'] = False
            diag['moviepy_version'] = f'err: {type(_e).__name__}: {_e}'
    except Exception:
        pass

    return jsonify({
        'status': 'ok',
        'twitch_report_ready': twitch_ready,
        'kick_report_ready': kick_ready,
        'diagnostics': diag,
    })

# --- GLOBAL JSON ERROR HANDLERS --------------------------

@app.errorhandler(HTTPException)
def handle_http_exception(e: HTTPException):
    response = {
        'error': e.description,
        'code': e.code,
        'name': e.name
    }
    return jsonify(response), e.code

@app.errorhandler(Exception)
def handle_unexpected_exception(e: Exception):
    # Log to stderr for visibility
    print('[unhandled]', repr(e))
    return jsonify({'error': 'Internal Server Error', 'type': e.__class__.__name__}), 500


# --- optional: scheduler wiring if used elsewhere (left as-is) ---

@app.route('/api/ensure-cache/status')
def api_ensure_cache_status():
    path = get_data_path('ensure_cache_progress.json')
    prog = _safe_read_json(path, default=None)
    if not prog:
        prog = {'state': 'idle', 'total': 0, 'done': 0, 'errors': 0}
    return jsonify(prog)

if __name__ == '__main__':
    # Flask app entrypoint
    try:
        port = int(os.getenv('PORT', '5001'))
    except ValueError:
        port = 5001
    host = os.getenv('HOST', '127.0.0.1')
    # Wymuszamy debug False
    debug = False

    # Kontrola uruchamiania harmonogramu przez ENV (domyślnie wyłączony)
    should_start_scheduler = str(os.getenv('ENABLE_SCHEDULER', 'false')).lower() in ('1','true','yes','on')
    if should_start_scheduler:
        scheduler = BackgroundScheduler(timezone="UTC")
        scheduler.add_job(job_update_streamers, 'interval', minutes=30, id='update_streamers', replace_existing=True, coalesce=True, max_instances=1)
        # auto-odświeżanie raportu co 10 minut (spójne z frontendowym licznikiem)
        scheduler.add_job(job_generate_twitch_report, 'interval', minutes=10, id='generate_report', replace_existing=True, coalesce=True, max_instances=1)
        scheduler.add_job(job_refresh_kick_and_report, 'interval', minutes=45, id='kick_refresh', replace_existing=True, coalesce=True, max_instances=1)
        scheduler.start()
        # expose instance for /api/schedule-info
        scheduler_instance = scheduler
        atexit.register(lambda: scheduler.shutdown(wait=False))
    else:
        try:
            print('[scheduler] disabled (ENABLE_SCHEDULER not set to true)')
        except Exception:
            pass

    # Upewnij się, że Werkzeg nie oczekuje WERKZEUG_SERVER_FD
    try:
        import os as _os
        _os.environ.pop('WERKZEUG_SERVER_FD', None)
        _os.environ.pop('WERKZEUG_RUN_MAIN', None)
    except Exception:
        pass

    app.run(host=host, port=port, debug=debug, use_reloader=False, threaded=True, use_debugger=False)

    # Parametryzacja host/port/debug z ENV

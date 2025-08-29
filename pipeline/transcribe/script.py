import os
import re
import json
import logging
import unicodedata
import textwrap
import numpy as np
import argparse
import sys

# Ensure vendored whisperx package is importable when not installed via pip
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_VENDORED_WX_BASE = os.path.join(_THIS_DIR, 'whisperx')
if os.path.isdir(os.path.join(_VENDORED_WX_BASE, 'whisperx')) and _VENDORED_WX_BASE not in sys.path:
    sys.path.insert(0, _VENDORED_WX_BASE)

from dotenv import load_dotenv
import whisperx

from moviepy.editor import VideoFileClip, AudioFileClip
from moviepy.video.compositing.CompositeVideoClip import CompositeVideoClip
from moviepy.video.VideoClip import VideoClip
from PIL import Image, ImageDraw, ImageFont


# =========================
# Logi
# =========================
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("shorts")


# =========================
# Baza przekleństw (JSON)
# =========================
def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()

def load_badwords(path="badwords.json"):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    bad = {_normalize(w.strip()) for w in raw if str(w).strip()}
    log.info(f"Załadowano {len(bad)} przekleństw z {path}")
    return bad

WORD_RE = re.compile(r"\b[\w\-']+\b", re.UNICODE)

def is_bad_token(token: str, badwords) -> bool:
    return _normalize(token) in badwords

def censor_token(token: str) -> str:
    n = len(token)
    if n <= 2: return "*" * n
    if n <= 4: return token[0] + "*" * (n-1)
    return token[0] + "*" * (n-2) + token[-1]

def censor_text_line(line: str, badwords) -> str:
    out, i = [], 0
    for m in WORD_RE.finditer(line):
        out.append(line[i:m.start()])
        tok = m.group(0)
        out.append(censor_token(tok) if is_bad_token(tok, badwords) else tok)
        i = m.end()
    out.append(line[i:])
    return "".join(out)


# =========================
# Kolory mówców
# =========================
SPEAKER_COLORS = ["yellow","cyan","magenta","lime","orange","deepskyblue","violet","salmon"]
def get_speaker_color(speaker: str) -> str:
    try:
        idx = int(str(speaker).replace("SPEAKER_", ""))
    except Exception:
        idx = 0
    return SPEAKER_COLORS[idx % len(SPEAKER_COLORS)]


# =========================
# Wygładzanie przeskoków mówców
# =========================
def smooth_speakers(segments, min_turn_dur=0.30):
    for seg in segments:
        words = seg.get("words") or []
        if len(words) < 2:
            continue
        smoothed = [words[0]]
        for w in words[1:]:
            prev = smoothed[-1]
            # jeśli zmiana mówcy i „przerwa” krótsza niż próg → przypisz poprzedniego
            if w.get("speaker") != prev.get("speaker") and (w["end"] - prev["end"]) < min_turn_dur:
                w["speaker"] = prev.get("speaker")
            smoothed.append(w)
        seg["words"] = smoothed
    return segments


# =========================
# Karaoke clip (przezroczysta maska, highlight na aktywnym słowie)
# =========================
def make_karaoke_clip(words, seg_start, seg_end, highlight_color,
                      badwords, font_path=None, fontsize=65, height=1080, wrap_width=25):
    # budujemy cenzurowane tokeny 1:1 do słów (zachowujemy indeksy)
    tokens = []
    for w in words:
        t = w['word'].strip('.,?!:;')
        tokens.append(censor_token(t) if is_bad_token(t, badwords) else t)
    display_text = " ".join(tokens)

    try:
        font = ImageFont.truetype(font_path or "arialbd.ttf", fontsize)
    except IOError:
        font = ImageFont.load_default()

    wrapped = textwrap.wrap(display_text, width=wrap_width)
    text_width = max((font.getbbox(line)[2] - font.getbbox(line)[0]) for line in wrapped) if wrapped else 0
    line_spacing = fontsize + 10
    text_height = len(wrapped) * line_spacing + 30

    # pozycje słów (kolejno od góry-lewej do prawej)
    word_positions = []
    y_offset = 15
    for line in wrapped:
        wline = line.split()
        line_w = font.getbbox(line)[2] - font.getbbox(line)[0]
        x = (int(text_width) - line_w) / 2 + 20
        for t in wline:
            w_w = font.getbbox(t)[2] - font.getbbox(t)[0]
            word_positions.append((x, y_offset, w_w))
            x += font.getbbox(t + " ")[2] - font.getbbox(" ")[0]
        y_offset += line_spacing

    word_times = [(w['start'], w['end']) for w in words]

    def make_frame_rgba(t):
        # znajdź aktywne słowo
        idx = None
        abs_time = t + seg_start
        for i, (s, e) in enumerate(word_times):
            if s <= abs_time < e:
                idx = i; break
            if abs_time >= e:
                idx = i
        if idx is None:
            idx = 0

        img = Image.new("RGBA", (int(text_width) + 40, int(text_height)), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # highlight
        if highlight_color and 0 <= idx < len(word_positions):
            x, y, ww = word_positions[idx]
            draw.rectangle((x - 5, y - 5, x + ww + 5, y + fontsize + 5), fill=highlight_color)

        # tekst
        y_draw = 15
        for line in wrapped:
            line_w = font.getbbox(line)[2] - font.getbbox(line)[0]
            x = (int(text_width) - line_w) / 2 + 20
            draw.text((x, y_draw), line, font=font, fill="white", stroke_width=3, stroke_fill="black")
            y_draw += line_spacing

        rgba = np.array(img)
        rgb = rgba[..., :3]
        alpha = rgba[..., 3] / 255.0
        return rgb, alpha

    duration = max(0.0, float(seg_end - seg_start))
    def make_color_frame(t):
        rgb, _ = make_frame_rgba(t); return rgb
    def make_mask_frame(t):
        _, a = make_frame_rgba(t); return a

    color_clip = VideoClip(make_color_frame, ismask=False, duration=duration)
    mask_clip  = VideoClip(make_mask_frame, ismask=True,  duration=duration)
    karaoke_clip = color_clip.set_mask(mask_clip)
    return karaoke_clip.set_start(seg_start).set_position(("center", height * 0.5))


# =========================
# CLI: Nakładka karaoke na już wyrenderowane wideo
# =========================
def _split_on_punct(words_list):
    subs, cur = [], []
    for w in words_list:
        cur.append(w)
        if w['word'].strip().endswith(('.', ',', '?', '!')):
            subs.append(cur); cur = []
    if cur: subs.append(cur)
    return subs

def apply_karaoke_overlay(input_path: str, json_path: str, output_path: str | None = None,
                          offset: float = 0.0, height: int = 1080, fps: int = 30,
                          font_path: str | None = None) -> int:
    """Zastosuj overlay karaoke do istniejącego wideo, na podstawie JSON (word-level).
    Zapis do output_path lub in-place (zamiana pliku wejściowego) jeśli output_path nie podany.
    Zwraca kod 0 przy sukcesie, >0 przy błędzie.
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data_json = json.load(f)
        segments = data_json.get('segments') or []
        if not segments:
            log.warning("Brak segmentów w JSON – nic do nałożenia.")
            return 2
    except Exception as e:
        log.error(f"Nie udało się wczytać JSON: {e}")
        return 3

    # baza
    base = VideoFileClip(input_path)
    subtitle_clips = []

    for seg in segments:
        words = seg.get('words', []) or []
        if not words:
            continue
        speakers = [w.get("speaker", "SPEAKER_00") for w in words]
        main_speaker = max(set(speakers), key=speakers.count) if speakers else "SPEAKER_00"
        color = get_speaker_color(main_speaker)

        for sub in _split_on_punct(words):
            seg_start = max(0.0, float(sub[0]['start'] - offset))
            seg_end   = max(seg_start, float(sub[-1]['end'] - offset))
            clip = make_karaoke_clip(
                sub, seg_start, seg_end, color,
                badwords=set(), font_path=font_path, fontsize=65, height=height, wrap_width=25
            )
            subtitle_clips.append(clip)

    if not subtitle_clips:
        log.warning("Brak zbudowanych klipów napisów – pomijam.")
        try:
            base.close()
        except Exception:
            pass
        return 4

    final_video = CompositeVideoClip([base, *subtitle_clips])
    tmp_out = (output_path or input_path) + '.karaoke.tmp.mp4'
    try:
        final_video.write_videofile(
            tmp_out,
            fps=fps,
            codec='libx264',
            audio_codec='aac',
            audio_bitrate='192k',
            preset='medium',
            ffmpeg_params=['-crf', '18', '-movflags', '+faststart']
        )
    finally:
        try:
            final_video.close()
        except Exception:
            pass
        try:
            base.close()
        except Exception:
            pass

    # podmień
    try:
        if output_path:
            os.replace(tmp_out, output_path)
        else:
            os.replace(tmp_out, input_path)
    except Exception as e:
        log.error(f"Nie udało się podmienić pliku wynikowego: {e}")
        try:
            os.remove(tmp_out)
        except Exception:
            pass
        return 5

    log.info("Nałożono overlay karaoke.")
    return 0


# =========================
# Cenzura audio (padding + fade)
# =========================
def censor_audio(audio_path, word_segments, output_audio_path, badwords,
                 pad_ms=80, fade_ms=20, prob_threshold=None):
    import soundfile as sf, librosa
    y, sr = librosa.load(audio_path, sr=None)
    N = len(y)
    mask = np.ones(N, dtype=np.float32)

    def to_samples(t): return int(max(0, min(N, t * sr)))

    for w in word_segments:
        if prob_threshold is not None and float(w.get("prob", 1.0)) < prob_threshold:
            continue
        if not is_bad_token(w["word"], badwords):
            continue
        s = to_samples(max(0.0, w['start'] - pad_ms/1000.0))
        e = to_samples(min(N/sr,     w['end']   + pad_ms/1000.0))
        if e <= s: continue

        fade = int(fade_ms * sr / 1000)
        if fade > 0 and e - s > 2*fade:
            mask[s:s+fade] *= np.linspace(1.0, 0.0, fade, dtype=np.float32)
            mask[s+fade:e-fade] = 0.0
            mask[e-fade:e] *= np.linspace(0.0, 1.0, fade, dtype=np.float32)
        else:
            mask[s:e] = 0.0

    y_censored = y * mask
    sf.write(output_audio_path, y_censored, sr)
    log.info(f"Zapisano ocenzurowane audio: {output_audio_path}")


# =========================
# Tryb transkrypcji (bez renderu) → SRT
# =========================
def transcribe_to_srt(video_path, subtitle_path, hf_token, badwords,
                      language="pl", model_size="medium", diarize=True):
    temp_audio = "temp_audio.wav"
    try:
        # Ekstrakcja audio
        video = VideoFileClip(video_path)
        video.audio.write_audiofile(temp_audio)

        device = "cpu"
        model = whisperx.load_model(model_size, device=device, compute_type="float32")
        result = model.transcribe(temp_audio, language=language)

        align_model, metadata = whisperx.load_align_model(language_code=language, device=device)
        aligned = whisperx.align(result["segments"], align_model, metadata, temp_audio, device)

        if diarize:
            try:
                from whisperx.diarize import DiarizationPipeline  # lazy import
                diarize_model = DiarizationPipeline(device=device) if not hf_token else DiarizationPipeline(use_auth_token=hf_token, device=device)
                diar = diarize_model(temp_audio)
                final = whisperx.assign_word_speakers(diar, aligned)
                final["segments"] = smooth_speakers(final["segments"], min_turn_dur=0.30)
            except Exception as e:
                log.warning(f"Diarization failed, continuing without it: {e}")
                final = aligned
        else:
            # Bez diarization: zostaw segmenty/words jak są
            final = aligned

        # Budowa SRT
        srt_lines = []
        idx = 1
        for seg in final["segments"]:
            seg_text = censor_text_line(seg.get("text", "").strip(), badwords) if badwords else seg.get("text", "").strip()
            s = seg['start']; e = seg['end']
            start_srt = f"{int(s//3600):02}:{int((s%3600)//60):02}:{int(s%60):02},{int((s%1)*1000):03}"
            end_srt   = f"{int(e//3600):02}:{int((e%3600)//60):02}:{int(e%60):02},{int((e%1)*1000):03}"
            srt_lines.append(f"{idx}\n{start_srt} --> {end_srt}\n{seg_text}\n\n")
            idx += 1

        os.makedirs(os.path.dirname(subtitle_path) or ".", exist_ok=True)
        with open(subtitle_path, "w", encoding="utf-8") as f:
            f.writelines(srt_lines)
        log.info(f"Zapisano SRT: {subtitle_path}")

        # Extra: zapisz JSON z segmentami/words
        try:
            json_path = os.path.splitext(subtitle_path)[0] + ".json"
            payload = {
                "language": "pl",
                "diarize": True,
                "segments": final.get("segments", [])
            }
            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump(payload, jf, ensure_ascii=False, indent=2)
            log.info(f"Zapisano JSON: {json_path}")
        except Exception as e:
            log.warning(f"Nie udało się zapisać JSON (full): {e}")
    finally:
        try:
            if os.path.exists(temp_audio):
                os.remove(temp_audio)
        except Exception:
            pass


# =========================
# Główna procedura (domyślnie TikTok/IG)
# =========================
def add_captions(video_path, output_path, subtitle_path, hf_token, badwords,
                 font_path=None, fontsize=65, wrap_width=25,
                 pad_ms=80, fade_ms=20, prob_threshold=None):
    temp_audio = "temp_audio.wav"
    temp_censored = "temp_audio_censored.wav"

    video = VideoFileClip(video_path)
    try:
        # 1) audio
        video.audio.write_audiofile(temp_audio)

        # 2) ASR (dokładniej: large-v2) + align + diarization
        device = "cpu"
        model = whisperx.load_model("medium", device=device, compute_type="float32")  # model CPU default
        result = model.transcribe(temp_audio, language="pl")

        align_model, metadata = whisperx.load_align_model(language_code="pl", device=device)
        aligned = whisperx.align(result["segments"], align_model, metadata, temp_audio, device)

        if True:
            try:
                from whisperx.diarize import DiarizationPipeline  # lazy import
                diarize_model = DiarizationPipeline(device=device) if not hf_token else DiarizationPipeline(use_auth_token=hf_token, device=device)
                diar = diarize_model(temp_audio)
                final = whisperx.assign_word_speakers(diar, aligned)
                final["segments"] = smooth_speakers(final["segments"], min_turn_dur=0.30)
            except Exception as e:
                log.warning(f"Diarization failed in full mode, continuing without it: {e}")
                final = aligned
        # 3) Cenzura audio
        all_words = []
        for seg in final["segments"]:
            all_words.extend(seg.get("words", []) or [])
        censor_audio(temp_audio, all_words, temp_censored, badwords, pad_ms, fade_ms, prob_threshold)

        # 4) Przygotowanie obrazu na pion 1080x1920
        width, height = video.size
        target_w, target_h = 1080, 1920  # TikTok/IG
        # jeśli video jest szerokie (np. 16:9), wycinamy pion ze środka:
        src_ar = width / height
        dst_ar = target_w / target_h  # 9:16 = 0.5625
        if src_ar > dst_ar:
            # za szerokie -> crop szerokość
            new_w = int(height * dst_ar)
            video = video.crop(width=new_w, height=height, x_center=width//2, y_center=height//2)
        elif src_ar < dst_ar:
            # za wąskie -> crop wysokość (rzadszy przypadek)
            new_h = int(width / dst_ar)
            video = video.crop(width=width, height=new_h, x_center=width//2, y_center=height//2)

        # 5) Napisy: tylko gdy ktoś mówi (czas = [seg_start, seg_end])
        subtitle_clips = []
        srt_lines = []
        idx = 1

        def split_on_punct(words_list):
            subs, cur = [], []
            for w in words_list:
                cur.append(w)
                if w['word'].strip().endswith(('.', ',', '?', '!')):
                    subs.append(cur); cur = []
            if cur: subs.append(cur)
            return subs

        for seg in final["segments"]:
            words = seg.get("words", []) or []
            if not words: continue

            for sub in split_on_punct(words):
                seg_start = sub[0]['start']
                seg_end   = sub[-1]['end']  # nie trzymamy dłużej → znika po ciszy
                speakers = [w.get("speaker", "SPEAKER_00") for w in sub]
                main_speaker = max(set(speakers), key=speakers.count) if speakers else "SPEAKER_00"
                color = get_speaker_color(main_speaker)

                clip = make_karaoke_clip(
                    sub, seg_start, seg_end, color,
                    badwords=badwords, font_path=font_path, fontsize=fontsize, height=target_h, wrap_width=wrap_width
                )
                subtitle_clips.append(clip)

            # SRT z cenzurą
            seg_text = censor_text_line(seg["text"].strip(), badwords)
            s = seg['start']; e = seg['end']
            start_srt = f"{int(s//3600):02}:{int((s%3600)//60):02}:{int(s%60):02},{int((s%1)*1000):03}"
            end_srt   = f"{int(e//3600):02}:{int((e%3600)//60):02}:{int(e%60):02},{int((e%1)*1000):03}"
            srt_lines.append(f"{idx}\n{start_srt} --> {end_srt}\n{seg_text}\n\n")
            idx += 1

        with open(subtitle_path, "w", encoding="utf-8") as f:
            f.writelines(srt_lines)
        log.info(f"Zapisano SRT: {subtitle_path}")

        # 6) Składanie + eksport pod TikTok/IG (1080x1920, 30fps, H.264 High, CRF 17)
        base = video.set_audio(AudioFileClip(temp_censored))
        final_video = CompositeVideoClip([base, *subtitle_clips])

        final_video.write_videofile(
            output_path,
            fps=30,  # TikTok/IG lub 60; 30 wystarcza
            codec="libx264",
            audio_codec="aac",
            audio_bitrate="192k",
            ffmpeg_params=[
                "-preset", "slow",
                "-crf", "17",
                "-vf", "scale=1080:1920:flags=lanczos,format=yuv420p",
                "-profile:v", "high",
                "-level", "4.2",
                "-movflags", "+faststart"
            ]
        )
        log.info(f"Zapisano wideo: {output_path}")

    finally:
        for p in (temp_audio, temp_censored):
            try:
                if os.path.exists(p): os.remove(p)
            except Exception:
                pass


def main():
    # Argumenty CLI (opcjonalne)
    parser = argparse.ArgumentParser(description="Transkrypcja/Render shortów")
    parser.add_argument("--mode", choices=["full", "transcribe"], default=None, help="full=render, transcribe=tylko SRT")
    parser.add_argument("--input", help="Ścieżka do pliku wideo (gdy --mode transcribe lub --apply-karaoke)")
    parser.add_argument("--subtitle", help="Ścieżka docelowa SRT (gdy --mode transcribe)")
    parser.add_argument("--language", default="pl")
    parser.add_argument("--model", default="medium")
    parser.add_argument("--no-diarization", action="store_true", help="Wyłącz diarization (domyślnie włączony)")
    parser.add_argument("--badwords", default="badwords.json")
    # Tryb: apply-karaoke
    parser.add_argument("--apply-karaoke", action="store_true", help="Nałóż overlay karaoke na istniejący plik wideo, bazując na JSON word-level")
    parser.add_argument("--json", help="Ścieżka do pliku JSON z word-level danymi (gdy --apply-karaoke)")
    parser.add_argument("--offset", type=float, default=0.0, help="Offset (sekundy) względem -ss zastosowanego przy renderze")
    parser.add_argument("--height", type=int, default=1080, help="Wysokość docelowego obszaru wideo (np. 1080)")
    parser.add_argument("--fps", type=int, default=30, help="FPS wyjściowy (np. 30)")
    parser.add_argument("--font", default=None, help="Ścieżka do czcionki TTF (opcjonalnie)")
    parser.add_argument("--output", default=None, help="Ścieżka pliku wyjściowego (opcjonalnie; domyślnie in-place)")
    args = parser.parse_args()

    load_dotenv()
    hf_token = os.getenv("HF_TOKEN")  # jeśli masz, użyje; jak nie – spróbuje bez
    if not hf_token:
        log.warning("Brak HF_TOKEN w .env — jeśli pyannote jest gated, dołóż token.")

    # Tryb apply-karaoke (in-place)
    if args.apply_karaoke:
        if not args.input or not args.json:
            log.error("Dla --apply-karaoke podaj --input i --json")
            return
        rc = apply_karaoke_overlay(
            input_path=args.input,
            json_path=args.json,
            output_path=args.output,
            offset=float(args.offset or 0.0),
            height=int(args.height or 1080),
            fps=int(args.fps or 30),
            font_path=args.font,
        )
        # zakończ proces z kodem rc (ważne dla subprocess w API)
        sys.exit(int(rc))

    # Tryb transcribe-only
    if args.mode == "transcribe":
        if not args.input or not args.subtitle:
            log.error("Dla --mode transcribe podaj --input i --subtitle")
            return
        bw = set()
        if args.badwords and os.path.exists(args.badwords):
            try:
                bw = load_badwords(args.badwords)
            except Exception as e:
                log.warning(f"Nie udało się wczytać badwords: {e}")
        transcribe_to_srt(
            video_path=args.input,
            subtitle_path=args.subtitle,
            hf_token=hf_token,
            badwords=bw,
            language=args.language,
            model_size=args.model,
            diarize=(not args.no_diarization)
        )
        return

    # Domyślnie: batch z folderu (oryginalne zachowanie)
    input_folder = "clips"
    output_folder = "output"
    subtitle_folder = "subtitles"
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(subtitle_folder, exist_ok=True)

    badwords = load_badwords(args.badwords) if os.path.exists(args.badwords) else set()

    for fn in os.listdir(input_folder):
        if not fn.lower().endswith(".mp4"): continue
        in_p = os.path.join(input_folder, fn)
        out_p = os.path.join(output_folder, f"captioned_{fn}")
        sub_p = os.path.join(subtitle_folder, f"{os.path.splitext(fn)[0]}.srt")
        log.info(f"Processing: {fn}")
        add_captions(
            video_path=in_p,
            output_path=out_p,
            subtitle_path=sub_p,
            hf_token=hf_token,
            badwords=badwords,
            font_path=None,       # lub wskaż TTF, np. "C:/Windows/Fonts/arialbd.ttf"
            fontsize=65,
            wrap_width=25,
            pad_ms=80,
            fade_ms=20,
            prob_threshold=None   # ustaw np. 0.4, jeśli masz pewność per słowo
        )

    log.info("Wszystkie klipy zostały przetworzone!")


if __name__ == "__main__":
    main()
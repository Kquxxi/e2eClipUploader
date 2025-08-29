import os
import sys
import subprocess
from typing import Optional


def transcribe_srt(
    input_path: str,
    subtitle_path: str,
    language: str = "pl",
    model: str = "medium",
    diarize: bool = True,
    python_exe: Optional[str] = None,
    hf_token: Optional[str] = None,
    cwd: Optional[str] = None,
    timeout_sec: Optional[int] = None,
) -> int:
    """
    Uruchamia pipeline/transcribe/script.py w trybie transcribe-only (generuje SRT bez renderu).

    Zwraca kod wyjścia procesu (0 przy sukcesie). Rzuca wyjątek w przypadku błędów IO.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "script.py")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Nie znaleziono script.py: {script_path}")

    os.makedirs(os.path.dirname(os.path.abspath(subtitle_path)) or ".", exist_ok=True)

    exe = python_exe or sys.executable or "python"

    # diagnostyka (bez sekretów)
    print(f"[adapter] using python exe: {exe}")
    print(f"[adapter] script path: {script_path}")

    args = [
        exe,
        script_path,
        "--mode", "transcribe",
        "--input", input_path,
        "--subtitle", subtitle_path,
        "--language", language,
        "--model", model,
    ]
    if not diarize:
        args.append("--no-diarization")

    # Środowisko: przekazujemy istniejące + opcjonalnie HF_TOKEN
    env = os.environ.copy()
    if hf_token:
        env["HF_TOKEN"] = hf_token

    proc = subprocess.run(
        args,
        cwd=cwd or script_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )

    # Przekaż standardowe logi do konsoli wywołującej (ułatwia debug)
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)

    return proc.returncode


if __name__ == "__main__":
    # Prosty manualny test (edytuj ścieżki poniżej albo uruchamiaj przez import)
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--subtitle", required=True)
    p.add_argument("--language", default="pl")
    p.add_argument("--model", default="medium")
    p.add_argument("--no-diarization", action="store_true")
    args = p.parse_args()

    rc = transcribe_srt(
        input_path=args.input,
        subtitle_path=args.subtitle,
        language=args.language,
        model=args.model,
        diarize=(not args.no_diarization),
    )
    sys.exit(rc)
import base64
import os
import re
import json
import time
from datetime import datetime, timedelta

import requests

from airflow.sdk import dag, task, Variable
from airflow.sdk.exceptions import AirflowFailException

SEGMENTS_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Segments"
TRANSCRIPTIONS_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Transcriptions"

OPENROUTER_URL = "https://openrouter.ai/api/v1/audio/transcriptions"
WHISPER_MODEL = "openai/whisper-large-v3-turbo"

try:
    API_KEY = Variable.get("openrouter_api_key")
except Exception:
    API_KEY = os.getenv("OPENROUTER_API_KEY", "")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
}


@task
def list_wav_files() -> list:
    if not os.path.isdir(SEGMENTS_DIR):
        raise FileNotFoundError(f"Lipsă director: {SEGMENTS_DIR}")

    # Pattern: ep_<numar>_seg_<numar>.wav
    segment_pattern = re.compile(r"^ep_\d+_seg_\d+\.wav$", re.IGNORECASE)

    files = []
    for root, _, filenames in os.walk(SEGMENTS_DIR):
        for f in sorted(filenames):
            if segment_pattern.match(f):
                files.append(os.path.join(root, f))

    files.sort()
    print(f"[list_wav_files] Găsite {len(files)} segmente individuale.")
    return files


@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
def transcribe_with_openrouter(file_path: str) -> dict:
    os.makedirs(TRANSCRIPTIONS_DIR, exist_ok=True)

    basename = os.path.basename(file_path)
    name_no_ext = os.path.splitext(basename)[0]
    json_output = os.path.join(TRANSCRIPTIONS_DIR, f"{name_no_ext}.json")

    if os.path.exists(json_output):
        print(f"[SKIP] Există deja: {json_output}")
        with open(json_output, "r", encoding="utf-8") as f:
            cached = json.load(f)
        return {
            "original_path": file_path,
            "api_response": cached,
            "model_used": WHISPER_MODEL,
            "cached": True,
        }

    if not API_KEY:
        raise AirflowFailException(
            "Lipsă API key. Setează variabila 'openrouter_api_key' în Airflow."
        )

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > 25:
        raise AirflowFailException(
            f"Fișier prea mare ({file_size_mb:.1f} MB): {basename}. "
            "Segmentarea din DAG 2 ar trebui să limiteze la 30 sec."
        )

    with open(file_path, "rb") as audio_file:
        audio_base64 = base64.b64encode(audio_file.read()).decode("utf-8")

    payload = {
        "model": WHISPER_MODEL,
        "input_audio": {
            "data": audio_base64,
            "format": "wav",
        },
        "language": "ro",
    }

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={**HEADERS, "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
    except requests.exceptions.Timeout:
        raise Exception("Timeout la OpenRouter. Se va retry.")
    except requests.exceptions.ConnectionError as ce:
        raise Exception(f"Eroare conexiune: {ce}")

    if resp.status_code == 402:
        raise AirflowFailException(
            "OpenRouter: Payment Required (402). Credite epuizate! "
            "Alimentează contul și apoi dă Clear la task-urile eșuate."
        )
    elif resp.status_code == 401:
        raise AirflowFailException(
            "OpenRouter: Unauthorized (401). Cheie API invalidă."
        )
    elif resp.status_code == 429:
        raise Exception(f"Rate limit (429): {resp.text}")
    elif resp.status_code >= 500:
        raise Exception(f"Eroare server OpenRouter ({resp.status_code}): {resp.text}")

    resp.raise_for_status()

    try:
        response_json = resp.json()
    except json.JSONDecodeError:
        raise AirflowFailException(f"Răspuns invalid JSON de la API: {resp.text[:200]}")

    with open(json_output, "w", encoding="utf-8") as jf:
        json.dump(response_json, jf, ensure_ascii=False, indent=2)

    print(f"[OK] Transcris: {basename} -> {json_output}")
    time.sleep(0.5)

    return {
        "original_path": file_path,
        "api_response": response_json,
        "model_used": WHISPER_MODEL,
        "cached": False,
    }


@task
def summary_report(results: list) -> dict:
    total = len(results)
    cached = sum(1 for r in results if r and r.get("cached"))
    fresh = sum(1 for r in results if r and not r.get("cached"))
    failed = total - cached - fresh

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_files": total,
        "transcribed_fresh": fresh,
        "skipped_cached": cached,
        "failed": failed,
        "transcriptions_dir": TRANSCRIPTIONS_DIR,
    }

    print("=" * 50)
    print("RAPORT TRANSCRIERE OPENROUTER")
    print("=" * 50)
    for k, v in report.items():
        print(f"  {k}: {v}")
    print("=" * 50)

    return report


@dag(
    dag_id="03_transcriere_openrouter",
    description="Transcrie segmentele WAV folosind Whisper prin OpenRouter",
    start_date=datetime(2026, 5, 16),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["deep_learning", "lab3", "transcription", "openrouter"],
    default_args={
        "owner": "airflow",
        "execution_timeout": timedelta(minutes=10),
    },
)
def transcription_dag():
    wav_files = list_wav_files()
    results = transcribe_with_openrouter.expand(file_path=wav_files)
    report = summary_report(results)

    wav_files >> results >> report


dag_instance = transcription_dag()

import csv
import json
import os
import re
import shutil
import tarfile
from datetime import datetime, timedelta

from airflow.sdk import dag, task

SEGMENTS_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Segments"
TRANSCRIPTIONS_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Transcriptions"
DATASET_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Dataset"


@task
def collect_dataset() -> dict:
    os.makedirs(DATASET_DIR, exist_ok=True)

    pattern = re.compile(r"^ep_(\d+)_seg_(\d+)\.json$", re.IGNORECASE)

    rows = []
    copied_files = []

    for fname in sorted(os.listdir(TRANSCRIPTIONS_DIR)):
        match = pattern.match(fname)
        if not match:
            continue

        ep_id = match.group(1)
        seg_num = match.group(2).zfill(3)

        json_path = os.path.join(TRANSCRIPTIONS_DIR, fname)
        wav_name = f"ep_{ep_id}_seg_{seg_num}.wav"
        wav_src = os.path.join(SEGMENTS_DIR, f"ep_{ep_id}", wav_name)
        wav_dst = os.path.join(DATASET_DIR, wav_name)

        if not os.path.exists(wav_src):
            print(f"[WARN] Lipsă: {wav_src}")
            continue

        shutil.copy2(wav_src, wav_dst)
        copied_files.append(wav_name)

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        text = data.get("text", "").strip()

        rows.append(
            {
                "episode_id": ep_id,
                "segment_number": seg_num,
                "wav_filename": wav_name,
                "wav_path": f"./{wav_name}",
                "text": text,
            }
        )

    csv_path = os.path.join(DATASET_DIR, "transcriptions.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "episode_id",
                "segment_number",
                "wav_filename",
                "wav_path",
                "text",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    result = {
        "csv_path": csv_path,
        "total_segments": len(rows),
        "copied_wavs": len(copied_files),
    }

    print("=" * 50)
    print("RAPORT COLECTARE DATASET")
    print("=" * 50)
    print(f"  CSV: {csv_path}")
    print(f"  Total segmente: {len(rows)}")
    print(f"  WAV-uri copiate: {len(copied_files)}")
    print("=" * 50)

    return result


@task
def archive_dataset(result: dict) -> dict:
    csv_path = result["csv_path"]
    csv_dir = os.path.dirname(csv_path)
    archive_name = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar.gz"
    archive_path = os.path.join(os.path.dirname(csv_dir), archive_name)

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(csv_dir, arcname=os.path.basename(csv_dir))

    print(f"Archive created: {archive_path}")
    return {"archive_path": archive_path}


@dag(
    dag_id="04_colectare_dataset",
    description="Colectează transcribierile într-un dataset pentru antrenare STT",
    start_date=datetime(2026, 5, 16),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["deep_learning", "lab3", "dataset", "stt"],
    default_args={
        "owner": "airflow",
        "execution_timeout": timedelta(minutes=10),
    },
)
def dataset_dag():
    result = collect_dataset()
    archive_dataset(result)


dag_instance = dataset_dag()

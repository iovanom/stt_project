import os
import glob
from pathlib import Path

import pendulum
import torch
import librosa
import soundfile as sf
from pydub import AudioSegment, silence

from airflow.sdk import dag, task


# ─── Config ───────────────────────────────────────────────────
DOWNLOAD_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Download"
OUTPUT_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Segments"
MAX_DURATION_MS = 30 * 1000  # 30 secunde în milisecunde
MIN_SPEECH_DURATION_MS = 250  # ignoră fragmente mai mici de 250ms
SAMPLING_RATE = 16000


# ─── Silero VAD setup ─────────────────────────────────────────
# Se descarcă automat la primul run
model, utils = torch.hub.load(
    repo_or_dir="snakers4/silero-vad",
    model="silero_vad",
    force_reload=False,
    onnx=False,
    trust_repo=True,
)
(
    get_speech_timestamps,
    save_audio,
    read_audio,
    VADIterator,
    collect_chunks,
) = utils


@dag(
    dag_id="02_segmentare_audio_inteligenta",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["segmentare", "vad", "audio"],
)
def audio_segmentation_pipeline():

    # ─────────────────────────────────────────────────────────
    # TASK 1: Listează toate fișierele .wav descărcate
    # ─────────────────────────────────────────────────────────
    @task
    def list_audio_files() -> list[str]:
        pattern = os.path.join(DOWNLOAD_DIR, "*.wav")
        files = sorted(glob.glob(pattern))
        print(f"Găsite {len(files)} fișiere audio.")
        return files

    # ─────────────────────────────────────────────────────────
    # TASK 2: Aplică VAD și taie la granițele naturale
    # Input:  cale fișier .wav
    # Output: listă de căi către segmentele generate
    # ─────────────────────────────────────────────────────────
    @task
    def split_audio_intelligently(file_path: str) -> list[str]:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_name = Path(file_path).stem
        out_dir = os.path.join(OUTPUT_DIR, file_name)
        os.makedirs(out_dir, exist_ok=True)

        # --- 2.1 Încarcă audio cu librosa (mono, 16kHz) ---
        wav, sr = librosa.load(file_path, sr=SAMPLING_RATE, mono=True)

        # --- 2.2 Rulează Silero VAD pentru a obține timestamp-uri de vorbire ---
        # get_speech_timestamps returnează o listă de dict-uri:
        # [{'start': 12345, 'end': 23456}, ...] în eșantione (samples)
        speech_timestamps = get_speech_timestamps(
            torch.tensor(wav),
            model,
            threshold=0.5,  # probabilitate minimă pentru vorbire
            sampling_rate=SAMPLING_RATE,
            min_speech_duration_ms=MIN_SPEECH_DURATION_MS,
            max_speech_duration_s=30,  # ajută la chunking
            min_silence_duration_ms=300,
        )

        if not speech_timestamps:
            print(f"Nu a fost detectată vorbire în {file_path}")
            return []

        # --- 2.3 Extrage doar segmentele cu vorbire (elimină muzică/tăcere) ---
        # `collect_chunks` concatenează segmentele de vorbire detectate
        # Elimină automat porțiunile fără vorbire dintre ele
        speech_audio = collect_chunks(speech_timestamps, torch.tensor(wav)).numpy()

        # --- 2.4 Salvează temporar audio-ul curățat ---
        # Aici avem un singur wave cu doar vorbire, dar poate fi >30s
        cleaned_path = os.path.join(out_dir, f"{file_name}_speech_only.wav")
        sf.write(cleaned_path, speech_audio, SAMPLING_RATE)

        # --- 2.5 Împarte în chunk-uri de max 30s la granițe de tăcere ---
        # Folosim pydub pentru a detecta silențile și a tăia inteligent
        audio = AudioSegment.from_wav(cleaned_path)

        # Detectează silenții de minim 200ms unde putem tăia
        silence_ranges = silence.detect_nonsilent(
            audio,
            min_silence_len=200,  # minim 200ms de tăcere ca să tai aici
            silence_thresh=-40,  # dBFS
        )

        # Dacă tot fișierul e sub 30s, îl păstrăm ca atare
        if len(audio) <= MAX_DURATION_MS:
            final_path = os.path.join(out_dir, f"{file_name}_seg_000.wav")
            audio.export(final_path, format="wav")
            return [final_path]

        # --- 2.6 Algoritm de chunking la granițe naturale ---
        segments = []
        current_chunk = AudioSegment.empty()
        chunk_idx = 0

        # Parcurgem intervalele de vorbire (non-silență)
        for start_ms, end_ms in silence_ranges:
            speech_part = audio[start_ms:end_ms]

            # Dacă adăugând acest fragment depășim 30s,
            # salvăm chunk-ul curent și începem altul
            if len(current_chunk) + len(speech_part) > MAX_DURATION_MS:
                if len(current_chunk) > 0:
                    out_path = os.path.join(
                        out_dir, f"{file_name}_seg_{chunk_idx:03d}.wav"
                    )
                    current_chunk.export(out_path, format="wav")
                    segments.append(out_path)
                    chunk_idx += 1
                    current_chunk = AudioSegment.empty()

            current_chunk += speech_part

        # Salvează ce a rămas
        if len(current_chunk) > 0:
            out_path = os.path.join(out_dir, f"{file_name}_seg_{chunk_idx:03d}.wav")
            current_chunk.export(out_path, format="wav")
            segments.append(out_path)

        print(
            f"Fișier {file_name}: {len(segments)} segmente generate "
            f"(fără muzică, max 30s)."
        )
        return segments

    # ─────────────────────────────────────────────────────────
    # ORCHESTRARE
    # ─────────────────────────────────────────────────────────
    audio_files = list_audio_files()

    # Dynamic Task Mapping: pentru FIECARE fișier audio rulează segmentarea
    segment_lists = split_audio_intelligently.expand(file_path=audio_files)

    # Dacă vrei să aplatizezi rezultatele într-o singură listă pentru DAG-ul următor:
    @task
    def flatten_segments(nested: list[list[str]]) -> list[str]:
        flat = [p for sublist in nested for p in sublist]
        print(f"Total segmente generate în toate folderele: {len(flat)}")
        return flat

    all_segments = flatten_segments(segment_lists)


audio_segmentation_pipeline()

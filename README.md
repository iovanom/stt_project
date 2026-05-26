# 🎙️ Laborator 3: Pipeline Audio complet — de la Scraping la Fine-Tuning Whisper

Acest proiect implementează un pipeline complet de procesare audio pentru limba română:

1. **Colectare** — scraping automat al emisiunilor de pe `moldova1.md`
2. **Segmentare** — VAD inteligent cu Silero pentru izolarea vorbirii
3. **Transcriere** — Whisper Large v3 Turbo via OpenRouter API
4. **Asamblare dataset** — generare `transcriptions.csv` + arhivă
5. **Fine-tuning** — LoRA pe Whisper cu datasetul colectat
6. **Inferență live** — transcriere din microfon în browser

---

## 🛠️ 1. Cerințe de Sistem

- **Python** ≥ 3.14
- **`ffmpeg`** — pentru conversia audio
- **`uv`** — manager de pachete Python (Astral)
- **GPU CUDA** (opțional, recomandat pentru fine-tuning)

---

## 📦 2. Inițializarea Proiectului și Dependențele

### Crearea proiectului

```bash
uv init .
```

### Adăugarea dependențelor

```bash
# Scraping
uv add "beautifulsoup4>=4.14.3" requests

# Apache Airflow (cu constrângeri de versiuni)
AIRFLOW_VERSION=3.2.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
uv add "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Procesare audio & ML
uv add librosa pydub soundfile torch torchaudio

# Dataset & Transformers (pentru fine-tuning)
uv add datasets transformers peft evaluate jiwer accelerate

# UI
uv add streamlit
```

> **Notă:** Structura `.venv` este instalată automat și `uv.lock` generat. Folosește `uv run` pentru a executa orice comandă în mediul virtual.

---

## ⚙️ 3. Configurarea Airflow

```bash
mkdir -p "./airflow_home/dags"
AIRFLOW_HOME="$(pwd)/airflow_home" uv run airflow db migrate

uv run airflow users create \
    --username admin \
    --firstname Student \
    --lastname Lab3 \
    --role Admin \
    --email student@exemplu.md \
    --password admin
```

---

## 📂 4. DAG-urile Pipeline-ului

Proiectul conține **4 DAG-uri** secvențiale în `airflow_home/dags/`:

### 4.1 `01_colectare_audio_moldova1_bash` — Colectare Audio

Scanează paginile catalogului moldova1.md (show #59), extrage link-uri de episoade, identifică stream-uri m3u8 și descarcă audio ca fișiere WAV (16kHz, Mono).

| Task | Tip | Descriere |
|------|-----|-----------|
| `discover_pages` | `@task` | Iterează paginile catalogului până la 404 |
| `extract_episode_links_from_page` | `@task` (mapped) | Extrage link-uri `/f/ro/<id>` cu BeautifulSoup |
| `flatten_episode_links` | `@task` | Aplatizează și deduplică lista de link-uri |
| `extract_m3u8_from_episode` | `@task` (mapped) | Găsește URL-ul m3u8 din pagina episodului |
| `download_audio` | `@task.bash` (mapped) | `ffmpeg` descarcă audio → WAV 16kHz mono |

Ieșire: `Download/ep_<id>.wav`

### 4.2 `02_segmentare_audio_inteligenta` — Segmentare cu VAD

Folosește **Silero VAD** pentru a elimina muzica și tăcerile, apoi segmentează vorbirea în bucăți de maxim 30 secunde.

| Task | Tip | Descriere |
|------|-----|-----------|
| `list_audio_files` | `@task` | Listează toate fișierele `.wav` din `Download/` |
| `split_audio_intelligently` | `@task` (mapped) | VAD → eliminare non-vorbire → segmentare la pauze |
| `flatten_segments` | `@task` | Aplatizează lista de segmente |

Parametri VAD: `threshold=0.5`, `min_speech=250ms`, `max_speech=30s`, `min_silence=300ms`

Ieșire: `Segments/ep_<id>/ep_<id>_seg_<NNN>.wav`

### 4.3 `03_transcriere_openrouter` — Transcriere cu Whisper

Trimite fiecare segment la OpenRouter API folosind modelul **`openai/whisper-large-v3-turbo`** pentru transcriere în limba română.

| Task | Tip | Descriere |
|------|-----|-----------|
| `list_wav_files` | `@task` | Listează segmentele `.wav` |
| `transcribe_with_openrouter` | `@task` (mapped, retries=3) | POST la OpenRouter cu audio base64 |
| `summary_report` | `@task` | Raport final: total, fresh, cached, failed |

Ieșire: `Transcriptions/ep_<id>/ep_<id>_seg_<NNN>.json`

### 4.4 `04_colectare_dataset` — Asamblare Dataset

Copiază perechile WAV + JSON într-un folder unitar și generează fișierul CSV pentru antrenament.

| Task | Tip | Descriere |
|------|-----|-----------|
| `collect_dataset` | `@task` | Copiază WAV + JSON → creează `transcriptions.csv` |
| `archive_dataset` | `@task` | Arhivează `Dataset/` în `dataset_YYYYMMDD_HHMMSS.tar.gz` |

Ieșire: `Dataset/` cu `transcriptions.csv` și fișierele `.wav`

---

## 🚀 5. Pornirea Airflow

```bash
AIRFLOW_HOME="$(pwd)/airflow_home" uv run airflow standalone
```

Accesează **http://localhost:8080**, autentificare `admin` / `admin`, activează DAG-urile și declanșează-le manual.

### Comenzi rapide cu `just`

```bash
just airflow standalone          # Pornește Airflow
just watch <dag_id>              # Verifică progresul unui DAG
just watch-live <dag_id> [sec]   # Monitorizare live
just archive                     # Arhivează Dataset/
```

---

## 📊 6. Monitorizarea Progresului

Script-ul `scripts/dag_progress.py` oferă monitorizare în timp real:

```bash
# Verificare unică
uv run python scripts/dag_progress.py 03_transcriere_openrouter

# Watch mode (actualizare la 10s)
uv run python scripts/dag_progress.py 03_transcriere_openrouter -w

# Watch cu interval personalizat
uv run python scripts/dag_progress.py 03_transcriere_openrouter -w -i 5
```

**Ieșire tipică:**
```
DAG: 03_transcriere_openrouter
Run: manual__2026-05-17T14:52:39
  DAG State:   running
  Total Tasks: 553
  ✓ success: 372   ● running: 12   ○ queued: 4   ◐ scheduled: 164
  Progress: 372/553 (67.3%)
```

**Legendă:** `✓` success | `●` running | `○` queued | `◐` scheduled | `✗` failed | `↻` up_for_retry

---

## 📁 7. Structura Dataset-ului

După rularea completă a celor 4 DAG-uri:

```
Dataset/
├── ep_5639_seg_000.wav
├── ep_5639_seg_001.wav
├── ...
└── transcriptions.csv
```

### `transcriptions.csv`

| Coloană | Descriere |
|---------|-----------|
| `episode_id` | ID-ul emisiei originale |
| `segment_number` | Numărul segmentului (000, 001, ...) |
| `wav_filename` | Numele fișierului audio |
| `wav_path` | Calea relativă către fișier |
| `text` | Transcrierea text în limba română |

### Specificații audio

- **Format:** WAV (PCM 16-bit)
- **Sample rate:** 16000 Hz (16kHz)
- **Canale:** Mono (1 channel)
- **Durată maximă segment:** 30 secunde

---

## 🎧 8. Transcript Viewer (Streamlit)

Aplicație interactivă pentru verificarea și editarea transcrierilor.

```bash
uv run streamlit run app_transcript_viewer.py
```

Disponibil la **http://localhost:8501**

### Funcționalități

- **Selector fișiere** — dropdown pentru navigare
- **Filtru status** — All / Unchecked / Checked
- **Căutare** — filtrează după cuvinte cheie
- **Redare audio** — ascultă fiecare segment în browser
- **Editare transcript** — modifică textul direct
- **Marcare verificare** — checkbox checked/unchecked
- **Salvare** — persistă automat în `Dataset/transcriptions.csv`

---

## 🤖 9. Fine-Tuning Whisper cu LoRA

Notebook-ul `whisper_finetuning.ipynb` antrenează un adaptor LoRA pe **Whisper Large v3 Turbo** folosind datasetul colectat.

### 9.1 Arhitectură

| Componentă | Detalii |
|------------|---------|
| **Model bază** | `openai/whisper-large-v3-turbo` (~809M params) |
| **Adaptare** | LoRA (PEFT) — `r=32`, `alpha=64`, `dropout=0.05` |
| **Module țintă** | `q_proj`, `v_proj` (query & value projections) |
| **Parametri antrenabili** | 6,553,600 (0.8% din total) |
| **Atenție** | `attn_implementation="eager"` (flash_attn blocat) |
| **Precizie** | fp16 + gradient checkpointing |

### 9.2 Antrenament

| Parametru | Valoare |
|-----------|---------|
| **Dataset** | 551 exemple (501 train / 50 eval) |
| **Batch size efectiv** | 16 (4 per device × 4 accumulation) |
| **Epochs** | 3 |
| **Learning rate** | 1e-4 |
| **Warmup** | 5% |
| **Optimizator** | AdamW (torch) |
| **Evaluare** | WER (Word Error Rate) la fiecare 200 steps |
| **Salvare** | Cel mai bun model după WER (max 2 checkpoint-uri) |
| **Input maxim** | 30 secunde audio |
| **Output maxim** | 448 tokens / 225 la generare |

### 9.3 Rularea Notebook-ului

```bash
# Instalează dependențele suplimentare pentru fine-tuning
uv add datasets transformers peft evaluate jiwer accelerate ipykernel ipywidgets

# Lansează Jupyter
uv run jupyter notebook whisper_finetuning.ipynb
```

### 9.4 Celulele Notebook-ului

| Celulă | Descriere |
|--------|-----------|
| **1. Block flash_attn** | Împiedică importul `flash_attn` care cauzează erori pe GPU fără suport |
| **2. Setup & Config** | Importuri, constante (`MODEL_NAME`, `CSV_PATH`, `OUTPUT_DIR`) |
| **3. Încărcare date** | Citește `Dataset/transcriptions.csv`, filtrează fișiere existente, split train/eval 90/10 |
| **4. Dataset HF** | Convertește DataFrame → `datasets.Dataset` cu coloană `Audio` la 16kHz |
| **5. Processor** | Inițializează `WhisperProcessor`, `WhisperTokenizer`, `WhisperFeatureExtractor` |
| **6. Preprocesare** | Extrage `input_features` (log-mel spectrogram) și tokenizează textul |
| **7. Data Collator** | Padding dinamic pentru batch-uri de input_features și labels |
| **8. Metrică WER** | Word Error Rate via `evaluate` (fallback la `jiwer`) |
| **9. Model + LoRA** | Încarcă modelul, aplică configurația LoRA, afișează parametrii antrenabili |
| **10. Training Args** | `Seq2SeqTrainingArguments` cu toți hiperparametrii |
| **11. Antrenare** | `Seq2SeqTrainer.train()` — ~9 minute pe Tesla T4 pentru 93 steps |
| **12. Salvare** | Salvează adaptorul LoRA și processor-ul în `OUTPUT_DIR` |
| **13. Inferență demo** | Pipeline ASR pe un sample din setul de evaluare |
| **14. Ascultare + comparație** | Redă audio + afișează referința și predicția |
| **15. Înregistrare microfon** | Înregistrare audio din browser (HTML5 MediaRecorder) |
| **16. Transcriere live** | Convertește înregistrarea → WAV → transcriere cu modelul fine-tuned |

### 9.5 Rezultate

După 3 epochs pe Tesla T4:
- **Training loss:** ~0.649
- **Trainable params:** 6,553,600 (0.8% din 815M)
- **Adaptor salvat:** `./whisper-turbo-ro-lora/` (doar ~25 MB)
- **Inferență:** Transcritere live din microfon în browser

### 9.6 Utilizare Model Antrenat

```python
from transformers import WhisperForConditionalGeneration, pipeline
from peft import PeftModel
import torch

base = WhisperForConditionalGeneration.from_pretrained(
    "openai/whisper-large-v3-turbo",
    torch_dtype=torch.float16,
    attn_implementation="eager"
).to("cuda")

model = PeftModel.from_pretrained(base, "./whisper-turbo-ro-lora").to("cuda")
model.eval()

pipe = pipeline(
    "automatic-speech-recognition",
    model=model,
    tokenizer=processor.tokenizer,
    feature_extractor=processor.feature_extractor,
    chunk_length_s=30,
    device=0,
)

result = pipe("audio.wav", generate_kwargs={"language": "romanian", "task": "transcribe"})
print(result["text"])
```

---

## 📋 10. Workflow Complet

```
moldova1.md                    Whisper Large v3 Turbo
    │                                 │
    ▼                                 ▼
01_colectare ──► Download/      03_transcriere ──► Transcriptions/
    │              (.wav)            │                (.json)
    ▼                                 ▼
02_segmentare ──► Segments/      04_colectare ──► Dataset/
                    (.wav)                           (.wav + .csv)
                                                       │
                                                       ▼
                                              whisper_finetuning.ipynb
                                                       │
                                                       ▼
                                              whisper-turbo-ro-lora/
                                              (adaptor LoRA ~25MB)
```

---

## 🛠️ 11. Comenzi Utile

```bash
# Rulare Airflow
just airflow standalone

# Monitorizare DAG-uri
just watch 01_colectare_audio_moldova1_bash
just watch-live 03_transcriere_openrouter

# Arhivare dataset
just archive

# Streamlit Transcript Viewer
uv run streamlit run app_transcript_viewer.py

# Jupyter pentru fine-tuning
uv run jupyter notebook whisper_finetuning.ipynb

# Verificare sistem
uv run python scripts/system_info.py
```

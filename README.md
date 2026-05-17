# 🎙️ Laborator 3: Extragere Audio pentru Modele STT (Speech-to-Text)

Acest proiect conține un Data Pipeline creat în **Apache Airflow**. Scopul său este să extragă (scrape) automat linkurile emisiunilor video de pe `moldova1.md` (paginând dinamic) și să le descarce direct sub formă de clipuri audio `.wav` optimizate pentru modele Machine Learning STT (16kHz, Mono).

## 🛠️ 1. Cerințe de Sistem (Arch Linux)

* **Utilitarul `ffmpeg`:** Pentru conversia audio direct din shell (verifică dacă este instalat).
* **Utilitarul Python `uv`:** Dezvoltat de Astral.

---

## 📦 2. Inițializarea Proiectului și Dependențele (uv init & add)

Vom crea proiectul folosind modulele de gestiune ale `uv`, astfel fișierele `pyproject.toml` și `uv.lock` vor rezolva perfect și reproductibil acest mediu.

### Crearea proiectului Python:
```bash
# Creează structura inițială (pyproject.toml etc) în folderul curent
uv init .
```

### Adăugarea dependențelor pentru Scraping:
```bash
uv add "beautifulsoup4>=4.14.3" requests
```

### Adăugarea Apache Airflow respectând Constrângerile:
Construcția imagistică complexă a Airflow cere ca dependențele sale să fie fixate. Cu versiunile recente `uv` putem adăuga pachetul declarând url-ul de *constraint* ca opțiune integrată la comanda de adăugare:

```bash
AIRFLOW_VERSION=3.2.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Adăugăm Airflow respectând limitele corecte ale pachetelor de sub capotă:
uv add apache-airflow=="${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

> **Notă:** Rulând aceste comenzi, structura `.venv` a fast instalată și `uv.lock` generat pentru rulări sigure în viitor. Nu este nici măcar nevoie să activezi explicit `.venv` manual atâta timp cât folosești comanda `uv run`.

---

## ⚙️ 3. Configurarea Ecosistemului Airflow

Pentru că am instalat Airflow în interiorul virtual enviroment-ului gestionat de `uv`, vom lansa utilitarele precedate de `uv run`.

### Setarea variabilelor și folderul intern
```bash
# Setăm un namespace local ca mediul nativ Airflow

mkdir -p "./airflow_home/dags"
```

### Migrarea bazei de date și crearea contului Admin
```bash
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

## 📂 4. Adăugarea DAG-ului

Salvează / copiaza logica DAG-ului tău (codul cu cele 3 task-uri și comanda bash python `ffmpeg`) în folderul de rulare proaspăt creat:
```bash
cp calea/catre/moldova1_audio_scraper.py "$AIRFLOW_HOME/dags/"
```

**Notă:** Verifică locația de download `DOWNLOAD_DIR` declarată sub `.py`, fie lăsând `/tmp/dataset_audio`, fie punând rezultatele direct în proiectul tău: `DOWNLOAD_DIR = "/home/userul_tau/Cale_Laborator/Download"`. Meniul Arch standard le gestionează rapid.

---

## 🚀 5. Start și Rulare (Standalone)

Airflow combină funcțiile majore și te eliberează de management multi-proces dacă alegi utilitarul modular:

```bash
AIRFLOW_HOME="$(pwd)/airflow_home" uv run airflow standalone
```

### Conectarea în aplicație
1. Accesează browserul și confirmă local: **http://localhost:8080**
2. Fă login folosind `admin` / `admin`.
3. Găsește fluxul DAG: `01_colectare_audio_moldova1_bash`.
4. Comută de pe **Paused** pe **Unpaused** folosind întrerupătorul din colț.
5. Declanșează task-ul de **Play (▶️ Trigger DAG)**.

---

## 📊 6. Monitorizarea Progresului DAG-urilor

Pentru a verifica starea și progresul unui DAG în timp real, folosește scriptul `scripts/dag_progress.py`:

### Comandă rapidă (o singură verificare):
```bash
uv run python scripts/dag_progress.py <dag_id>
```

### Watch mode (actualizare automată la fiecare 10 secunde):
```bash
uv run python scripts/dag_progress.py <dag_id> -w
```

### Watch mode cu interval personalizat:
```bash
uv run python scripts/dag_progress.py <dag_id> -w -i 5
```

**Exemple:**
```bash
# Verifică progresul DAG-ului de transcriere
uv run python scripts/dag_progress.py 03_transcriere_openrouter

# Urmărește progresul în timp real (implicit 10s)
uv run python scripts/dag_progress.py 03_transcriere_openrouter -w

# Verifică mai rapid (la fiecare 5 secunde)
uv run python scripts/dag_progress.py 03_transcriere_openrouter -w -i 5
```

**Ieșire tipică:**
```
============================================================
DAG: 03_transcriere_openrouter
============================================================

Run: manual__2026-05-17T14:52:39.990464+00:00
  DAG State:   running
  Total Tasks: 553
  Task States:
    ? null                :     1
    ○ queued              :     4
    ● running             :    12
    ◐ scheduled           :   164
    ✓ success             :   372
  Progress:
    Completed: 372/553 (67.3%)
    Remaining: 180
```

**Legenda statusuri:**
- `✓` success - task completat cu succes
- `●` running - task în execuție
- `○` queued - task așteaptă în coadă
- `◐` scheduled - task programat de scheduler
- `✗` failed - task eșuat
- `↻` up_for_retry - task în așteptarea retry-ului

---

## 📁 7. Arhiva Dataset-ului

După rularea completă a celor 4 DAG-uri, rezultatele sunt salvate în folderul `Dataset/`.

### Structura de fișiere

```
Dataset/
├── ep_{episode_id}_seg_{segment_number}.wav   # Fișiere audio (16kHz, Mono, PCM 16-bit)
└── transcriptions.csv                         # Fișier CSV cu transcrieri
```

### Structura fișierului CSV

Fișierul `transcriptions.csv` conține următoarele coloane:

| Coloană | Descriere |
|---------|-----------|
| `episode_id` | ID-ul emisiei originale (ex: 5639, 5640) |
| `segment_number` | Numărul segmentului (000, 001, ...) |
| `wav_filename` | Numele fișierului audio (ex: ep_5639_seg_000.wav) |
| `wav_path` | Calea relativă către fișierul audio |
| `text` | Transcrierea text a segmentului audio |

### Exemplu de conținut

```csv
episode_id,segment_number,wav_filename,wav_path,text
5639,000,ep_5639_seg_000.wav,./ep_5639_seg_000.wav,"Într-o lume în care dezinformarea se răspândește rapid..."
5639,001,ep_5639_seg_001.wav,./ep_5639_seg_001.wav,"lang sovietic. Cei care vin dinspre Uniunea Europeană..."
```

### Specificații tehnice audio

- **Format:** WAV (PCM 16-bit)
- **Sample rate:** 16000 Hz (16kHz)
- **Canale:** Mono (1 channel)
- **Durată maximă segment:** 30 secunde

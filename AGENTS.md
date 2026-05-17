# AGENTS.md - Development Guidelines for This Project

## Project Overview

This is an Apache Airflow-based audio processing pipeline for extracting, segmenting, and transcribing audio from Moldova1 broadcasts. The project uses `uv` for Python package management and includes DAGs for scraping, VAD-based segmentation, and Whisper transcription via OpenRouter.

## Build, Lint, and Test Commands

### Package Management (uv)

```bash
# Install dependencies (uses pyproject.toml + uv.lock)
uv sync

# Add a new dependency
uv add <package>

# Add dependency with constraints (like Airflow)
uv add apache-airflow==<version> --constraint <constraint-url>

# Run commands in the virtual environment
uv run <command>

# Create/update lock file
uv lock
```

### Running Airflow

```bash
# Start Airflow standalone (webserver + scheduler)
just airflow
# Or: AIRFLOW_HOME="$(pwd)/airflow_home" uv run airflow standalone

# Run Airflow CLI commands
AIRFLOW_HOME="$(pwd)/airflow_home" uv run airflow <command>
```

### Testing

There are currently no test files in this project. When tests are added:

```bash
# Run all tests with pytest
uv run pytest

# Run a single test file
uv run pytest tests/test_file.py

# Run a specific test function
uv run pytest tests/test_file.py::test_function_name

# Run with verbose output
uv run pytest -v

# Run with coverage
uv run pytest --cov=. --cov-report=html
```

### Linting and Formatting

```bash
# Run ruff linter (if installed)
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Run ruff formatter (if installed)
uv run ruff format .

# Run mypy type checker
uv run mypy .
```

Add these to `pyproject.toml` if needed:
```toml
[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4"]
ignore = ["E501"]

[tool.mypy]
python_version = "3.14"
warn_return_any = true
warn_unused_configs = true
```

## Code Style Guidelines

### Python Version
- Minimum Python 3.14 (as specified in `pyproject.toml`)

### Imports
- Use absolute imports (e.g., `from airflow.sdk import dag, task`)
- Group imports in the following order:
  1. Standard library (`os`, `re`, `json`, etc.)
  2. Third-party packages (`requests`, `pendulum`, `torch`, etc.)
  3. Local/application imports
- Sort alphabetically within each group
- Example:
  ```python
  import os
  import re
  from datetime import datetime, timedelta
  
  import requests
  import torch
  
  from airflow.sdk import dag, task
  ```

### Type Hints
- Use type hints for all function parameters and return values
- Use `| None` instead of `Optional[]` for simple types
- Example:
  ```python
  def process_audio(file_path: str) -> list[str]:
      ...
  
  def extract_m3u8_from_episode(episode_url: str) -> dict | None:
      ...
  ```

### Naming Conventions
- **Variables/functions**: snake_case (e.g., `download_dir`, `get_speech_timestamps`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `DOWNLOAD_DIR`, `SAMPLING_RATE`)
- **DAGs**: descriptive, with prefix (e.g., `01_colectare_audio_moldova1_bash`)
- **Task functions**: descriptive verbs (e.g., `discover_pages`, `extract_episode_links_from_page`)

### Docstrings
- Use Google-style docstrings for public functions
- Include: short description, args, returns, raises (if applicable)
- Example:
  ```python
  def extract_m3u8_from_episode(episode_url: str) -> dict | None:
      """Extract m3u8 URL from an episode page.
      
      Args:
          episode_url: URL of the episode page (e.g., https://moldova1.md/f/ro/8293)
      
      Returns:
          Dict with episode_url and m3u8_url, or None if not found.
      """
  ```

### Error Handling
- Use specific exception types when possible
- Use try/except for expected failure cases (network errors, missing files)
- Use `raise_for_status()` for HTTP errors
- Log errors with descriptive messages
- Example:
  ```python
  try:
      resp = requests.get(url, timeout=15)
      resp.raise_for_status()
  except requests.HTTPError:
      print(f"HTTP error at {url} — stopping.")
      return None
  except Exception as e:
      print(f"Error at {url}: {e}")
      return None
  ```

### DAG Structure (Airflow)
- Use the Airflow SDK (not old `airflow.operators`)
- Use `@dag` decorator with full configuration (schedule, start_date, catchup, tags)
- Use `@task` for Python operators, `@task.bash` for bash commands
- Use Dynamic Task Mapping with `.expand()` for parallel processing
- Place DAG file in `airflow_home/dags/` directory

### Configuration
- Store paths as module-level constants (SCREAMING_SNAKE_CASE)
- Use `os.getenv()` or Airflow `Variable.get()` for environment-sensitive config
- Keep hardcoded values (URLs, directories) at the top of the file

### Code Organization in DAGs
1. Imports
2. Configuration constants
3. Model loading (if outside task, for caching)
4. DAG decorator
5. Main function defining the DAG
6. Task definitions (as inner functions or top-level)
7. Orchestration (task dependencies)

### File Paths
- Use absolute paths or resolve relative to known locations
- Use `pathlib.Path` for path manipulation when beneficial
- Example: `file_path = Path("/home/ivan/tmp/new/deep_learning/lab_3/Download")`

### Audio Processing Conventions
- Sample rate: 16000 Hz (16kHz) for STT models
- Channels: Mono (1 channel)
- Format: WAV (PCM 16-bit)
- Max segment duration: 30 seconds

### General Best Practices
- Use f-strings for string formatting
- Use `print()` for logging in DAGs (Airflow logs separately)
- Avoid mutating global state across tasks
- Use idempotent operations (check if file exists before processing)
- Set appropriate timeouts for network requests (15-30 seconds typical)
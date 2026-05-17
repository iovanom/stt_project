---
name: airflow-cli
description: Manage Apache Airflow DAGs, tasks, and runs via CLI
---

# Airflow CLI Skill

This skill provides commands for managing Apache Airflow DAGs, tasks, and runs.

## Setup

First, set the AIRFLOW_HOME variable:
```bash
export AIRFLOW_HOME="/home/ivan/tmp/new/deep_learning/lab_3/airflow_home"
```

Or use the justfile command:
```bash
just airflow
```

## Common Commands

All commands use the pattern: `just airflow <command>`

### DAGs

```bash
# List all DAGs
just airflow dags list

# List DAGs in table format
just airflow dags list -o table

# Show DAG info
just airflow dags list-dag-runs <dag_id>

# Trigger a DAG manually
just airflow dags trigger -r "my_run_id" <dag_id>

# Pause/Unpause a DAG
just airflow dags pause <dag_id>
just airflow dags unpause <dag_id>
```

### DAG Runs

```bash
# List runs for a DAG
just airflow dags list-dag-runs <dag_id>

# List runs in table format
just airflow dags list-dag-runs <dag_id> -o table

# Get run details
just airflow dags get-runs <dag_id>
```

### Tasks

```bash
# List task instances for a DAG run
just airflow tasks list <dag_id> <execution_date>

# Get task instance details
just airflow tasks instance-details <dag_id> <task_id> <execution_date>

# Clear task instance (reset)
just airflow tasks clear <dag_id> <task_id> <execution_date>

# Retry a failed task
just airflow tasks retry <dag_id> <task_id> <execution_date>
```

### Logs

```bash
# Get task logs
just airflow tasks logs <dag_id> <task_id> <execution_date>
```

### Variables & Connections

```bash
# List variables
just airflow variables list

# Get a variable
just airflow variables get <key>

# Set a variable
just airflow variables set <key> <value>

# List connections
just airflow connections list

# Get connection details
just airflow connections get <conn_id>
```

### Tips

- Use `-o table` for readable output
- Use `-h` or `--help` on any command for more options
- Execution date format: `YYYY-MM-DD` (e.g., `2026-05-17`)
- The Airflow UI runs on port 8080 when standalone is running
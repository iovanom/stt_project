# Această linie trebuie să fie la început, neindentată
export AIRFLOW_HOME := justfile_directory() + "/airflow_home"

# Abia acum urmează comanda (rețeta)
airflow *args:
    @mkdir -p "$AIRFLOW_HOME"
    @uv run airflow {{args}}

# Monitorizează progresul unui DAG (o singură verificare)
# Usage: just watch <dag_id> once
watch dag_id:
    @uv run python scripts/dag_progress.py {{dag_id}}

# Monitorizează progresul unui DAG în timp real (implicit 10s)
# Usage: just watch-live <dag_id> [interval]
watch-live dag_id interval="10":
    @uv run python scripts/dag_progress.py {{dag_id}} -w -i {{interval}}

# Creează arhiva datasetului
# Usage: just archive
archive:
    @uv run python scripts/create_dataset_archive.py
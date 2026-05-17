#!/usr/bin/env python3
import sys
import sqlite3
import os
import time
import argparse
from pathlib import Path

AIRFLOW_HOME = Path(
    os.getenv("AIRFLOW_HOME", "/home/ivan/tmp/new/deep_learning/lab_3/airflow_home")
)
DB_PATH = AIRFLOW_HOME / "airflow.db"


def get_dag_progress(dag_id: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    run_query = """
        SELECT run_id, state, logical_date, triggered_by
        FROM dag_run
        WHERE dag_id = ?
        ORDER BY logical_date DESC
        LIMIT 5
    """
    cur.execute(run_query, (dag_id,))
    runs = cur.fetchall()

    results: dict = {"dag_id": dag_id, "runs": []}

    for run in runs:
        ti_query = """
            SELECT state, COUNT(*) as cnt
            FROM task_instance
            WHERE dag_id = ? AND run_id = ?
            GROUP BY state
        """
        cur.execute(ti_query, (dag_id, run["run_id"]))
        task_states: dict = {row["state"]: row["cnt"] for row in cur.fetchall()}

        total = sum(task_states.values()) if task_states else 0

        results["runs"].append(
            {
                "run_id": run["run_id"],
                "dag_state": run["state"],
                "logical_date": run["logical_date"],
                "triggered_by": run["triggered_by"],
                "task_states": task_states,
                "total_tasks": total,
            }
        )

    conn.close()
    return results


def format_progress(data: dict) -> str:
    lines: list = []
    lines.append(f"=" * 60)
    lines.append(f"DAG: {data['dag_id']}")
    lines.append(f"=" * 60)

    state_icons = {
        "success": "✓",
        "running": "●",
        "queued": "○",
        "scheduled": "◐",
        "failed": "✗",
        "upstream_failed": "⚠",
        "skipped": "S",
        "up_for_retry": "↻",
    }

    for run in data["runs"]:
        dag_state = run["dag_state"] or "unknown"
        logical_date = run["logical_date"] or "unknown"
        triggered_by = run["triggered_by"] or "unknown"

        lines.append(f"\nRun: {run['run_id']}")
        lines.append(f"  DAG State:   {dag_state}")
        lines.append(f"  Date:        {logical_date}")
        lines.append(f"  Triggered:   {triggered_by}")
        lines.append(f"  Total Tasks: {run['total_tasks']}")

        lines.append("  Task States:")
        for state, count in sorted(
            run["task_states"].items(), key=lambda x: x[0] or ""
        ):
            state_str = state or "null"
            icon = state_icons.get(state_str, "?")
            lines.append(f"    {icon} {state_str:20s}: {count:5d}")

        if run["total_tasks"] > 0:
            success = run["task_states"].get("success", 0)
            running = run["task_states"].get("running", 0)
            queued = run["task_states"].get("queued", 0)
            scheduled = run["task_states"].get("scheduled", 0)
            progress_pct = (success / run["total_tasks"]) * 100
            remaining = running + queued + scheduled

            lines.append(f"  Progress:")
            lines.append(
                f"    Completed: {success}/{run['total_tasks']} ({progress_pct:.1f}%)"
            )
            lines.append(f"    Remaining: {remaining}")

    return "\n".join(lines)


def watch_progress(dag_id: str, interval: int = 10) -> None:
    last_data = None

    while True:
        data = get_dag_progress(dag_id)
        output = format_progress(data)

        if output != last_data:
            print("\033[2J\033[H")
            print(output)
            print(f"\nWatching... (Ctrl+C to stop) [{time.strftime('%H:%M:%S')}]")
            last_data = output

        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Airflow DAG progress")
    parser.add_argument("dag_id", help="DAG ID to monitor")
    parser.add_argument("-w", "--watch", action="store_true", help="Watch for changes")
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=10,
        help="Seconds between checks (default: 10)",
    )

    args = parser.parse_args()

    if args.watch:
        watch_progress(args.dag_id, args.interval)
    else:
        data = get_dag_progress(args.dag_id)
        print(format_progress(data))

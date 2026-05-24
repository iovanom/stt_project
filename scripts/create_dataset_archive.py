#!/usr/bin/env python3
"""Standalone script to archive the Dataset folder."""

import os
import tarfile
from datetime import datetime

BASE_DIR = os.getcwd()
DATASET_DIR = os.path.join(BASE_DIR, "Dataset")


def main() -> None:
    if not os.path.isdir(DATASET_DIR):
        print(f"[ERROR] Directorul {DATASET_DIR} nu există.")
        return

    archive_name = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar.gz"
    archive_path = os.path.join(BASE_DIR, archive_name)

    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(DATASET_DIR, arcname=os.path.basename(DATASET_DIR))

    print(f"Archive created: {archive_path}")


if __name__ == "__main__":
    main()

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def list_completions(path: Path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        records = json.load(file)
    if not isinstance(records, list):
        raise ValueError(f"Expected a JSON array in {path}")
    return records


def get_completion(path: Path, job_id: int) -> dict | None:
    """Return the saved completion for a job, if one exists."""
    return next((record for record in list_completions(path) if record["job_id"] == job_id), None)


def next_job_id(path: Path) -> int:
    """Return the next ID after the jobs already stored in the file."""
    path = Path(path)
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as file:
        records = json.load(file)
    if not isinstance(records, list):
        raise ValueError(f"Expected a JSON array in {path}")
    return max((record["job_id"] for record in records), default=-1) + 1


def save_completion(path: Path, job_id: int, command: str, stdout: str) -> None:
    """Append a completed job to a JSON array, replacing the file atomically."""
    path = Path(path)
    if path.exists():
        with path.open(encoding="utf-8") as file:
            records = json.load(file)
        if not isinstance(records, list):
            raise ValueError(f"Expected a JSON array in {path}")
    else:
        records = []

    records.append({
        "job_id": job_id,
        "command": command,
        "stdout": stdout,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as file:
            temporary_path = Path(file.name)
            json.dump(records, file, indent=2)
            file.write("\n")
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

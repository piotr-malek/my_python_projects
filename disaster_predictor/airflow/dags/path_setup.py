"""Shared sys.path / .env setup for Airflow task callables."""

from pathlib import Path


def setup_airflow_paths() -> None:
    """
    Load .env and prepend import paths for task execution.

    Prefers the live project root over airflow/include/ when both exist so local
    runs do not silently use stale copies (run ``make sync`` before Docker deploy).
    """
    import os
    import sys
    from dotenv import load_dotenv

    dag_dir = Path(__file__).resolve().parent
    airflow_root_dir = dag_dir.parent

    for env_candidate in [airflow_root_dir / ".env", airflow_root_dir.parent / ".env"]:
        if env_candidate.is_file():
            load_dotenv(dotenv_path=env_candidate, override=True)
            break

    include_path = airflow_root_dir / "include"
    project_root = airflow_root_dir.parent

    # insert(0) last wins; project root must end up before include/
    if include_path.exists():
        include_path_str = str(include_path)
        if include_path_str not in sys.path:
            sys.path.insert(0, include_path_str)

    airflow_root_str = str(airflow_root_dir)
    if airflow_root_str not in sys.path:
        sys.path.insert(0, airflow_root_str)

    project_root_str = str(project_root)
    if project_root.exists() and project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

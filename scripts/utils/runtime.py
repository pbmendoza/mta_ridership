"""Shared runtime helpers for script entrypoints.

This module centralizes project-root detection and script logging bootstrap
to reduce repeated setup code across pipeline scripts.
"""

from __future__ import annotations

from datetime import datetime
import logging
from pathlib import Path
from typing import Optional, TextIO, Tuple


def find_project_root(start: Optional[Path] = None, *, require_git: bool = False) -> Path:
    """Find the repository root by searching upward for a ``.git`` directory.

    Args:
        start: Directory to begin searching from. Defaults to ``Path.cwd()``.
        require_git: When True, raise if no ``.git`` directory is found.

    Returns:
        The discovered project root, or the resolved start directory when
        ``require_git`` is False and no repository marker is found.
    """
    start_path = (start or Path.cwd()).resolve()

    for directory in [start_path, *start_path.parents]:
        if (directory / ".git").exists():
            return directory

    if require_git:
        raise RuntimeError(
            f"Could not find project root (.git) starting from: {start_path}"
        )
    return start_path


def setup_script_logging(
    *,
    base_dir: Path,
    logger_name: str,
    log_filename: Optional[str] = None,
    timestamped_prefix: Optional[str] = None,
    level: int = logging.INFO,
    fmt: str = "%(asctime)s - %(levelname)s - %(message)s",
    datefmt: Optional[str] = None,
    stream: Optional[TextIO] = None,
    clear_handlers: bool = True,
) -> Tuple[logging.Logger, Path]:
    """Configure file + stream logging for a script.

    Exactly one of ``log_filename`` or ``timestamped_prefix`` must be provided.
    """
    if (log_filename is None) == (timestamped_prefix is None):
        raise ValueError(
            "Provide exactly one of log_filename or timestamped_prefix."
        )

    log_dir = base_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if log_filename is not None:
        log_path = log_dir / log_filename
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"{timestamped_prefix}_{timestamp}.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    if clear_handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(stream) if stream is not None else logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger, log_path

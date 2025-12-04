"""Logging configuration and utilities."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from src.core.config import get_settings


def setup_logging(
    name: Optional[str] = None,
    level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Set up application logging with console and file handlers.

    Args:
        name: Logger name (defaults to root logger)
        level: Log level (defaults to settings)
        log_file: Log file path (defaults to settings)

    Returns:
        Configured logger instance
    """
    settings = get_settings()
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    log_level = level or settings.logging.level
    logger.setLevel(getattr(logging, log_level))

    formatter = logging.Formatter(
        settings.logging.format,
        datefmt=settings.logging.date_format,
    )

    # Console handler
    if settings.logging.console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File handler
    if settings.logging.file_enabled:
        file_path = log_file or settings.logging.file_path
        log_dir = Path(file_path).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=settings.logging.file_max_bytes,
            backupCount=settings.logging.file_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(getattr(logging, log_level))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Logger instance
    """
    return setup_logging(name)

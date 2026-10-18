"""
FILE:         Log_Writer.py
PROJECT:      SENG2045 Assignment 2 - Logging Server Tool
PROGRAMMER:   Brad Kajganich - 90326321
FIRST VERSION: 2026-10-17
DESCRIPTION:  The LogWriter contains the logic for writing received log
              entries to the log file. Every entry is stamped by the server
              with the current time in UTC, never with a time sent by a client.
"""

import os
from datetime import datetime, timezone

from Server_Config import LEVELS, ServerConfig


class LogWriter:
    """Formats entries with the configured template and appends them to the file."""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.min_rank = LEVELS.index(config.min_level)
        folder = os.path.dirname(config.log_file)
        if folder:
            os.makedirs(folder, exist_ok=True)

    def is_wanted(self, level: str) -> bool:
        """True if the level is at or above the configured minimum."""
        return LEVELS.index(level) >= self.min_rank

    def write(self, client_name: str, ip: str, port: int,
              level: str, message: str) -> bool:
        """Appends one entry to the log file. Returns False on I/O failure."""
        # Always UTC, always read from the server's own clock: logs from any
        # machine line up, and the server computer's timezone setting (or a
        # client) can never change what gets written.
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        entry = self.config.format.format(
            timestamp=timestamp, client_name=client_name, ip=ip,
            port=port, level=level, message=message)
        try:
            # Opened per write so the file can be rotated or deleted while
            # the server runs and files cannot be interleaved
            with open(self.config.log_file, "a", encoding="utf-8") as log:
                log.write(entry + "\n")
        except OSError as error:
            print(f"Could not write to {self.config.log_file}: {error}")
            return False
        return True

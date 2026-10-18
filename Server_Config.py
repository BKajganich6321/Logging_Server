"""
FILE:         Server_Config.py
PROJECT:      SENG2045 Assignment 2 - Logging Server Tool
PROGRAMMER:   Brad Kajganich - 90326321
FIRST VERSION: 2026-10-17
DESCRIPTION:  Holds and Validates config.ini file settings
"""

import configparser
import sys
from dataclasses import dataclass

# Log levels from least to most severe. Their order is used for min_level.
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]


@dataclass
class ServerConfig:
    """Holds every setting read from config.ini."""
    host: str
    port: int
    max_clients: int
    log_file: str
    format: str
    min_level: str
    max_line: int
    framelimit_messages: int
    framelimit_seconds: int
    ratelimit_disconnect: int


def load_config(path: str) -> ServerConfig:
    """
    FUNCTION:   load_config
    DESCRIPTION: Reads and validates the configuration file.
    PARAMETERS: path - location of the ini file
    RETURNS:    a populated ServerConfig; exits the program on bad input
    """
    parser = configparser.ConfigParser()
    if not parser.read(path):
        sys.exit(f"Config file not found or unreadable: {path}")

    try:
        config = ServerConfig(
            host=parser.get("server", "host"),
            port=parser.getint("server", "port"),
            max_clients=parser.getint("server", "max_clients"),
            log_file=parser.get("logging", "log_file"),
            format=parser.get("logging", "format", raw=True),
            min_level=parser.get("logging", "min_level").upper(),
            max_line=parser.getint("logging", "max_line"),
            framelimit_messages=parser.getint("ratelimit", "framelimit_messages"),
            framelimit_seconds=parser.getint("ratelimit", "framelimit_seconds"),
            ratelimit_disconnect=parser.getint("ratelimit", "ratelimit_disconnect"),
        )
    except (configparser.Error, ValueError) as error:
        sys.exit(f"Configuration ( {path} ) format encounterd error: {error}")

    # Confirm limits set are within scope
    if config.min_level not in LEVELS:
        sys.exit(f"min_level must be one of {LEVELS}")
    if not 1 <= config.port <= 65535:
        sys.exit("port must be between 1 and 65535")
    if (config.framelimit_messages < 1 or config.framelimit_seconds < 1
            or config.ratelimit_disconnect < 1):
        sys.exit("all [ratelimit] values must be at least 1")

    # Make sure the log template only uses the metrics made available
    try:
        config.format.format(timestamp="", client_name="", ip="", port=0, level="", message="")
    except (KeyError, IndexError, ValueError) as error:
        sys.exit(f"Bad log format template in {path}: {error!r}")
    return config

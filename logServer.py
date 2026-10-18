"""
FILE:         LogServer.py
PROJECT:      SENG2045 Assignment 2 - Logging Server Tool
PROGRAMMER:   Brad Kajganich - 90326321
FIRST VERSION: 2026-10-14
Purpose: TCP logging service. Loads configuration, listens asynchronouslly
            for TCP Clients, creates connections, parses and filters
            Client Levels and Messages before printing them to a consolidated
            log file using consistent organizational patterns.
"""

import argparse
import asyncio

from Logging_Server import LoggingServer
from Server_Config import load_config

DEFAULT_CONFIG_PATH = "config.ini"


def main() -> None:
    parser = argparse.ArgumentParser(description="TCP logging service")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH,
                        help="path to the config file (default: config.ini)")
    args = parser.parse_args()

    server = LoggingServer(load_config(args.config))
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nLogging service stopped.")


if __name__ == "__main__":
    main()

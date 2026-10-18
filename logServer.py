"""
SENG2045 Assignment 2 - Logging Service - LogServerog.py
Brad Kajganich
2026-10-14
Purpose: TCP logging service. Loads configuration, listens asynchronouslly
            for TCP Clients, creates connections, parses and filters
            Client Levels and Messages before printing them to a consolidated
            log file using consistent organizational patterns.
"""

import argparse
import asyncio
import configparser
import re
import sys
from dataclasses import dataclass

DEFAULT_CONFIG_PATH = "config.ini"
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
CLIENT_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
CONNECTION_TIMEOUT_SECONDS = 10


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
    RETURNS:    a populated ServerConfig; close on bad input
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
            ratelimit_disconnect=parser.getint(
                "ratelimit", "ratelimit_disconnect"),
        )
    except (configparser.Error, ValueError) as error:
        sys.exit(f"Bad config file {path}: {error}")

    if config.min_level not in LEVELS:
        sys.exit(f"min_level must be one of {LEVELS}")
    if not 1 <= config.port <= 65535:
        sys.exit("port must be between 1 and 65535")
    return config


class LoggingServer:
    """Accepts TCP clients and writes their log messages."""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.active_clients = 0

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """
        FUNCTION:   handle_client
        DESCRIPTION: Runs once per connection: Accepts into open connection (if any)
         then a read loop.
        """
        ip, port = writer.get_extra_info("peername")[:2]
        if self.active_clients >= self.config.max_clients:
            await self._reply(writer, "ERR server_full")
            await self._close(writer)
            print(f"Rejected {ip}:{port} (server full)")
            return

        self.active_clients += 1
        client_name = None
        try:
            client_name = await self.handle_client(reader, writer)
            if client_name is None:
                return
            print(f"Client '{client_name}' connected from {ip}:{port}")

            while True:
                line = await self._read_line(reader)
                if line is None:  # client disconnected
                    break
                if line == "ERR_TOO_LONG":
                    await self._reply(writer, "ERR line_too_long")
                    continue
                await self._reply(writer, "OK")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass  # abrupt disconnect - just clean up below
        finally:
            self.active_clients -= 1
            await self._close(writer)
            print(f"Client '{client_name or 'unknown'}' "
                  f"disconnected ({ip}:{port})")

    async def _handshake(self, reader, writer):
        """Expects 'HELLO' as the first line. Returns the name."""
        try:
            line = await asyncio.wait_for(self._read_line(reader),
                                          CONNECTION_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            await self._reply(writer, "ERR handshake_timeout")
            return None

        parts = (line or "").split(maxsplit=1)
        if (len(parts) != 2 or parts[0] != "HELLO"
                or not CLIENT_NAME.match(parts[1].strip())):
            await self._reply(writer, "ERR bad_format")
            return None

        await self._reply(writer, "OK")
        return parts[1].strip()

    async def _read_line(self, reader):
        """
        Returns the next line (stripped), None on disconnect, or an ERR message
        """
        try:
            raw = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError as error:
            await reader.readexactly(error.consumed)  # discard oversize data
            return "ERR_TOO_LONG"
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if len(text) > self.config.max_line:
            return "ERR_TOO_LONG"
        return text

    @staticmethod
    async def _reply(writer, text: str) -> None:
        writer.write((text + "\n").encode("utf-8"))
        await writer.drain()

    @staticmethod
    async def _close(writer) -> None:
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass

    async def run(self) -> None:
        """Starts listening and serves until interrupted."""
        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port,
            limit=self.config.max_line + 2)
        print(f"Logging service listening on "
              f"{self.config.host}:{self.config.port}")
        async with server:
            await server.serve_forever()


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

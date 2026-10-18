"""
FILE:         Logging_Server.py
PROJECT:      SENG2045 Assignment 2 - Logging Server Tool
PROGRAMMER:   Brad Kajganich - 90326321
FIRST VERSION: 2026-10-17
DESCRIPTION:  Accepts TCP clients, secures connections, parses LEVEL and MESSAGE
              passes to LogWriter. Each client is rate limited
              clients that keep exceeding limits it are disconnected.
"""

import asyncio
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from Log_Writer import LogWriter
from Server_Config import LEVELS, ServerConfig

CLIENT_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
CONNECTION_TIMEOUT = 10


class LineTooLong(Exception):
    """Raised when a client sends a line longer than max_line
    Prevents disconnecting for a line longer than max_line"""


@dataclass
class ClientState:
    """Everything the server tracks about one connected client."""
    name: str
    ip: str
    port: int
    recent: deque = field(default_factory=deque)  # times of accepted messages
    violations: int = 0          # rate-limit violations in the current run
    last_violation: float = 0.0  # time of the most recent violation
    burst_dropped: int = 0       # messages dropped in the current burst
    burst_started: float = 0.0   # time of the first drop in the current burst


class LoggingServer:
    """Accepts TCP clients and writes their log messages to the log file."""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.log_writer = LogWriter(config)
        self.active_clients = 0

    def _allow(self, client: ClientState) -> bool:
        """
        FUNCTION:   _allow
        DESCRIPTION: Sliding-window rate limit: a client may have at most
                    framelimit_messages accepted within framelimit_seconds.
                    Every excess message counts as a violation. Violations
                    are forgiven after a full quiet window.
        RETURNS:    True if the message may be processed
        """
        now = time.monotonic()
        frame = self.config.framelimit_seconds
        while client.recent and now - client.recent[0] >= frame:
            client.recent.popleft()

        if len(client.recent) < self.config.framelimit_messages:
            client.recent.append(now)
            self._end_burst(client)  # back under the limit: burst is over
            return True

        if client.burst_dropped == 0:
            client.burst_started = now
        client.burst_dropped += 1
        if now - client.last_violation > frame:
            client.violations = 0  # a full window passed without violations
        client.violations += 1
        client.last_violation = now
        return False

    def _end_burst(self, client: ClientState) -> None:
        """Writes ONE summary line for a finished burst of dropped messages.
        Server notices are always logged, whatever min_level is set to."""
        if client.burst_dropped == 0:
            return
        duration = client.last_violation - client.burst_started
        self.log_writer.write(
            client.name, client.ip, client.port, "WARN",
            f"rate limit exceeded: dropped {client.burst_dropped} message(s) "
            f"in {duration:.1f}s (limit {self.config.framelimit_messages} "
            f"per {self.config.framelimit_seconds}s)")
        client.burst_dropped = 0

    def _process_line(self, client_name: str, ip: str, port: int,
                      line: str) -> str:
        """
        FUNCTION:   _process_line
        DESCRIPTION: Parses '<LEVEL> <message>' and logs it.
        RETURNS:    the reply to send: OK, ERR bad_format, ERR bad_level
                    or ERR log_failed
        """
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            return "ERR bad_format"
        level = parts[0].upper()
        if level not in LEVELS:
            return "ERR bad_level"
        if not self.log_writer.is_wanted(level):
            return "OK"  # accepted, but below min_level so not stored
        if not self.log_writer.write(client_name, ip, port, level, parts[1]):
            return "ERR log_failed"
        return "OK"

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """
        FUNCTION:   handle_client
        DESCRIPTION: Runs once per connection: handshake, then a read loop.
        """
        ip, port = writer.get_extra_info("peername")[:2]
        if self.active_clients >= self.config.max_clients:
            await self._reply(writer, "ERR server_full")
            await self._close(writer)
            print(f"Rejected {ip}:{port} (server full)")
            return

        self.active_clients += 1
        client_name = None
        client = None
        try:
            client_name = await self._connect(reader, writer)
            if client_name is None:
                return
            client = ClientState(client_name, ip, port)
            print(f"Client '{client_name}' connected from {ip}:{port}")

            while True:
                too_long = False
                try:
                    line = await self._read_line(reader)
                except LineTooLong:
                    line, too_long = "", True
                if line is None:  # client disconnected
                    break

                # Every line counts towards the limit, valid or not.
                if not self._allow(client):
                    await self._reply(writer, "ERR rate_limited")
                    if client.violations >= self.config.ratelimit_disconnect:
                        self._end_burst(client)
                        self.log_writer.write(
                            client.name, ip, port, "WARN",
                            f"disconnecting client after {client.violations} "
                            f"rate-limit violations")
                        break
                    continue

                if too_long:
                    await self._reply(writer, "ERR line_too_long")
                else:
                    await self._reply(
                        writer, self._process_line(client.name, ip, port, line))
        except (ConnectionError, asyncio.IncompleteReadError):
            pass  # abrupt disconnect - just clean up below
        finally:
            if client is not None:
                self._end_burst(client)  # flush a burst still in progress
            self.active_clients -= 1
            await self._close(writer)
            print(f"Client '{client_name or 'unknown'}' "
                  f"disconnected ({ip}:{port})")

    async def _connect(self, reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter) -> Optional[str]:
        """Expects 'HELLO <client_name>' as the first line. Returns the name."""
        try:
            line = await asyncio.wait_for(self._read_line(reader),
                                          CONNECTION_TIMEOUT)
        except asyncio.TimeoutError:
            await self._reply(writer, "ERR handshake_timeout")
            return None
        except LineTooLong:
            await self._reply(writer, "ERR bad_format")
            return None

        parts = (line or "").split(maxsplit=1)
        if (len(parts) != 2 or parts[0] != "HELLO"
                or not CLIENT_NAME.match(parts[1].strip())):
            await self._reply(writer, "ERR bad_format")
            return None

        await self._reply(writer, "OK")
        return parts[1].strip()

    async def _read_line(self, reader: asyncio.StreamReader) -> Optional[str]:
        """
        Returns the next line (without the newline), or None if the client
        disconnected. Raises LineTooLong if the line exceeded the limit.
        """
        try:
            raw = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError as error:
            await reader.readexactly(error.consumed)  # discard oversize data
            await self._discard_rest_of_line(reader)
            raise LineTooLong() from error
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if len(text) > self.config.max_line:
            raise LineTooLong()
        return text

    @staticmethod
    async def _discard_rest_of_line(reader: asyncio.StreamReader) -> None:
        """Throws away data up to and including the next newline, so the
        tail of an oversize line is not mistaken for a new message."""
        while True:
            try:
                await reader.readuntil(b"\n")
                return
            except asyncio.LimitOverrunError as error:
                await reader.readexactly(error.consumed)
            except asyncio.IncompleteReadError:
                return  # client disconnected mid-line

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

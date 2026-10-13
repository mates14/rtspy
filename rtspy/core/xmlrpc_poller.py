"""
Background XML-RPC client for devices fronting a polled XML-RPC server.

Jan Fuchs' Perek 2m daemons (spectrograph, Quido relays, ...) all work the
same way: one method returns the whole state, a few others change it and
answer at once, and completion is only visible in the next state.  This
class owns one such server: a single thread polls it and executes queued
commands, so the RTS2 event loop never waits on the network and
xmlrpc.client's non-thread-safe transport is only touched from one place.
"""

import http.client
import logging
import queue
import threading
import time
import xmlrpc.client
from typing import Any, Callable, Optional, Tuple, Union


class _TimeoutTransport(xmlrpc.client.Transport):
    """Plain HTTP transport with a socket timeout, so a hung server fails a call."""

    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        if isinstance(conn, http.client.HTTPConnection):
            conn.timeout = self.timeout
        return conn


# Result callback: called on the poller thread with (ok, result_or_exception)
ResultCallback = Callable[[bool, Any], None]


class XmlRpcPoller:
    """
    Poll the server every `poll_interval` seconds; run queued calls first.

    `poll` is the name of the method returning the state, or a callable
    taking the ServerProxy, for servers whose state takes several calls.

    The status callback gets the poll result, or None when the server could
    not be reached (reported once per outage in the log, not once per poll).
    """

    def __init__(self, name: str, host: str, port: int,
                 poll: Union[str, Callable[[xmlrpc.client.ServerProxy], Any]],
                 poll_interval: float = 1.0, timeout: float = 5.0):
        self.name = name
        self.url = "http://%s:%d" % (host, port)
        self.poll = poll
        self.poll_interval = poll_interval
        self.timeout = timeout

        self.proxy: Optional[xmlrpc.client.ServerProxy] = None
        self.connected = False
        self.failure_reported = False
        self.status_callback: Optional[Callable[[Any], None]] = None

        self.commands: "queue.Queue[Tuple[str, tuple, Optional[ResultCallback]]]" = queue.Queue()
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, name="%s-xmlrpc" % self.name,
                                       daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=self.poll_interval + self.timeout * 2)
        self.thread = None

    def set_status_callback(self, callback: Callable[[Any], None]):
        self.status_callback = callback

    def is_connected(self) -> bool:
        return self.connected

    def call(self, method: str, *args, on_result: Optional[ResultCallback] = None):
        """Queue a server call.  Returns at once; on_result, if given, gets the outcome."""
        self.commands.put((method, args, on_result))

    def _loop(self):
        while self.running:
            started = time.time()

            if self.proxy is None:
                # Constructing the proxy does no I/O; a dead server shows up
                # on the first real call
                self.proxy = xmlrpc.client.ServerProxy(
                    self.url, allow_none=True, transport=_TimeoutTransport(self.timeout))

            try:
                self._drain_commands()
                if callable(self.poll):
                    values = self.poll(self.proxy)
                else:
                    values = getattr(self.proxy, self.poll)()

                if not self.connected:
                    logging.info("%s server %s is responding", self.name, self.url)
                    self.connected = True
                    self.failure_reported = False

                self._report(values)

            except Exception as exc:
                if not self.failure_reported:
                    logging.error("%s server %s unreachable: %s", self.name, self.url, exc)
                    self.failure_reported = True
                self.connected = False
                # A fresh transport next time, not a broken keep-alive socket
                self.proxy = None
                self._fail_queued(exc)
                self._report(None)

            elapsed = time.time() - started
            if self.running and elapsed < self.poll_interval:
                time.sleep(self.poll_interval - elapsed)

    def _report(self, values):
        if self.status_callback is None:
            return
        try:
            self.status_callback(values)
        except Exception as exc:
            logging.error("Error in %s status callback: %s", self.name, exc, exc_info=True)

    def _drain_commands(self):
        """Run every queued call, oldest first.  A transport failure propagates."""
        while True:
            try:
                method, args, on_result = self.commands.get_nowait()
            except queue.Empty:
                return

            try:
                result = getattr(self.proxy, method)(*args)
            except xmlrpc.client.Fault as fault:
                # The server answered: the connection is fine, the call is not
                logging.error("%s %s%s failed: %s", self.name, method, args, fault)
                self._notify(on_result, False, fault)
                continue
            except Exception as exc:
                logging.error("%s %s%s failed: %s", self.name, method, args, exc)
                self._notify(on_result, False, exc)
                raise

            logging.debug("%s %s%s => %r", self.name, method, args, result)
            self._notify(on_result, True, result)

    def _fail_queued(self, exc):
        """The server is gone: fail what is queued rather than run it at reconnect."""
        while True:
            try:
                method, args, on_result = self.commands.get_nowait()
            except queue.Empty:
                return
            logging.error("%s %s%s dropped: server unreachable", self.name, method, args)
            self._notify(on_result, False, exc)

    def _notify(self, on_result, ok, result):
        if on_result is None:
            return
        try:
            on_result(ok, result)
        except Exception as exc:
            logging.error("Error in %s result callback: %s", self.name, exc, exc_info=True)

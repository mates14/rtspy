"""
RTS2 daemon startup: locking, backgrounding, privileges and signals.

This gives an rtspy driver the same startup contract a C++ RTS2 daemon has,
so `rts2-start`, `rts2-stop` and `rts2@.service` can drive it directly with
no shell wrapper in between. The contract, read out of
`base/packaging/rts2-start.in` and `base/kernel/src/daemon.cpp`:

  * The lock file `/var/run/rts2_<device>` is held with flock() for the
    process lifetime, and carries the pid. "Is it running" is answered by
    the flock, never by the pid - a pid goes stale, a kernel lock cannot.
  * Exit 255 means the lock is held by another instance, 254 means the lock
    file cannot be used at all, 0 means the daemon is up, anything else
    means startup failed and rts2-start prints the captured stderr.
  * The process forks early but the parent does not exit until the child
    reports, over a pipe held across the fork, that it finished
    initialising. That is what makes the exit status mean something, and
    what systemd's Type=forking assumes.
  * -i skips the fork but NOT the lock: an interactive start of an
    already-running daemon still exits 255.

The fork has to happen before any thread exists. Forking a process that has
already started threads hands the child mutexes locked by threads that do
not exist in it - which is why App resolves configuration, takes the lock
and forks before it constructs the device.

Full write-up in docs/daemonising-rtspy.md.
"""

import errno
import fcntl
import grp
import logging
import os
import pwd
import select
import signal
import sys
import syslog
import threading
import time

# see RTS2_EXIT_* in base/kernel/include/daemon.h
EXIT_ALREADY_RUNNING = 255
EXIT_LOCK_ERROR = 254

# matches RTS2_LOCK_PREFIX in the C++ build
DEFAULT_LOCK_PREFIX = "/var/run/rts2_"

DEFAULT_DAEMONIZE_TIMEOUT = 120


class LockFile:
    """
    The daemon's flock-based mutual exclusion.

    Taken before the fork, inherited across it. Because the lock lives on
    the open file description rather than the descriptor, the child holds
    the very same lock and the parent's copy closing on exit does not
    release it.
    """

    def __init__(self, path):
        self.path = path
        self.fd = None

    def acquire(self):
        """
        Create and flock the lock file.

        Returns 0 on success, -1 if another instance holds it, -2 if the
        file cannot be used at all.
        """
        old_mask = os.umask(0o022)
        try:
            fd = self._open()
        except OSError as exc:
            logging.error("cannot create lock file %s: %s - do you have correct "
                          "permission? Try running as root (sudo,..)", self.path, exc)
            return -2
        finally:
            os.umask(old_mask)

        if fd is None:
            return -2

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                logging.error("lock file %s owned by another process", self.path)
                return -1
            logging.error("cannot flock %s: %s", self.path, exc)
            return -2

        self.fd = fd
        return 0

    def _open(self):
        try:
            return os.open(self.path, os.O_RDWR | os.O_CREAT, 0o666)
        except FileNotFoundError:
            # A --lock-prefix pointing below /run or /tmp is normal - that is
            # how a second, out-of-tree daemon runs beside a packaged one -
            # but those are tmpfs and are gone after a reboot. Create the
            # directory rather than calling it a lock error.
            directory = os.path.dirname(self.path)
            if not directory:
                raise
            try:
                os.mkdir(directory, 0o755)
            except FileExistsError:
                pass
            return os.open(self.path, os.O_RDWR | os.O_CREAT, 0o666)

    def write_pid(self):
        """Record our pid. Done after the fork, so it is the daemon's own."""
        if self.fd is None:
            return
        try:
            os.ftruncate(self.fd, 0)
            os.lseek(self.fd, 0, os.SEEK_SET)
            os.write(self.fd, b"%d\n" % os.getpid())
            os.fsync(self.fd)
        except OSError as exc:
            logging.error("cannot write pid to lock file %s: %s", self.path, exc)

    def release(self):
        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None


def lock_path_for(device_name, lock_prefix=None):
    """Where this device's lock file lives."""
    return "%s%s" % (lock_prefix or DEFAULT_LOCK_PREFIX, device_name)


# --------------------------------------------------------------------------
# backgrounding
# --------------------------------------------------------------------------

_notify_fd = None


def do_daemonize(timeout=DEFAULT_DAEMONIZE_TIMEOUT):
    """
    Fork into the background, keeping a pipe back to the parent.

    The parent blocks in `_wait_for_child` and exits with a status that says
    whether the daemon actually came up. The child returns from here and
    carries on initialising, with stdin/stdout/stderr still attached to the
    starting terminal so anything it logs on the way up is seen; it calls
    `daemonize_ready()` when it is up, and only then lets go of the console.

    Must be called before any thread is created.
    """
    global _notify_fd

    if threading.active_count() > 1:
        # Not fatal, but it means someone reordered startup and the child is
        # about to inherit locks held by threads that do not exist in it.
        logging.warning("forking with %d threads already running - "
                        "the daemon may deadlock", threading.active_count())

    read_fd, write_fd = os.pipe()
    # keep the handshake out of anything we later exec()
    os.set_inheritable(read_fd, False)
    os.set_inheritable(write_fd, False)

    try:
        pid = os.fork()
    except OSError as exc:
        logging.error("cannot fork to background: %s", exc)
        sys.exit(6)

    if pid:
        os.close(write_fd)
        os._exit(_wait_for_child(pid, read_fd, timeout))

    os.close(read_fd)
    _notify_fd = write_fd
    os.setsid()


def _wait_for_child(pid, notify_fd, timeout):
    """
    Block until the forked daemon reports the outcome of its initialisation,
    and turn that into an exit status for the process the shell is waiting on.

    One byte means it is up. EOF without a byte means it died on the way -
    reap it and report its real status. Nothing at all within the timeout is
    not failure: slow hardware is not broken hardware, so leave it running
    and say so.
    """
    poller = select.poll()
    poller.register(notify_fd, select.POLLIN)

    while True:
        try:
            events = poller.poll(timeout * 1000 if timeout and timeout > 0 else None)
        except InterruptedError:
            continue
        except OSError as exc:
            print("cannot wait for daemon initialisation: %s" % exc, file=sys.stderr)
            return 1
        break

    if not events:
        print("daemon is still initialising after %ds, leaving it running in background"
              % timeout, file=sys.stderr)
        return 0

    try:
        if len(os.read(notify_fd, 1)) == 1:
            return 0
    except OSError:
        pass

    # EOF without a byte - initialisation failed. The daemon has already
    # logged why, to syslog and to the stderr we share with it. Poll for its
    # status rather than blocking: with a restart watchdog the process we
    # forked outlives the failed attempt, and waitpid() would hang forever.
    for _ in range(50):
        try:
            waited, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            break
        if waited == pid:
            if os.WIFEXITED(status):
                return os.WEXITSTATUS(status) or 1
            if os.WIFSIGNALED(status):
                return 128 + os.WTERMSIG(status)
            return 1
        time.sleep(0.02)

    print("daemon failed to complete initialisation", file=sys.stderr)
    return 1


def daemonize_ready():
    """
    Tell the waiting parent that everything which can fail has succeeded,
    then let go of the console. Safe to call when not daemonized.
    """
    global _notify_fd

    if _notify_fd is None:
        return

    try:
        os.write(_notify_fd, b"\0")
    except (BrokenPipeError, OSError):
        # the parent gave up waiting - harmless
        pass
    try:
        os.close(_notify_fd)
    except OSError:
        pass
    _notify_fd = None

    detach_from_console()


def detach_from_console():
    """Point the standard descriptors at /dev/null."""
    try:
        devnull = os.open(os.devnull, os.O_RDWR)
    except OSError as exc:
        logging.error("cannot open %s: %s", os.devnull, exc)
        return
    for fd in (0, 1, 2):
        try:
            os.dup2(devnull, fd)
        except OSError:
            pass
    if devnull > 2:
        os.close(devnull)


def is_daemonized():
    """True while a parent is still waiting for our readiness report."""
    return _notify_fd is not None


# --------------------------------------------------------------------------
# privileges
# --------------------------------------------------------------------------

def drop_privileges(spec):
    """
    Drop to the user (and optionally group) named by --run-as.

    Accepts "user" or "user.group", matching the C++ option. Returns True on
    success; on failure it logs why and returns False, and the caller should
    treat that as a failed startup - silently carrying on as root is worse
    than not starting.
    """
    if not spec:
        return True

    if "." in spec:
        user_name, group_name = spec.split(".", 1)
    else:
        user_name, group_name = spec, None

    try:
        user = pwd.getpwnam(user_name)
    except KeyError:
        logging.error("--run-as: no such user '%s'", user_name)
        return False

    if group_name:
        try:
            gid = grp.getgrnam(group_name).gr_gid
        except KeyError:
            logging.error("--run-as: no such group '%s'", group_name)
            return False
    else:
        gid = user.pw_gid

    if os.geteuid() != 0:
        if os.geteuid() == user.pw_uid:
            return True
        logging.error("--run-as %s: not running as root, cannot change user", spec)
        return False

    try:
        os.initgroups(user_name, gid)
        os.setgid(gid)
        os.setuid(user.pw_uid)
    except OSError as exc:
        logging.error("--run-as %s failed: %s", spec, exc)
        return False

    os.environ["HOME"] = user.pw_dir
    os.environ["USER"] = user_name
    logging.info("running as %s (uid %d, gid %d)", user_name, user.pw_uid, gid)
    return True


# --------------------------------------------------------------------------
# signals
# --------------------------------------------------------------------------

class ShutdownRequest:
    """
    Bridges a signal into the main loop.

    rts2-stop sends SIGTERM and then waits; Python's default action would
    terminate the interpreter immediately, so the device's stop() - and with
    it every hardware thread, serial port and socket - would never run.
    """

    def __init__(self):
        self.event = threading.Event()
        self.signum = None

    def install(self):
        signal.signal(signal.SIGTERM, self._terminate)
        signal.signal(signal.SIGINT, self._terminate)
        signal.signal(signal.SIGHUP, self._hup)
        # a peer closing a socket must not kill the daemon
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    def _terminate(self, signum, frame):
        if self.event.is_set():
            # second signal while already shutting down - go now
            os._exit(1)
        self.signum = signum
        self.event.set()

    def _hup(self, signum, frame):
        # C++ reloads its configuration here; rtspy has no equivalent yet.
        logging.warning("SIGHUP received - configuration reload is not implemented")

    def wait(self):
        """Block until a termination signal arrives."""
        while not self.event.wait(0.5):
            pass
        return self.signum


# --------------------------------------------------------------------------
# logging
# --------------------------------------------------------------------------

_SYSLOG_PRIORITY = {
    logging.CRITICAL: syslog.LOG_CRIT,
    logging.ERROR: syslog.LOG_ERR,
    logging.WARNING: syslog.LOG_WARNING,
    logging.INFO: syslog.LOG_INFO,
    logging.DEBUG: syslog.LOG_DEBUG,
}


class Rts2SyslogHandler(logging.Handler):
    """
    Syslog under the shared ident "rts2", the way every C++ daemon does.

    The point of the shared ident is the shipped rsyslog rule, which matches
    programname == 'rts2' and routes the whole observatory into
    /var/log/rts2.log. Python's own SysLogHandler cannot set an ident, so
    this goes through the libc syslog(3) binding instead - which also means
    the state survives fork() exactly as it does in C++.
    """

    def __init__(self, ident="rts2"):
        super().__init__()
        syslog.openlog(ident, syslog.LOG_PID, syslog.LOG_DAEMON)

    def emit(self, record):
        try:
            priority = _SYSLOG_PRIORITY.get(record.levelno, syslog.LOG_INFO)
            syslog.syslog(priority, self.format(record))
        except Exception:
            self.handleError(record)

    def close(self):
        try:
            syslog.closelog()
        finally:
            super().close()

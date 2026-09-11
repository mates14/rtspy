import os
import sys
import argparse
import inspect
import logging
import logging.handlers
import time
from typing import Dict, List, Optional, Type, Any
from datetime import datetime, timezone
from pathlib import Path

from rtspy.core.device import Device
from rtspy.core import daemon as rts2daemon

# Custom formatter class to handle the specific format you want
class RTS2LogFormatter(logging.Formatter):
    # set once the configuration is resolved, before any Device is built
    device_name = None

    def format(self, record):
        # Convert level names to single letters
        level_map = {
            'DEBUG': 'D',
            'INFO': 'I',
            'WARNING': 'W',
            'ERROR': 'E',
            'CRITICAL': 'C'
        }

        # Prefer the name resolved from the configuration: the daemon logs
        # (lock errors, privilege failures) before any Device exists, and
        # "UNKNOWN" in those lines is exactly where the name is wanted most.
        from rtspy.core.device import Device
        device = Device.get_instance()
        device_name = getattr(device, 'device_name', None) if device else None
        if not device_name:
            device_name = RTS2LogFormatter.device_name or 'UNKNOWN'

        # Format timestamp in UTC
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        # Get the single letter level
        level = level_map.get(record.levelname, '?')

        # Format the message
        formatted_msg = f"{timestamp} UTC {device_name} {level} {record.getMessage()}"
        return formatted_msg

class App:
    """Lightweight application launcher for RTS2 device drivers."""

    def __init__(self, description: str = "RTS2 Device"):
        """Initialize the application framework."""
        self.parser = argparse.ArgumentParser(description=description)
        self.args = None
        self.device = None
        self.registry = None
        self.config = None
        self.lock = None
        self.shutdown = None

    def register_device_options(self, device_class: Type[Device]):
        """
        Register device options using the DeviceConfig system.

        The device class handles both standard RTS2 options and device-specific options.

        Args:
            device_class: Device class that uses DeviceConfig
        """
        # Check if device class uses DeviceConfig
        from rtspy.core.config import DeviceConfig
        if not issubclass(device_class, DeviceConfig):
            raise RuntimeError(f"Device class {device_class.__name__} must inherit from DeviceConfig")

        # Use the DeviceConfig system to register options
        device_class.register_options(self.parser)

    def parse_args(self):
        """Parse command line arguments."""
        self.args = self.parser.parse_args()

        # Basic logging setup will be refined later by device configuration processing
        # We only do minimal setup here to avoid interference with device config
        self._setup_basic_logging()

        return self.args

    def _setup_basic_logging(self):
        """
        Set up basic logging before device configuration processing.

        This provides minimal logging capability until the device's configuration
        system takes over and applies the full logging configuration.
        """
        # Very basic setup - device config will override this
        if hasattr(self.args, 'verbose') and self.args.verbose:
            level = logging.DEBUG
        elif hasattr(self.args, 'debug') and self.args.debug:
            level = logging.DEBUG
        else:
            level = logging.INFO

        logging.basicConfig(level=level, format='%(levelname)s: %(message)s')

    def create_device(self, device_class: Type[Device], **kwargs):
        """
        Create device instance and apply configuration.

        Args:
            device_class: Device class to instantiate
            **kwargs: Additional parameters for device constructor

        Returns:
            Configured device instance
        """
        # Check if device class uses DeviceConfig
        from rtspy.core.config import DeviceConfig
        if not issubclass(device_class, DeviceConfig):
            raise RuntimeError(f"Device class {device_class.__name__} must inherit from DeviceConfig")

        # Extract basic device parameters from args and kwargs
        # Note: These might be overridden by the configuration system
        device_name = kwargs.get('device_name') or getattr(self.args, 'device', None)
        port = kwargs.get('port') or getattr(self.args, 'port', 0)

        # Create device instance with basic parameters
        self.device = device_class(device_name=device_name, port=port)

        # Apply configuration from all sources using DeviceConfig system.
        # A daemon has already resolved this before forking (it had to know
        # what to lock), so reuse it rather than parsing everything twice.
        device_class.process_args(self.device, self.args,
                                  self.registry, self.config)

        # At this point, logging configuration has been applied by the device config system
        # so we need to reconfigure logging with the proper RTS2 formatter
        self._setup_rts2_logging()

        # Start device network and device itself
        self.device.network.start()
        self.device.start()

        return self.device

    def _setup_rts2_logging(self):
        """
        Set up RTS2-style logging with multiple outputs after device configuration is processed.

        Outputs:
        1. Console (always)
        2. File (device config path OR fallback to /var/log or ~/log)
        3. Syslog (system integration)
        """
        # Create RTS2 formatter
        formatter = RTS2LogFormatter()

        # Syslog formatter (no timestamp - syslog adds it)
        device_name = getattr(self.device, 'device_name', 'UNKNOWN') if self.device else 'UNKNOWN'
        syslog_formatter = logging.Formatter(f'{device_name} %(levelname)s %(message)s')

        # Get current log level (set by device configuration)
        current_level = logging.getLogger().level

        # Remove any existing handlers to avoid duplicate logs
        root_logger = logging.getLogger()
        for hdlr in root_logger.handlers[:]:
            root_logger.removeHandler(hdlr)

        root_logger.setLevel(current_level)

        handlers_added = []

        # 1. CONSOLE HANDLER. stderr rather than stdout on purpose: while a
        # daemon is initialising, rts2-start captures its stderr and prints
        # it under the failure line, so this is what turns "FAILED (exit 12)"
        # into a message saying why.
        try:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(current_level)
            root_logger.addHandler(console_handler)
            handlers_added.append("console")
        except Exception as e:
            print(f"Warning: Could not setup console handler: {e}", file=sys.stderr)

        # 2. FILE HANDLER with smart fallback
        log_file = None

        # First try device configuration
        if self.device and hasattr(self.device, '_resolved_config'):
            logging_config = self.device._resolved_config.get('logging', {})
            config_file = logging_config.get('file')
            if config_file:
                try:
                    # Test if we can write to the configured path
                    with open(config_file, 'a') as f:
                        pass
                    log_file = config_file
                except (PermissionError, OSError):
                    print(f"Warning: Cannot write to configured log file {config_file}, using fallback", file=sys.stderr)

        # Fallback file path logic
        if not log_file:
            # Try system log directory first
            try:
                system_log_dir = Path("/var/log")
                if system_log_dir.exists() and os.access(system_log_dir, os.W_OK):
                    log_file = system_log_dir / "rts2-debug.log"
                    # Test write access
                    with open(log_file, 'a') as f:
                        pass
            except (PermissionError, OSError):
                log_file = None

            # Fall back to user home directory
            if not log_file:
                try:
                    home_dir = Path.home()
                    user_log_dir = home_dir / "log"
                    user_log_dir.mkdir(exist_ok=True)
                    log_file = user_log_dir / "rts2-debug.log"
                except OSError:
                    log_file = None

        # Create file handler if we have a path
        if log_file:
            try:
                # Use rotating file handler to prevent huge log files
                file_handler = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=10*1024*1024,  # 10MB max file size
                    backupCount=5,          # Keep 5 backup files
                    encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                file_handler.setLevel(current_level)
                root_logger.addHandler(file_handler)
                handlers_added.append(f"file({log_file})")
            except Exception as e:
                print(f"Warning: Could not setup file handler: {e}", file=sys.stderr)

        # 3. SYSLOG HANDLER, under the shared ident "rts2" that every C++
        # RTS2 daemon uses. That ident is not cosmetic: the shipped rsyslog
        # rule matches programname == 'rts2' and routes the whole
        # observatory into /var/log/rts2.log, so a daemon logging under any
        # other name is simply missing from the site log.
        try:
            syslog_handler = rts2daemon.Rts2SyslogHandler("rts2")
            syslog_handler.setFormatter(syslog_formatter)
            syslog_handler.setLevel(current_level)
            root_logger.addHandler(syslog_handler)
            handlers_added.append("syslog(rts2)")
        except Exception as e:
            print(f"Warning: Could not setup syslog handler: {e}", file=sys.stderr)

        # Configure noisy module loggers
        logging.getLogger('kafka').setLevel(logging.WARNING)
        logging.getLogger('gcn_kafka').setLevel(logging.INFO)
        logging.getLogger('psycopg2').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)

        # Log successful setup
        if handlers_added:
            logging.info(f"RTS2 logging configured: {', '.join(handlers_added)} (level: {logging.getLevelName(current_level)})")
        else:
            logging.warning("No logging handlers could be configured!")


    def run(self):
        """
        Run the application main loop.

        Nothing happens here - the device lives on its network and hardware
        threads. This just holds the main thread until a signal arrives, so
        that rts2-stop's SIGTERM reaches device.stop() instead of killing
        the interpreter out from under it.
        """
        if not self.device:
            raise RuntimeError("Device not created - call create_device() first")

        try:
            if self.shutdown is not None:
                signum = self.shutdown.wait()
                logging.info("received signal %d, shutting down", signum)
            else:
                # not started through main() - no handlers installed
                while True:
                    time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            if self.device:
                self.device.stop()
            if self.lock:
                self.lock.release()

    # ------------------------------------------------------------------
    # the daemon entry point
    # ------------------------------------------------------------------

    def main(self, device_class: Type[Device]) -> int:
        """
        Start a driver the way a C++ RTS2 daemon starts.

        Order matters throughout and mirrors Daemon::init()/run() - see
        docs/daemonising-rtspy.md. In particular the configuration is
        resolved, the lock taken and the fork done before the device exists,
        because every one of those needs to happen while the process is
        still single-threaded.

        Returns a process exit status; drivers should sys.exit() it.
        """
        self.register_device_options(device_class)
        self.args = self.parser.parse_args()

        # 1. resolve the configuration with no device and no threads
        self.registry, self.config = device_class.resolve_config(self.args)

        if self.config.get('show_config', False):
            print(self.registry.format_config_summary(self.config))
            return 0

        device_name = self._resolve_device_name(device_class)
        if device_name is None:
            return 1
        RTS2LogFormatter.device_name = device_name

        self._setup_early_logging()

        for opt in self.registry.unimplemented_options(self.config):
            logging.warning("%s is accepted for compatibility but does nothing", opt)

        # 2. take the lock. -i does not skip this: an interactive start of a
        #    daemon that is already running is still a duplicate.
        lock_path = rts2daemon.lock_path_for(device_name,
                                             self.config.get('lock_prefix'))
        self.lock = rts2daemon.LockFile(lock_path)
        ret = self.lock.acquire()
        if ret == -1:
            return rts2daemon.EXIT_ALREADY_RUNNING
        if ret < 0:
            return rts2daemon.EXIT_LOCK_ERROR

        # 3. fork, keeping a pipe back to the process the shell waits on
        if not self.config.get('interactive'):
            rts2daemon.do_daemonize(self.config.get('daemonize_timeout', 120))

        # 4. drop privileges, then record the pid we ended up with
        if not rts2daemon.drop_privileges(self.config.get('run_as')):
            return 1
        self.lock.write_pid()

        # 5. signal handlers, before anything can need stopping
        self.shutdown = rts2daemon.ShutdownRequest()
        self.shutdown.install()

        # 6. everything that can fail. Until daemonize_ready() below, stderr
        #    is still the terminal that started us, so whatever goes wrong
        #    here is what rts2-start prints under its failure line.
        try:
            self.create_device(device_class)
        except Exception as exc:
            logging.error("cannot start %s: %s", device_name, exc, exc_info=True)
            return 12

        # 7. up. Tell the waiting parent and let go of the console.
        rts2daemon.daemonize_ready()
        logging.info("%s started", device_name)

        self.run()
        return 0

    def _resolve_device_name(self, device_class: Type[Device]) -> Optional[str]:
        """
        Work out what to lock, before the device that knows its own name exists.

        -d wins; otherwise fall back to the driver's own default, which is
        the device_name default in its __init__ signature (F0, W0, ...).
        """
        name = self.config.get('device')
        if name:
            return name

        try:
            default = inspect.signature(device_class.__init__) \
                .parameters['device_name'].default
        except (ValueError, KeyError):
            default = None

        if default and default is not inspect.Parameter.empty:
            return default

        print("%s: no device name - pass -d <name>" % self.parser.prog,
              file=sys.stderr)
        return None

    def _setup_early_logging(self):
        """
        Minimal stderr logging for the pre-fork stage.

        Lock failures and privilege errors happen before the real logging is
        configured and before any device exists; without this they would be
        invisible in exactly the case rts2-start most needs to explain.
        """
        level = logging.INFO
        if self.config.get('debug'):
            level = logging.DEBUG
        elif self.config.get('verbose'):
            level = logging.INFO

        root = logging.getLogger()
        for handler in root.handlers[:]:
            root.removeHandler(handler)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(RTS2LogFormatter())
        handler.setLevel(level)
        root.addHandler(handler)
        root.setLevel(level)

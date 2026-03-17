import os
import sys
import argparse
import logging
import logging.handlers
import time
from typing import Dict, List, Optional, Type, Any
from datetime import datetime, timezone
from pathlib import Path

from rtspy.core.device import Device

# Custom formatter class to handle the specific format you want
class RTS2LogFormatter(logging.Formatter):
    def format(self, record):
        # Convert level names to single letters
        level_map = {
            'DEBUG': 'D',
            'INFO': 'I',
            'WARNING': 'W',
            'ERROR': 'E',
            'CRITICAL': 'C'
        }

        # Get the device name from the Device singleton if available
        from rtspy.core.device import Device
        device = Device.get_instance()
        device_name = getattr(device, 'device_name', 'UNKNOWN') if device else 'UNKNOWN'

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

        # Apply configuration from all sources using DeviceConfig system
        device_class.process_args(self.device, self.args)

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

        # 1. CONSOLE HANDLER (always present for development)
        try:
            console_handler = logging.StreamHandler(sys.stdout)
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

        # 3. SYSLOG HANDLER for system integration
        try:
            # Try different syslog paths
            syslog_paths = [
                '/dev/log',        # Most Linux systems
                '/var/run/syslog', # Some systems
                ('localhost', 514) # UDP fallback
            ]

            syslog_handler = None
            for path in syslog_paths:
                try:
                    if isinstance(path, tuple):
                        syslog_handler = logging.handlers.SysLogHandler(address=path)
                    else:
                        syslog_handler = logging.handlers.SysLogHandler(address=path)
                    break
                except (OSError, ConnectionRefusedError):
                    continue

            if syslog_handler:
                syslog_handler.setFormatter(syslog_formatter)
                syslog_handler.setLevel(current_level)
                # Use local0 facility for custom applications
                syslog_handler.facility = logging.handlers.SysLogHandler.LOG_LOCAL0
                root_logger.addHandler(syslog_handler)
                handlers_added.append("syslog")

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
        """Run the application main loop."""
        if not self.device:
            raise RuntimeError("Device not created - call create_device() first")

        try:
            # Main application loop
            while True:
                time.sleep(10)

        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            if self.device:
                self.device.stop()

import argparse
import logging
import time
from typing import Dict, List, Optional, Type, Any
from datetime import datetime, timezone

from device import Device

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
        from device import Device
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
        from config import DeviceConfig
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
        from config import DeviceConfig
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
        Set up RTS2-style logging after device configuration is processed.

        The device configuration system has already set the log level,
        now we apply the RTS2 formatter.
        """
        # Create RTS2 formatter
        formatter = RTS2LogFormatter()

        # Get current log level (set by device configuration)
        current_level = logging.getLogger().level

        # Determine log file from device configuration if available
        log_file = None
        if self.device and hasattr(self.device, '_resolved_config'):
            logging_config = self.device._resolved_config.get('logging', {})
            log_file = logging_config.get('file')

        # Create appropriate handler
        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler()

        handler.setFormatter(formatter)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(current_level)

        # Remove any existing handlers to avoid duplicate logs
        for hdlr in root_logger.handlers[:]:
            root_logger.removeHandler(hdlr)

        root_logger.addHandler(handler)

        logging.info(f"RTS2 logging configured (level: {logging.getLevelName(current_level)})")

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

# RTS2-Python

A Python implementation of the RTS2 (Remote Telescope System 2nd Version) device drivers framework. This project provides a complete Python-based alternative to the C++ RTS2 components, focusing on ease of development and extensibility while maintaining full protocol compatibility with the original system.

## Overview

RTS2 (Remote Telescope System 2nd Version) is an open-source observatory control system designed for robotic observatories. This Python implementation provides:

- Complete RTS2 device driver (daemon) network protocol support
- Unified configuration system with command line, config files, and environment variables
- Device abstraction framework with mixin architecture for multi-function devices
- Type-safe value system with automatic network distribution
- Command handling with automatic registration and dispatch
- Device-to-device communication with automatic connection management
- Production-ready implementations for filter wheels, focusers, and GRB detection

The framework is designed to work seamlessly within existing RTS2 installations, connecting to centrald and communicating with other RTS2 components.

## Installation

### From PyPI (when published)

```bash
pip install rtspy
```

### Development Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/rts2-python.git
cd rtspy

# Install in development mode
pip install -e .
```

### Legacy Installation (for existing deployments)

If you need to use the old flat structure:

```bash
# Clone the repository
git clone https://github.com/yourusername/rts2-python.git
cd rtspy

# Install dependencies manually
pip install psycopg2-binary gcn-kafka astropy pyserial
```

## Quick Start

Run the example dummy filter wheel to test the system:

```bash
# After pip installation, use entry points:
rtspy-filterd-dummy -d W0 -p 0 --filters "J:H:K:R:G:B"

# Or in development mode without pip install:
python rtspy/drivers/filterd_dummy.py -d W0 -p 0 --filters "J:H:K:R:G:B"
```

## Available Entry Points

After installation with pip, the following commands are available:

**Production Daemons:**
- `rtspy-grbd` - GRB detection daemon with GCN Kafka interface
- `rtspy-queue-selector` - Queue-based target selector
- `rtspy-watcher` - Device monitoring daemon

**Test/Dummy Devices:**
- `rtspy-filterd-dummy` - Simulated filter wheel
- `rtspy-focusd-dummy` - Simulated focuser
- `rtspy-sensor-temp` - Example temperature sensor

**Hardware Drivers:**
- `rtspy-filterd-ovis` - OVIS spectrograph filter wheel driver

**CLI Utilities:**
- `rtspy-value` - Get/set device values via network protocol
- `rtspy-grbinfo` - Query GRB information from database
- `rtspy-queuemanual` - Manual queue management

**Legacy RTS2 Wrappers:**
- `rts2-gcnkafka` - Daemon wrapper for rtspy-grbd (shell script in rtspy/scripts/)
- `rts2-queuer` - Daemon wrapper for rtspy-queue-selector (shell script in rtspy/scripts/)

All commands support `--help` for detailed usage information.

## Experimental Code

The `experimental/` directory contains work-in-progress components that are not included in the package:
- `teld/` - Telescope drivers (incomplete)
- `imgprocd*.py` - Image processing daemon variants (experimental)

## Architecture

The system is built around several core components:

- **App**: Application framework handling configuration, command line parsing, and device lifecycle
- **Device**: Base device class with integrated network communication and configuration
- **DeviceConfig**: Unified configuration system supporting multiple sources with proper precedence
- **NetworkManager**: Handles all network communication, authentication, and automatic device discovery
- **Value**: Type-safe values with change tracking, callbacks, and automatic distribution
- **Mixins**: Reusable device functionality components for filters, focusers, and other instruments

## Supported Devices

### Filter Wheels
- **FilterMixin**: Complete filter wheel functionality as a mixin class
- **Filterd**: Standalone filter wheel base class
- **DummyFilter**: Simulated filter wheel for testing
- **OvisMultiFunction**: Real hardware driver for OVIS spectrograph (combines filter + focuser)

### Focusers
- **FocuserMixin**: Complete focuser functionality with temperature compensation
- **Focusd**: Standalone focuser base class with position control and limits
- **DummyFocuser**: Simulated focuser with gradual movement simulation

### Observatory Control
- **GrbDaemon**: Production GRB detection daemon with modern GCN Kafka interface
- **TemperatureSensor**: Example environmental monitoring device
- **WatcherDevice**: Device monitoring and inter-device communication example

### Multi-Function Devices
The mixin architecture allows combining multiple instrument types in a single device:

```python
class MultiInstrument(Device, FilterMixin, FocuserMixin):
    def setup_config(self, config):
        self.setup_filter_config(config)
        self.setup_focuser_config(config)

    def apply_config(self, config):
        super().apply_config(config)
        self.apply_filter_config(config)
        self.apply_focuser_config(config)
```

## Configuration System

The unified configuration system supports multiple sources with proper precedence:

1. Built-in defaults
2. System config files (`/etc/rts2/rts2.conf`)
3. User config files (`~/.rts2/rts2.conf`)
4. Explicit config files (`--config myconfig.conf`)
5. Environment variables (`RTS2_DEVICE_VARIABLE`)
6. Command line arguments (highest priority)

Example device with custom configuration:

```python
class MyDevice(Device, DeviceConfig):
    def setup_config(self, config):
        config.add_argument('--my-option', help='Custom option')
        config.add_argument('--my-port', type=int, default=8080,
                           section='hardware', help='Hardware port')

    def apply_config(self, config):
        super().apply_config(config)
        self.my_option = config.get('my_option')
        self.my_port = config.get('my_port')
```

## Device-to-Device Communication

Devices can automatically establish connections and monitor other devices:

```python
# Monitor another device's values
self.network.register_interest_in_value(
    device_name="TEMP",
    value_name="temperature",
    callback=self._on_temperature_update
)

# Monitor device state changes
self.network.register_state_interest(
    device_name="CCD1",
    state_callback=self._on_ccd_state_changed
)
```

Connections are established automatically when centrald provides device information, with automatic reconnection on failure.

## GRB Detection System

The included GRB daemon provides production-ready gamma-ray burst detection:

- Modern GCN Kafka interface replacing legacy socket protocols
- Robust VOEvent XML parsing with mission-specific handlers
- PostgreSQL database integration with RTS2 target system
- Automatic observation triggering with configurable constraints
- Comprehensive monitoring and statistics

```bash
python grbd.py --gcn-client-id YOUR_ID --gcn-client-secret YOUR_SECRET
```

## Creating Device Drivers

Extend the appropriate base class or use mixins:

```python
from filterd import Filterd

class MyFilterWheel(Filterd):
    def setup_config(self, config):
        super().setup_config(config)
        config.add_argument('--serial-port', help='Serial port device')

    def set_filter_num(self, new_filter):
        # Implement hardware-specific movement
        return 0  # Success

    def get_filter_num(self):
        # Read current position from hardware
        return self.current_position

if __name__ == "__main__":
    app = App(description='My Filter Wheel Driver')
    app.register_device_options(MyFilterWheel)
    args = app.parse_args()
    device = app.create_device(MyFilterWheel)
    app.run()
```

## Key Features

- **Full RTS2 Compatibility**: Seamless integration with existing RTS2 installations
- **Modern Python Architecture**: Type hints, dataclasses, and proper error handling
- **Unified Configuration**: Single system handling all configuration sources
- **Mixin Architecture**: Reusable components for multi-function devices
- **Automatic Device Discovery**: Zero-configuration device-to-device communication
- **Production Ready**: Used in real observatory environments

## Logging

The system uses RTS2-compatible logging with proper formatting:

```
2024-05-04T12:34:56.789 UTC DEVICENAME I Log message here
```

All network communication and device interactions are logged for debugging and monitoring.

## Contributing

Contributions are welcome! The codebase follows modern Python practices with comprehensive type hints and documentation.

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

## Acknowledgments

This project is inspired by the original RTS2 system by Petr Kubanek et al. The OVIS filter wheel implementation was developed as part of the "Otevřená Věda" program of the Academy of Sciences of the Czech Republic, with contributions from high school students and university collaborators.

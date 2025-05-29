# RTS2-Python

A Python implementation of the RTS2 (Remote Telescope System 2nd Version) device drivers framework. This project aims to provide a complete Python-based alternative to the C++ RTS2 components, focusing on ease of development and extensibility while maintaining protocol compatibility with the original system.

## Overview

RTS2 (Remote Telescope System 2nd Version) is an open-source observatory control system designed for robotic observatories. This Python implementation provides:

- RTS2 device driver (daemon) network protocol support
- Device abstraction and inheritance framework
- Value system with proper Python typing
- Command handling with automatic distribution
- Filter wheel device implementations
- Example device drivers
- Device-to-device communication with automatic connection management
- Value and state change callback system for inter-device communication

Note that the development currently focuses on the device driver functionality. The client role is not implemented (although the device-to-device communication is similar). The code is being tested against the original RTS2 system to make sure it can work within an existing RTS2 installation.

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/rts2-python.git
cd rts2-python

# Install dependencies
pip install pyserial
```

## Usage

The framework is designed to be used as a base for creating your own device drivers. Example implementations are provided for several types of devices, particularly filter wheels.

To run the example dummy filter wheel:

```bash
python filterd_dummy.py
```

## Architecture

The system is built around several core components:

- **App**: Application framework for handling command line arguments and device creation
- **Device**: Base device class with network communication and value management
- **NetworkManager**: Handles all network communication, authentication, and message distribution
- **Value**: Type-safe values with serialization and change tracking
- **Connection**: Network connection management
- **CommandRegistry**: Command handler registration and dispatch system

## Supported Devices

Currently, the project includes implementations for the following devices:

- **Filterd**: Base class for filter wheels
- **Ovis**: OVIS filter wheel (Otevrena Veda Imaging Spectrograph, OV2023-2024, Adam Denko, Jan Sova, Veronika Modrá, Filip Bobal, Barbora Nohová)
- **DummyFilter**: Simulated filter wheel for testing
- **TemperatureSensor**: Simple temperature sensor example device
- **WatcherDevice**: Example device for monitoring other devices' states and values

### Value Interest Registration

Devices can register interest in values from other devices and receive callbacks when those values change:

```python
# Register interest in a value from another device
self.network.register_interest_in_value(
    device_name="centrald",
    value_name="sun_alt",
    callback=self._on_sun_altitude_update
)

# Callback function
def _on_sun_altitude_update(self, value_data):
    # Process the updated value
    new_altitude = float(value_data)
    logging.info(f"Sun altitude changed to {new_altitude} degrees")
```

### State Interest Registration

Devices can also monitor the state changes of other devices:

```python
# Register interest in state changes of another device
self.network.register_state_interest(
    device_name="CCD1",
    state_callback=self._on_ccd_state_changed
)

# Callback function
def _on_ccd_state_changed(self, device_name, state, bop_state, message):
    # React to the state change
    if bop_state & self.BOP_EXPOSURE:
        logging.info(f"Device {device_name} is now exposing")
    else:
        logging.info(f"Device {device_name} is no longer exposing")
```

### Automatic Connection Management

The system automatically establishes and maintains connections to devices of interest:

- When a device registers interest in another device's values or state, the connection is established automatically
- If the connection is lost, the system will attempt to reconnect at regular intervals
- All connection authentication and protocol handling is managed internally

This makes it easy to build distributed systems where devices can depend on each other without complex connection management code.

## Creating a New Device Driver

To create a new device driver, extend the appropriate base class:

```python
from filterd import Filterd

class MyFilterWheel(Filterd):
    @classmethod
    def register_options(cls, parser):
        super().register_options(parser)
        parser.add_argument('--my-option', help='My custom option')

    def __init__(self, device_name="W0", port=0):
        super().__init__(device_name, port)
        # Initialize your hardware-specific code here

    def get_filter_num(self):
        # Implement hardware-specific code to get filter position
        return self.filter_num

    def set_filter_num(self, new_filter):
        # Implement hardware-specific code to set filter position
        return 0  # Return 0 on success, -1 on error
```

Then create a main section to run your device:

```python
if __name__ == "__main__":
    app = App(description='My Filter Wheel Driver')
    app.register_device_options(MyFilterWheel)
    args = app.parse_args()
    device = app.create_device(MyFilterWheel)
    app.run()
```

## Key Features

- **Thread Safety**: All operations are designed to be thread-safe
- **Type Safety**: Proper Python type hints throughout the codebase
- **Protocol Compatibility**: Fully compatible with existing RTS2 centrald and clients
- **Extensibility**: Easy extension points for new device types
- **Automatic Value Distribution**: Values are automatically distributed to all connected clients
- **Device Coordination**: Built-in support for device-to-device communication with callbacks
- **Automatic Connection Management**: Devices can register interest and connections are handled automatically

## Logging

The system uses Python's standard logging module with a custom formatter to match RTS2's log format:

```
2024-05-04T12:34:56.789 UTC DEVICENAME I Log message here
```

Where the single letter represents the log level (D for DEBUG, I for INFO, etc.)

Note that the original RTS2 network logging system (which sends logs over the network protocol for monitoring by other components like the RTS2 Monitor) is not yet implemented. Currently, logs are only written to the console/standard output.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

## Acknowledgments

This project is inspired by the original RTS2 system by Petr Kubanek et al. While reimplementing the functionality in Python, we strive to maintain compatibility with the original system.

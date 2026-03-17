# RTS2 Python Focuser Implementation - Complete Version

## Overview

The focuser implementation has been completely rewritten to follow the same patterns as the filter wheel implementation, using the C++ sources as reference for functionality and commands.

## Key Improvements

### 1. Unified Configuration System
- **Single initialization**: Removed multi-step initialization, now uses unified `apply_focuser_config()`
- **Consistent pattern**: Follows the same pattern as `FilterMixin` with `setup_focuser_config()` and `apply_focuser_config()`
- **External temperature**: Proper support for external temperature monitoring via `--temperature-variable DEVICE.VARIABLE`

### 2. Complete Value System
- **Core values**: FOC_POS, FOC_TAR, FOC_DEF with proper RTS2 types
- **Offset values**: FOC_FILTEROFF, FOC_FOFF, FOC_TOFF for different offset types
- **Temperature compensation**: Full linear offset system with slope, intercept, and night-only options
- **Speed control**: foc_speed value for movement speed estimation

### 3. Command Interface
- **move**: Move to absolute position
- **step**: Relative movement (step by amount)
- **home**: Home the focuser (if supported)
- **killall**: Reset errors and call script_ends
- **killall_wse**: Reset errors without script_ends
- **script_ends**: End-of-script cleanup

### 4. State Management
- **Focusing state**: Proper FOC_FOCUSING state during movement
- **Progress tracking**: Duration estimation and progress reporting
- **Error handling**: Timeout detection and error state management
- **Queuing**: Value changes are queued during focusing operations

### 5. Temperature Compensation
- **External monitoring**: Connect to other devices for temperature readings
- **Linear compensation**: Automatic position adjustment based on temperature
- **Night-only option**: Optionally restrict compensation to night hours
- **Real-time updates**: Continuous monitoring and adjustment

## C++ Compatibility

### Values Matching C++ Implementation
```cpp
// C++ values matched in Python
position     -> foc_pos      (FOC_POS)
target       -> foc_tar      (FOC_TAR)  
defaultPosition -> foc_def   (FOC_DEF)
filterOffset -> foc_filteroff (FOC_FILTEROFF)
focusingOffset -> foc_foff   (FOC_FOFF)
tempOffset   -> foc_toff     (FOC_TOFF)
speed        -> foc_speed    (foc_speed)
temperature  -> foc_temp     (FOC_TEMP)
```

### Command Compatibility
- **setValue()**: Handled via `on_value_changed_from_client()`
- **setPosition()**: Implemented as `set_position()`
- **isFocusing()**: Implemented as `is_focusing()`
- **endFocusing()**: Implemented as `end_focusing()`
- **scriptEnds()**: Implemented as `script_ends_focuser()`

### State Machine
- **FOC_SLEEPING**: Idle state
- **FOC_FOCUSING**: Movement in progress
- **BOP_EXPOSURE**: Block operation during movement
- **Progress reporting**: Start/end times for movement estimation

## Network Protocol
- **Value distribution**: Automatic distribution of position updates
- **Command responses**: Proper +/- responses for commands
- **Metadata**: Full E/F message support for value descriptions
- **Connection management**: Pending command handling during movement

## Dummy Focuser Features
- **Simulated movement**: Gradual position changes toward target
- **Temperature simulation**: Sinusoidal temperature variation
- **Configurable step size**: Adjustable movement speed
- **Position limits**: Min/max position enforcement
- **Direct position writing**: Support for FOC_POS updates
- **Homing simulation**: Move to position 0

## Usage Examples

### Basic Focuser
```python
class MyFocuser(Focusd):
    def set_to(self, position):
        # Send command to hardware
        self.hardware.move_to(position)
        return 0
    
    def is_at_start_position(self):
        return abs(self.get_position()) < 1.0
```

### With Temperature Compensation
```python
def setup_config(self, config):
    super().setup_config(config)
    config.add_argument('--temp-device', default='TEMP.OUT')
    
def apply_config(self, config):
    super().apply_config(config)
    if config.get('temp_device'):
        self.create_external_temperature(config['temp_device'])
```

### Command Line Usage
```bash
# Basic focuser
python focusd_dummy.py -d F0 -p 0

# With temperature monitoring
python focusd_dummy.py --temperature-variable TEMP.OUT

# With limits and simulation
python focusd_dummy.py --foc-min -1000 --foc-max 1000 --simulate-temperature
```

## Integration with Filter Wheel
The focuser can be combined with the filter wheel as a mixin:

```python
class FilterFocuser(Device, FilterMixin, FocuserMixin):
    def setup_config(self, config):
        self.setup_filter_config(config)
        self.setup_focuser_config(config)
    
    def apply_config(self, config):
        super().apply_config(config)
        self.apply_filter_config(config)
        self.apply_focuser_config(config)
```

This creates a single device that handles both filter wheel and focuser operations, with proper command routing and state management for both subsystems.

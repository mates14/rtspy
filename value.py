from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar, Generic
from enum import IntFlag, auto
import math
import logging
from dataclasses import dataclass
from functools import wraps
import threading

#from device import Device

# Keep the existing flags for compatibility
class ValueType(IntFlag):
    """Value type constants matching the C++ RTS2 implementation."""
    STRING = 0x00000001
    INTEGER = 0x00000002
    TIME = 0x00000003
    DOUBLE = 0x00000004
    FLOAT = 0x00000005
    BOOL = 0x00000006
    SELECTION = 0x00000007
    LONGINT = 0x00000008
    RADEC = 0x00000009
    ALTAZ = 0x0000000A
    PID = 0x0000000B

    STAT = 0x00000010
    MMAX = 0x00000020
    RECTANGLE = 0x00000030
    ARRAY = 0x00000040
    TIMESERIE = 0x00000070

    MASK = 0x0000007f
    BASE_MASK = 0x0000000f
    EXT_MASK = 0x00000070


class ValueFlags(IntFlag):
    """Flags for value attributes."""
    FITS = 0x0000_0100
    WRITABLE = 0x0200_0000
    NOTNULL = 0x0800_0000
    AUTOSAVE = 0x0080_0000
    CHANGED = 0x0000_0400
    NEED_SEND = 0x0100_0000
    SCRIPTTEMPORARY = 0x04000000
    ERROR = 0x20000000
    WARNING = 0x10000000
    ERRORMASK = 0x30000000


# Protocol constants
PROTO_METAINFO = "M"


# Define a type variable for generic value types
T = TypeVar('T')


def track_changes(method):
    """Decorator to mark values as changed when they're modified."""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        old_value = self._value
        result = method(self, *args, **kwargs)
        if old_value != self._value:
            self.changed()
        return result
    return wrapper


class Callback:
    """Simple callback handler with thread safety."""

    def __init__(self):
        self._callbacks = []
        self._lock = threading.RLock()

    def add_callback(self, callback: Callable[..., None]) -> None:
        """Add a callback function."""
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[..., None]) -> None:
        """Remove a callback function."""
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

    def clear_callbacks(self) -> None:
        """Remove all callbacks."""
        with self._lock:
            self._callbacks.clear()

    def trigger(self, *args, **kwargs) -> None:
        """Trigger all callbacks with the given arguments."""
        with self._lock:
            callbacks = self._callbacks.copy()

        for callback in callbacks:
            try:
                callback(*args, **kwargs)
            except Exception as e:
                logging.error(f"Error in callback {callback}: {e}")


class Value(Generic[T]):
    """Base class for all values with network awareness and queuing built-in."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, value_type: ValueType = None, writable: bool = False):
        self.name = name
        self.description = description
        self._value = None

        self.rts2_type = value_type.value if value_type else 0
        if write_to_fits:
            self.rts2_type |= ValueFlags.FITS
        self.rts2_type |= flags

        if writable:
            self.rts2_type |= ValueFlags.WRITABLE

        # Add callback system
        self._callbacks = Callback()

        # Register with device for network and queuing
        from device import Device
        device = Device.get_instance()  # Singleton device

        # immediately register the value for network distribution
        # (we may assume there is no point in having a Value not in the network)
        if device:
            device.register_value(self)

    @property
    def value(self) -> T:
        """Get the Python value."""
        return self._value

    @value.setter
    @track_changes
    def value(self, new_value: T) -> None:
        """
        Set the value, with automatic queuing and network distribution.
        For network updates, use update_from_network() instead.

        Raises:
            ValueError: If change was queued
        """
        was_set = self._set_value(new_value, from_network=False)
        # We can do something with was_set if needed,
        # e.g., log that it was queued

    def _set_value(self, new_value: Any, from_network: bool = False) -> bool:
        """
        Internal method to set value with control over network behavior.

        Returns:
            True if value was set immediately, False if it was queued
        """
        # Get singleton device
        from device import Device
        device = Device.get_instance()

        # Check if we should queue (only for non-network updates)
        if not from_network and device and device.should_queue_value(self):
            #device.network.distribute_value_immediate(self)
            device.queue_value_change(self, '=', new_value)
            logging.info(f"Value change for {self.name} was queued")
            return False  # Indicate value was queued

        # Proceed with change
        converted_value = self._convert_value(new_value)
        if self._value != converted_value:
            self._value = converted_value

            # Mark as changed (sets CHANGED and NEED_SEND flags)
            self.changed()

            # Notify callbacks
            self._callbacks.trigger(self, self._value)

            # Distribute to network if needed
            if not from_network:
                if device:
                    device.distribute_value(self)

        return True  # Indicate value was set immediately

    def update_from_network(self, value_string: str) -> None:
        """
        Update value from a network client.

        This method is called when a client sends a value change command.
        It updates the value and triggers any callbacks, including higher-level
        device notifications.

        Args:
            value_string: New value as string

        Returns:
            True if value was changed, False otherwise
        """
        try:
            # Convert string to appropriate value type
            new_value = self._convert_value(value_string)

            # Only proceed if value is different
            if self._value == new_value:
                return True  # No change needed

            # Update the internal value
            old_value = self._value
            self._value = new_value

            # Mark as changed
            self.changed()

            # Notify callbacks about client-originated change
            self._callbacks.trigger(self, old_value, new_value, from_client=True)

            # Get device instance to notify about value change
            from device import Device
            device = Device.get_instance()
            if device:
                # Notify device about client-originated value change
                device.on_value_changed_from_client(self, old_value, new_value)

            return True
        except Exception as e:
            logging.error(f"Error updating value {self.name} from client: {e}")
            raise

    def _convert_value(self, value: Any) -> T:
        """Convert input value to the appropriate type."""
        return value  # Base implementation just returns the value

    def get_string_value(self) -> str:
        """Get value as string for network transmission."""
        return str(self._value) if self._value is not None else ""

    def is_null(self) -> bool:
        """Check if value is null."""
        return self._value is None

    def add_callback(self, callback: Callable[[T, T], None]) -> None:
        """Add a callback that will be called when the value changes."""
        self._callbacks.add_callback(callback)

    def remove_callback(self, callback: Callable[[T, T], None]) -> None:
        """Remove a callback."""
        self._callbacks.remove_callback(callback)

    # Network status flags
    def changed(self) -> None:
        """Mark the value as changed and needing to be sent."""
        old_flags = self.rts2_type
        self.rts2_type |= ValueFlags.CHANGED | ValueFlags.NEED_SEND

        # Only trigger callbacks if this is a new change
        if not (old_flags & ValueFlags.CHANGED):
            self._callbacks.trigger(self, self._value)

    def was_changed(self) -> bool:
        """Check if value was changed since last reset."""
        return bool(self.rts2_type & ValueFlags.CHANGED)

    def need_send(self) -> bool:
        """Check if value needs to be sent."""
        return bool(self.rts2_type & ValueFlags.NEED_SEND)

    def reset_need_send(self) -> None:
        """Reset the need_send flag."""
        self.rts2_type &= ~ValueFlags.NEED_SEND

    def reset_value_changed(self) -> None:
        """Reset the changed flag."""
        self.rts2_type &= ~ValueFlags.CHANGED

    # Flag management
    def is_writable(self) -> bool:
        return bool(self.rts2_type & ValueFlags.WRITABLE)

    def is_fits(self) -> bool:
        return bool(self.rts2_type & ValueFlags.FITS)

    def is_autosave(self) -> bool:
        return bool(self.rts2_type & ValueFlags.AUTOSAVE)

    def is_temporary(self) -> bool:
        return bool(self.rts2_type & ValueFlags.SCRIPTTEMPORARY)

    def set_writable(self) -> None:
        self.rts2_type |= ValueFlags.WRITABLE

    def set_read_only(self) -> None:
        self.rts2_type &= ~ValueFlags.WRITABLE

    def set_temporary(self) -> None:
        self.rts2_type |= ValueFlags.SCRIPTTEMPORARY

    def mask_error(self, error: int) -> None:
        """Set error mask."""
        self.rts2_type = (self.rts2_type & ~ValueFlags.ERRORMASK) | error

    def get_error_mask(self) -> int:
        """Get error mask."""
        return self.rts2_type & ValueFlags.ERRORMASK

    def is_error(self) -> bool:
        """Check if value has error."""
        return bool(self.get_error_mask() & ValueFlags.ERROR)

    def is_warning(self) -> bool:
        """Check if value has warning."""
        return bool(self.get_error_mask() & ValueFlags.WARNING)

    # Network communication
    def send_meta_info(self, conn) -> int:
        """Send metadata information to the connection."""
        ret = self.send_type_meta_info(conn)
        if ret < 0:
            return ret

        self.send(conn)
        return 0

    def send_type_meta_info(self, conn) -> int:
        """Send type metadata information."""
        msg = f"{PROTO_METAINFO} {self.rts2_type} \"{self.name}\" \"{self.description}\""
        logging.debug(f"send_type_meta_info: {msg}")
        return conn.send_msg(msg)

    def send(self, conn) -> None:
        """Send value through connection."""
        conn.send_value_raw(self.name, self.get_string_value())

    def __str__(self) -> str:
        return f"{self.name}={self.get_string_value()}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', value={self._value})"


class ValueString(Value[str]):
    """String value with proper typing."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: str = "", writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.STRING)
        self._value = default

    def _convert_value(self, value: Any) -> str:
        """Convert any value to string."""
        return str(value) if value is not None else ""

    def is_null(self) -> bool:
        """Check if string is empty."""
        return not bool(self._value)


class ValueDouble(Value[float]):
    """Double precision floating point value."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: Optional[float] = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.DOUBLE)
        self._value = float('nan') if default is None else float(default)

    def _convert_value(self, value: Any) -> float:
        """Convert value to float."""
        if value is None:
            return float('nan')
        return float(value)

    def get_string_value(self) -> str:
        """Get precise string representation."""
        return f"{self._value:.20e}" if not math.isnan(self._value) else "nan"

    def is_null(self) -> bool:
        """Check if value is NaN."""
        return math.isnan(self._value)


class ValueTime(ValueDouble):
    """Time value with proper RTS2 typing."""
    def __init__(self, name, description, write_to_fits=False, flags=0, default=None, writable: bool = False):
        # Use TIME type (0x3) instead of DOUBLE type (0x4)
        super().__init__(name, description, write_to_fits, flags, default=default)
        self.rts2_type = ValueType['TIME']

class ValueInteger(Value[int]):
    """Integer value."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: Optional[int] = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.INTEGER)

        if flags & ValueFlags.NOTNULL and default is None:
            raise ValueError("NOTNULL flag requires a default value")

        self._value = default
        self._null_value = None  # None is the null marker for Python

    def _convert_value(self, value: Any) -> Optional[int]:
        """Convert value to integer or None."""
        if value is None:
            return None
        return int(value)

    def is_null(self) -> bool:
        """Check if value is null."""
        return self._value is None


class ValueBool(Value[bool]):
    """Boolean value."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: Optional[bool] = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.BOOL)
        self._value = default

    def _convert_value(self, value: Any) -> Optional[bool]:
        """Convert value to boolean."""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.lower()
            if value in ('true', 'on', '1', 'yes'):
                return True
            elif value in ('false', 'off', '0', 'no'):
                return False
            else:
                return None
        return bool(value)

    def get_string_value(self) -> str:
        """Get string representation."""
        if self._value is None:
            return "unknown"
        return "true" if self._value else "false"


@dataclass
class Coordinates:
    """Coordinate pair with proper semantics."""
    x: float
    y: float

    def __post_init__(self):
        """Validate coordinates during initialization."""
        if self.x is None or self.y is None:
            raise ValueError("Coordinates cannot be None")
        self.x = float(self.x)
        self.y = float(self.y)

    def __iter__(self):
        """Make coordinates iterable."""
        yield self.x
        yield self.y

    def __eq__(self, other):
        """Equality check."""
        if isinstance(other, Coordinates):
            return self.x == other.x and self.y == other.y
        elif isinstance(other, (tuple, list)) and len(other) == 2:
            return self.x == other[0] and self.y == other[1]
        return NotImplemented

    def to_tuple(self) -> Tuple[float, float]:
        """Convert to tuple."""
        return (self.x, self.y)


class ValueCoordinate(Value[Coordinates]):
    """Base class for coordinate values like RaDec and AltAz."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, value_type: ValueType = None,
                 default_x: float = None, default_y: float = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, value_type)

        if default_x is not None and default_y is not None:
            self._value = Coordinates(default_x, default_y)
        else:
            self._value = None

    def _convert_value(self, value: Any) -> Optional[Coordinates]:
        """Convert value to Coordinates."""
        if value is None:
            return None

        if isinstance(value, Coordinates):
            return value

        if isinstance(value, str):
            parts = value.split()
            if len(parts) != 2:
                raise ValueError(f"Expected 2 values, got {len(parts)}")
            return Coordinates(float(parts[0]), float(parts[1]))

        if isinstance(value, (list, tuple)) and len(value) == 2:
            return Coordinates(float(value[0]), float(value[1]))

        raise ValueError(f"Cannot convert {value} to Coordinates")

    def get_string_value(self) -> str:
        """Get string representation for network."""
        if self._value is None:
            return "nan nan"
        return f"{self._value.x:.20e} {self._value.y:.20e}"

    def is_null(self) -> bool:
        """Check if value is null."""
        return self._value is None

    @property
    def x(self) -> Optional[float]:
        """Get x coordinate."""
        return self._value.x if self._value else None

    @x.setter
    @track_changes
    def x(self, value: float) -> None:
        """Set x coordinate."""
        if self._value is None:
            self._value = Coordinates(float(value), float('nan'))
        else:
            self._value = Coordinates(float(value), self._value.y)

    @property
    def y(self) -> Optional[float]:
        """Get y coordinate."""
        return self._value.y if self._value else None

    @y.setter
    @track_changes
    def y(self, value: float) -> None:
        """Set y coordinate."""
        if self._value is None:
            self._value = Coordinates(float('nan'), float(value))
        else:
            self._value = Coordinates(self._value.x, float(value))

    def to_tuple(self) -> Optional[Tuple[float, float]]:
        """Get coordinates as tuple."""
        return self._value.to_tuple() if self._value else None


class ValueRaDec(ValueCoordinate):
    """Right ascension and declination coordinates."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default_ra: float = None, default_dec: float = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.RADEC,
                         default_ra, default_dec)

    @property
    def ra(self) -> Optional[float]:
        """Get right ascension."""
        return self.x

    @ra.setter
    def ra(self, value: float) -> None:
        """Set right ascension with normalization."""
        # Handle normalization if needed
        if self.rts2_type & 0x000c0000:  # RTS2_DT_DEG_DIST_180
            value = value % 360
            if value > 180.0:
                value -= 360.0
        self.x = value

    @property
    def dec(self) -> Optional[float]:
        """Get declination."""
        return self.y

    @dec.setter
    def dec(self, value: float) -> None:
        """Set declination with normalization."""
        if self.rts2_type & 0x000c0000:  # RTS2_DT_DEG_DIST_180
            value = value % 360
            if value > 180.0:
                value -= 360.0
        self.y = value


class ValueAltAz(ValueCoordinate):
    """Altitude and azimuth coordinates."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default_alt: float = None, default_az: float = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.ALTAZ,
                         default_alt, default_az)

    @property
    def alt(self) -> Optional[float]:
        """Get altitude."""
        return self.x

    @alt.setter
    def alt(self, value: float) -> None:
        """Set altitude."""
        self.x = value

    @property
    def az(self) -> Optional[float]:
        """Get azimuth."""
        return self.y

    @az.setter
    def az(self, value: float) -> None:
        """Set azimuth."""
        self.y = value


# Add statistical tracking
@dataclass
class Statistics:
    """Statistical information about a value."""
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0  # For variance calculation
    min_val: float = float('inf')
    max_val: float = float('-inf')

    @property
    def variance(self) -> float:
        """Calculate variance."""
        return self.m2 / self.count if self.count > 1 else 0.0

    @property
    def std_dev(self) -> float:
        """Calculate standard deviation."""
        return math.sqrt(self.variance) if self.count > 1 else 0.0

    def update(self, value: float) -> None:
        """Update statistics with a new value using Welford's algorithm."""
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)


class ValueStat(ValueDouble):
    """A value that tracks statistics."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: Optional[float] = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags | ValueType.STAT, default)
        self._stats = Statistics()

    @property
    def value(self) -> float:
        """Get the value."""
        return self._value

    @value.setter
    @track_changes
    def value(self, new_value: float) -> None:
        """Set the value and update statistics."""
        converted = self._convert_value(new_value)
        self._value = converted

        # Update statistics if value is not NaN
        if not math.isnan(converted):
            self._stats.update(converted)

    @property
    def stats(self) -> Statistics:
        """Get statistics object."""
        return self._stats

    @property
    def mean(self) -> float:
        """Get mean value."""
        return self._stats.mean

    @property
    def std_dev(self) -> float:
        """Get standard deviation."""
        return self._stats.std_dev

    @property
    def min_val(self) -> float:
        """Get minimum value."""
        return self._stats.min_val

    @property
    def max_val(self) -> float:
        """Get maximum value."""
        return self._stats.max_val

    def __str__(self) -> str:
        """String representation with statistics."""
        if math.isnan(self._value):
            return f"{self.name}=nan"
        return f"{self.name}={self._value:.6f} (mean={self.mean:.6f}, σ={self.std_dev:.6f}, n={self._stats.count})"


class ValueSelection(Value[int]):
    """Selection value (enumeration with string labels)."""

    def __init__(self, name: str, description: str = "", write_to_fits: bool = True,
                 flags: int = 0, default: Optional[int] = None, writable: bool = False):
        super().__init__(name, description, write_to_fits, flags, ValueType.SELECTION)
        self._value = default if default is not None else 0
        self._selection_values = []  # List of string values

    def _convert_value(self, value: Any) -> int:
        """Convert value to integer index."""
        if value is None:
            return 0

        # If it's a string, find its index
        if isinstance(value, str):
            try:
                # Try to convert to integer first
                return int(value)
            except ValueError:
                # Not a number, try to find it in selections
                try:
                    return self._selection_values.index(value)
                except ValueError:
                    raise ValueError(f"Invalid selection value: {value}")

        # Otherwise convert to integer
        return int(value)

    def get_string_value(self) -> str:
        """Get string representation for network."""
        if not self._selection_values:
            return "0"

        if self._value >= 0 and self._value < len(self._selection_values):
            return str(self._value)
        return "0"  # Default to first item if out of range

    def get_sel_name(self, index=None) -> str:
        """Get the name for a selection index."""
        if index is None:
            index = self._value

        if index >= 0 and index < len(self._selection_values):
            return self._selection_values[index]
        return ""

    def get_sel_index(self, name: str) -> int:
        """Get the index for a selection name."""
        try:
            return self._selection_values.index(name)
        except ValueError:
            return -1

    def add_sel_val(self, value: str) -> None:
        """Add a selection value."""
        self._selection_values.append(value)
        self.changed()

    def clear_selection(self) -> None:
        """Clear all selection values."""
        self._selection_values.clear()
        self._value = 0
        self.changed()

    def set_value_char_arr(self, value: str) -> int:
        """Set value from a string (either index or name)."""
        try:
            # Try to set as integer
            new_value = int(value)
            if new_value >= 0 and new_value < len(self._selection_values):
                self._value = new_value
                self.changed()
                return 0
        except ValueError:
            # Try to set as name
            try:
                index = self._selection_values.index(value)
                self._value = index
                self.changed()
                return 0
            except ValueError:
                pass

        # Value not found
        return -1

    def copy_sel(self, other_selection: 'ValueSelection') -> None:
        """Copy selection options from another ValueSelection."""
        self._selection_values = other_selection._selection_values.copy()
        self.changed()

    def sel_size(self) -> int:
        """Get number of selection options."""
        return len(self._selection_values)

    def __str__(self) -> str:
        """String representation."""
        if not self._selection_values:
            return f"{self.name}=0"

        sel_name = self.get_sel_name()
        return f"{self.name}={self._value} ({sel_name})"

    def update_from_network(self, value_string: str) -> bool:
        """Update value from network string."""
        old_value = self._value

        try:
            new_value = self._convert_value(value_string)
            if new_value < 0 or new_value >= len(self._selection_values):
                return False

            self._value = new_value
            self.changed()

            # Get device instance to notify about value change
            from device import Device
            device = Device.get_instance()
            if device:
                # Notify device about client-originated value change
                device.on_value_changed_from_client(self, old_value, new_value)

            return True
        except Exception as e:
            logging.error(f"Error updating selection value {self.name}: {e}")
            return False

    def send_selections(self, conn):
        """Send selection options to a connection."""
        # First send empty to clear existing selections
        conn.send(f"F \"{self.name}\"\n")

        # Then send each selection value
        for val in self._selection_values:
            conn.send(f"F \"{self.name}\" \"{val}\"\n")

        return 0  # Success

    def send_meta_info(self, conn):
        """Send metadata information to the connection."""
        ret = super().send_meta_info(conn)
        if ret < 0:
            return ret

        # Also send selections
        return self.send_selections(conn)

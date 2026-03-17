"""rtspy - Python implementation of RTS2 device drivers

A complete Python-based framework for RTS2 (Remote Telescope System)
device drivers, maintaining full protocol compatibility with the C++ system.
"""

__version__ = "0.1.0"
__author__ = "Martin Jelínek"

# Main API exports - core classes that users commonly need
from rtspy.core.device import Device
from rtspy.core.app import App
from rtspy.core.config import DeviceConfig
from rtspy.core.value import (
    Value, ValueBool, ValueString, ValueInteger,
    ValueDouble, ValueTime, ValueRaDec, ValueSelection
)
from rtspy.core.constants import DeviceType, ConnectionState
from rtspy.core.filterd import FilterMixin, Filterd
from rtspy.core.focusd import FocuserMixin, Focusd

__all__ = [
    "Device",
    "App",
    "DeviceConfig",
    "Value",
    "ValueBool",
    "ValueString",
    "ValueInteger",
    "ValueDouble",
    "ValueTime",
    "ValueRaDec",
    "ValueSelection",
    "DeviceType",
    "ConnectionState",
    "FilterMixin",
    "Filterd",
    "FocuserMixin",
    "Focusd",
    "__version__",
]

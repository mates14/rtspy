#!/usr/bin/env python3
"""
Coude/OES spectrograph of the Perek 2m telescope (Ondrejov)

Copyright (C) 2026 Martin Jelinek

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.

Hardware background:
--------------------

Jan Fuchs' spectrographd (alhena:8888, XML-RPC) runs some 28 numbered
elements of the 2m spectrograph room: the dichroic mirror, the spectral
(order sorting) filter, collimators, camera foci, the grating, lamps,
shutters, exposure meters, the iodine cell and so on.  The protocol is
described in docs/perek2m-protocol.md.  In short:

    spectrograph_info()        -> {'GLST': "<28 states>", 'SPGP_13': ..., ...}
    spectrograph_execute(cmd)  -> "1" when a command is accepted

and the commands are SPCH n v (change), SPAP n v / SPRP n v (absolute or
relative position), SPST n (stop), SPCA n (calibrate), SSTE n / SSPE n
(start/stop an exposure meter).

The spectral filter (element 2) is the one thing here that selects where
light goes - position 4 feeds FLORES, the others the CCD700 - so it is
exposed as this device's filter wheel.  Every other element is an ordinary
value, writable where the server takes a command for it.

The spectrograph is shared with observers working from Fuky's own clients.
The driver therefore never commands anything by itself (no move at startup,
no default filter unless --default-filter is given), and --readonly turns it
into a pure monitor.  While anything in the spectrograph is moving -
whoever moved it - exposures are blocked.
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from rtspy.core.app import App
from rtspy.core.constants import DeviceType
from rtspy.core.device import Device
from rtspy.core.filterd import FilterMixin
from rtspy.core.value import (ValueBool, ValueDouble, ValueInteger, ValueSelection,
                              ValueString)
from rtspy.core.xmlrpc_poller import XmlRpcPoller

# GLST state labels, indexed by the raw value the server reports.  A raw
# value outside the list shows as "unknown" (appended to every selection).
POS4 = ["stop", "1", "2", "3", "4", "moving", "timeout"]
POS5 = ["stop", "1", "2", "3", "4", "5", "moving", "timeout"]
COLLIMATOR = ["stop", "open", "closed", "open left", "open right", "moving", "timeout"]
STAR_CAL = ["stop", "star", "calibration", "moving", "timeout"]
COUDE_OES = ["stop", "Coude", "OES", "moving", "timeout"]
SHUTTER = ["stop", "open", "closed", "moving", "timeout"]
CORRECTOR = ["unknown", "in", "out"]
IODINE = ["stop", "inserted", "removed", "moving", "alarm"]
FOCUS_STATE = ["stop", "moving"]
GRATING_STATE = ["stop", "moving", "timeout"]

UNKNOWN = "unknown"

# Spectral filter (element 2) raw states that are not positions
SF_MOVING = 6
SF_TIMEOUT = 7
SF_POSITIONS = 5


@dataclass
class Element:
    """
    One discrete spectrograph element shown as an RTS2 value.

    `kind` is 'sel' (labels indexed by the raw state) or 'bool' (0 = off).
    `settable` maps a value the client may set to the SPCH argument sent;
    None means read-only.  `moving` lists raw states that mean the element
    is in motion (blocks exposures).
    """
    number: int
    name: str
    description: str
    kind: str = 'sel'
    labels: Optional[List[str]] = None
    settable: Optional[Dict[int, int]] = None
    moving: Tuple[int, ...] = ()
    fits: bool = True


def _same(*values):
    return {v: v for v in values}


ELEMENTS = [
    Element(1, "DICHROIC", "dichroic mirror position", labels=POS4,
            settable=_same(0, 1, 2, 3, 4), moving=(5,)),
    Element(3, "COLLIM", "Coude collimator", labels=COLLIMATOR,
            settable=_same(0, 1, 2, 3, 4), moving=(5,)),
    Element(6, "STARCAL", "star / calibration light feed", labels=STAR_CAL,
            settable=_same(0, 1, 2), moving=(3,)),
    Element(7, "COUDEOES", "Coude / OES light feed", labels=COUDE_OES,
            settable=_same(0, 1, 2), moving=(3,)),
    # Elements 8 (flat lamp) and 9 (comparison lamp) are left out on
    # purpose: those lamps are permanently gone, and values for them would
    # only be mistaken for the real ones - TOPTEC's FIBCOMP / FIBFLAT
    # (Quido relays).
    Element(10, "EMSHUT", "Coude exposure meter shutter", labels=SHUTTER,
            settable=_same(0, 1, 2), moving=(3,)),
    Element(11, "SHUT700", "camera 700 shutter", labels=SHUTTER,
            settable=_same(0, 1, 2), moving=(3,)),
    Element(12, "SHUT400", "camera 400 shutter", labels=SHUTTER,
            settable=_same(0, 1, 2), moving=(3,)),
    Element(15, "SLCFILT", "Coude slit camera filter", labels=POS5,
            settable=_same(0, 1, 2, 3, 4, 5), moving=(6,)),
    Element(16, "CORR700", "camera 700 corrector", labels=CORRECTOR),
    Element(17, "CORR400", "camera 400 corrector", labels=CORRECTOR),
    Element(21, "OESCOLL", "OES collimator", labels=COLLIMATOR,
            settable=_same(0, 1, 2, 3, 4), moving=(5,)),
    Element(23, "OESEMSHT", "OES exposure meter shutter", labels=SHUTTER,
            settable=_same(0, 1, 2), moving=(3,)),
    # Set 1 = remove, 2 = insert, but read 1 = inserted, 2 = removed:
    # the client sets the state it wants to read back
    Element(26, "IODINE", "iodine absorption cell", labels=IODINE,
            settable={0: 0, 1: 2, 2: 1}, moving=(3,)),
    Element(27, "SLCAMPWR", "Coude slit camera power", kind='bool',
            settable=_same(0, 1), fits=False),
    Element(28, "OESSLPWR", "OES slit camera power", kind='bool',
            settable=_same(0, 1), fits=False),
]

# Elements with a position axis: (number, value prefix, description, SPGP key)
AXES = [
    (4, "FOC700", "camera 700 focus", "SPGP_4"),
    (5, "FOC400", "camera 400 focus", "SPGP_5"),
    (22, "FOCOES", "OES focus", "SPGP_22"),
]

# Exposure meters: (number, value prefix, description)
METERS = [
    (14, "EM", "Coude exposure meter"),
    (24, "OESEM", "OES exposure meter"),
]

# Elements SPCA (calibrate/home) and SPRP (relative move) are known to work for
CALIBRATABLE = (4, 5, 22)
RELATIVE = (4, 5, 13, 22)

GRATING = 13


def grating_angle_to_steps(degrees: float) -> int:
    """Fuky's calibration, as used for SPAP 13."""
    return int(round(-205.294 * degrees + 12667.1))


def grating_steps_to_angle(steps: float) -> float:
    """Fuky's (separately fitted) inverse, as used for display."""
    return -0.00487106 * steps + 61.7024


def parse_angle(text: str) -> float:
    """Degrees from "DD:MM[.mmm]" (the observers' notation) or plain decimal degrees."""
    text = str(text).strip()
    if ':' in text:
        deg, minutes = text.split(':', 1)
        sign = -1.0 if deg.strip().startswith('-') else 1.0
        return sign * (abs(float(deg)) + float(minutes) / 60.0)
    return float(text)


def accepted(result: Any) -> bool:
    """spectrograph_execute answers "1" to a command it took."""
    return str(result).strip() == "1"


class Spec2m(Device, FilterMixin):
    """The 2m spectrograph: spectral filter as the wheel, the rest as values."""

    DEFAULT_FILTERS = "1:2:3:FLORES:5"

    def setup_config(self, config):
        self.setup_filter_config(config)

        config.add_argument('--spec-host', default='192.168.193.195',
                            help='spectrographd XML-RPC host (alhena)', section='spec2m')
        config.add_argument('--spec-port', type=int, default=8888,
                            help='spectrographd XML-RPC port', section='spec2m')
        config.add_argument('--poll-interval', type=float, default=1.0,
                            help='Status poll interval in seconds', section='spec2m')
        config.add_argument('--rpc-timeout', type=float, default=5.0,
                            help='XML-RPC call timeout in seconds', section='spec2m')
        config.add_argument('--filter-timeout', type=float, default=60.0,
                            help='Spectral filter movement timeout in seconds',
                            section='spec2m')
        config.add_argument('--readonly', action='store_true',
                            help='Monitor only: refuse every command to the spectrograph',
                            section='spec2m')

    def __init__(self, device_name="SPEC", port=0):
        super().__init__(device_name, DeviceType.FW, port)

        self.comm: Optional[XmlRpcPoller] = None
        self.hw_lock = threading.RLock()
        self.readonly = False

        # Filled in apply_config, once values can be registered
        self.element_values: Dict[int, Any] = {}
        self.value_elements: Dict[str, Element] = {}
        self.axis_values: Dict[int, Tuple[ValueInteger, ValueSelection]] = {}
        self.meter_values: Dict[int, Tuple[ValueBool, ValueDouble, ValueDouble]] = {}

        self._filt_target: Optional[int] = None    # raw position 1..5
        self._filt_started: Optional[float] = None
        self._moving: frozenset = frozenset()

        self.set_state(self.STATE_IDLE | self.NOT_READY, "Connecting to spectrograph")

    # ---------------------------------------------------------------- config

    def apply_config(self, config: Dict[str, Any]):
        super().apply_config(config)
        self.apply_filter_config(config)

        self.spec_host = config.get('spec_host', '192.168.193.195')
        self.spec_port = config.get('spec_port', 8888)
        self.poll_interval = config.get('poll_interval', 1.0)
        self.rpc_timeout = config.get('rpc_timeout', 5.0)
        self.filter_timeout = config.get('filter_timeout', 60.0)
        self.readonly = bool(config.get('readonly', False))

        if self.filter.sel_size() != SF_POSITIONS:
            logging.warning("The spectral filter has %d positions, -F names %d",
                            SF_POSITIONS, self.filter.sel_size())

        # FITS name of the spectral filter, as the OES headers already call
        # it.  The camera using this wheel writes FILTER itself.
        self.specfilt = ValueString("SPECFILT", "spectral (order sorting) filter",
                                    write_to_fits=True)

        for element in ELEMENTS:
            if element.kind == 'bool':
                value = ValueBool(element.name, element.description,
                                  write_to_fits=element.fits,
                                  writable=element.settable is not None)
            else:
                value = ValueSelection(element.name, element.description,
                                       write_to_fits=element.fits,
                                       writable=element.settable is not None)
                for label in element.labels + [UNKNOWN]:
                    value.add_sel_val(label)
                value.value = len(element.labels)    # unknown until polled
            self.element_values[element.number] = value
            self.value_elements[element.name] = element

        self.grating_angle = ValueDouble("GRATANG", "[deg] grating angle",
                                         write_to_fits=True, writable=True)
        self.grating_pos = ValueInteger("GRATPOS", "[steps] grating position",
                                        write_to_fits=True, writable=True)
        self.grating_state = self._state_selection("GRATST", "grating motion", GRATING_STATE)

        for number, prefix, description, _key in AXES:
            pos = ValueInteger(prefix, "[steps] %s position" % description,
                               write_to_fits=True, writable=True)
            state = self._state_selection(prefix + "ST", "%s motion" % description,
                                          FOCUS_STATE)
            self.axis_values[number] = (pos, state)

        for number, prefix, description in METERS:
            running = ValueBool(prefix + "RUN", "%s counting" % description,
                                write_to_fits=False, writable=True)
            count = ValueDouble(prefix + "CNT", "[counts] %s count" % description,
                                write_to_fits=True)
            freq = ValueDouble(prefix + "FRQ", "[counts/s] %s rate" % description,
                               write_to_fits=False)
            self.meter_values[number] = (running, count, freq)

        # Raw telemetry, for whatever the tables above do not decode
        self.glst = ValueString("GLST", "raw spectrograph state word", write_to_fits=False)
        self.spgs19 = ValueString("SPGS19", "element 19 state (meaning unknown)",
                                  write_to_fits=False)
        self.spgs20 = ValueString("SPGS20", "element 20 state (meaning unknown)",
                                  write_to_fits=False)

        self.spec_conn = ValueBool("spec_conn", "spectrograph server is responding",
                                   write_to_fits=False)
        self.spec_url = ValueString("spec_url", "spectrograph XML-RPC server URL",
                                    write_to_fits=False)
        self.spec_url.value = "http://%s:%d" % (self.spec_host, self.spec_port)
        self.spec_readonly = ValueBool("spec_readonly", "commands to the spectrograph refused",
                                       write_to_fits=False, writable=True)
        self.spec_readonly.value = self.readonly
        self.last_error = ValueString("last_error", "last refused or failed command",
                                      write_to_fits=False)
        self.spexec_answer = ValueString("spexec_answer", "answer to the last spexec command",
                                         write_to_fits=False)

    @staticmethod
    def _state_selection(name, description, labels):
        value = ValueSelection(name, description, write_to_fits=False)
        for label in labels + [UNKNOWN]:
            value.add_sel_val(label)
        value.value = len(labels)
        return value

    def _register_device_commands(self):
        super()._register_device_commands()
        self._register_filter_commands()
        self.network.command_registry.register_handler(SpecCommands(self))

    # --------------------------------------------------------------- startup

    def start(self):
        super().start()

        self.comm = XmlRpcPoller("spectrograph", self.spec_host, self.spec_port,
                                 "spectrograph_info", self.poll_interval, self.rpc_timeout)
        self.comm.set_status_callback(self._handle_status_update)
        self.comm.start()

        logging.info("Spectrograph device started, polling %s every %.1fs%s",
                     self.spec_url.value, self.poll_interval,
                     " (READ ONLY)" if self.readonly else "")

    def stop(self):
        if self.comm:
            self.comm.stop()
            self.comm = None
        super().stop()

    # ------------------------------------------------------------- telemetry

    def _handle_status_update(self, info: Optional[Dict[str, Any]]):
        """Poller thread: `info` is spectrograph_info(), or None when unreachable."""
        if info is None:
            if self.spec_conn.value:
                self.spec_conn.value = False
                self.set_state(self._state | self.ERROR_HW, "spectrograph server unreachable")
            return

        with self.hw_lock:
            glst = [int(x) for x in str(info['GLST']).split()]

            if not self.spec_conn.value:
                self.spec_conn.value = True
                self.set_state(self._state & ~(self.ERROR_HW | self.NOT_READY),
                               "spectrograph server responding")

            self.glst.value = " ".join(str(x) for x in glst)
            moving = set()

            for element in ELEMENTS:
                raw = self._raw(glst, element.number)
                value = self.element_values[element.number]
                if element.kind == 'bool':
                    value.value = bool(raw)
                else:
                    value.value = self._label_index(element.labels, raw)
                if raw in element.moving:
                    moving.add(element.name)

            for number, _prefix, _description, key in AXES:
                pos, state = self.axis_values[number]
                self._set_number(pos, self._int(info.get(key)))
                raw = self._raw(glst, number)
                state.value = self._label_index(FOCUS_STATE, raw)
                if raw == 1:
                    moving.add(pos.name)

            steps = self._int(info.get('SPGP_13'))
            self._set_number(self.grating_pos, steps)
            if steps is not None:
                self.grating_angle.value = round(grating_steps_to_angle(steps), 4)
            raw = self._raw(glst, GRATING)
            self.grating_state.value = self._label_index(GRATING_STATE, raw)
            if raw == 1:
                moving.add("GRATING")

            for number, _prefix, _description in METERS:
                running, count, freq = self.meter_values[number]
                running.value = bool(self._raw(glst, number))
                self._set_number(count, self._float(info.get('SPCE_%d' % number)))
                self._set_number(freq, self._float(info.get('SPFE_%d' % number)))

            self.spgs19.value = str(info.get('SPGS_19', '')).strip()
            self.spgs20.value = str(info.get('SPGS_20', '')).strip()

            sf_raw = self._raw(glst, 2)
            if sf_raw == SF_MOVING:
                moving.add("filter")

            self.infotime.value = time.time()

            self._check_filter(sf_raw)
            self._update_specfilt(sf_raw)
            self._update_moving(frozenset(moving))

    @staticmethod
    def _set_number(value, number):
        """Keep the last good reading when the server sends something unparseable."""
        if number is not None:
            value.value = number

    @staticmethod
    def _raw(glst: List[int], number: int) -> Optional[int]:
        """Raw state of element `number` (1-based), None if not reported."""
        return glst[number - 1] if len(glst) >= number else None

    @staticmethod
    def _label_index(labels: List[str], raw: Optional[int]) -> int:
        if raw is None or not 0 <= raw < len(labels):
            return len(labels)    # "unknown"
        return raw

    @staticmethod
    def _int(text) -> Optional[int]:
        try:
            return int(float(str(text).strip()))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _float(text) -> Optional[float]:
        try:
            return float(str(text).strip())
        except (TypeError, ValueError):
            return None

    def _update_moving(self, moving: frozenset):
        """Block exposures while anything in the spectrograph moves.  Under hw_lock."""
        if moving == self._moving:
            return
        started = moving - self._moving
        stopped = self._moving - moving
        self._moving = moving
        if started:
            logging.info("Moving: %s", ", ".join(sorted(started)))
        if stopped:
            logging.info("Stopped: %s", ", ".join(sorted(stopped)))
        self.set_state(self._state,
                       "spectrograph moving" if moving else "spectrograph still",
                       self.set_bop_exposure('spectrograph', bool(moving)))

    def _update_specfilt(self, raw: Optional[int]):
        if raw is not None and 1 <= raw <= SF_POSITIONS:
            name = self.filter.get_sel_name(raw - 1) or str(raw)
            if not self.movement_in_progress and self.filter.value != raw - 1:
                # Moved by someone else (Fuky's clients): follow it
                logging.info("Spectral filter is now at %d (%s)", raw, name)
                self.filter.value = raw - 1
        else:
            name = self._label_name(POS5, raw)
        if self.specfilt.value != name:
            self.specfilt.value = name

    @staticmethod
    def _label_name(labels, raw):
        if raw is None or not 0 <= raw < len(labels):
            return UNKNOWN
        return labels[raw]

    # ---------------------------------------------------------- filter wheel

    def set_filter_num(self, new_filter) -> int:
        """Start moving the spectral filter to selection index `new_filter`."""
        if not self._allow_command("move the spectral filter"):
            return -1
        if not 0 <= new_filter < SF_POSITIONS:
            logging.error("Spectral filter index %s out of range", new_filter)
            return -1

        target = new_filter + 1
        with self.hw_lock:
            self._filt_target = target
            self._filt_started = time.time()
            # set_filter_num_mask records this only after we return, which
            # is too late if the next poll already finds the filter there
            self._target_filter = new_filter

        self._execute("SPCH 2 %d" % target, on_done=self._filter_command_done)
        logging.info("Moving spectral filter to %d (%s)",
                     target, self.filter.get_sel_name(new_filter))
        return 0

    def _filter_command_done(self, ok: bool, message: str):
        """Poller thread: the SPCH 2 answer."""
        if ok:
            return
        with self.hw_lock:
            if self._filt_target is not None:
                self._fail_filter("spectral filter command refused: %s" % message)

    def _check_filter(self, raw: Optional[int]):
        """Complete, fail or time out a spectral filter move.  Under hw_lock."""
        if self._filt_target is None:
            return

        if raw == self._filt_target:
            logging.info("Spectral filter reached %d", raw)
            self._filt_target = None
            self._filt_started = None
            self.movement_completed()
            return

        if raw == SF_TIMEOUT:
            self._fail_filter("spectral filter reports timeout")
        elif time.time() - self._filt_started > self.filter_timeout:
            self._fail_filter("spectral filter did not reach %d in %.0fs (at %s)"
                              % (self._filt_target, self.filter_timeout, raw))

    def _fail_filter(self, message: str):
        """Under hw_lock."""
        logging.error(message)
        self.last_error.value = message
        self._filt_target = None
        self._filt_started = None
        self.movement_in_progress = False
        self._target_filter = None
        self.set_state((self._state & ~self.FILTERD_MOVE) | self.ERROR_HW, message,
                       self.set_bop_exposure('filter', False))
        conn = self.take_filter_waiter()
        if conn is not None:
            self.network._send_error_response(conn, message)

    def get_filter_num(self) -> int:
        return self.filter.value or 0

    # -------------------------------------------------------- client changes

    def on_value_changed_from_client(self, value, old_value, new_value):
        try:
            name = value.name

            if name == "spec_readonly":
                # --readonly only sets it at start: switched at run time to
                # hand the spectrograph over and take it back
                self.readonly = bool(new_value)
                logging.info("Spectrograph commands %s",
                             "refused (read-only)" if self.readonly else "allowed")
                return 0

            if name in self.value_elements:
                element = self.value_elements[name]
                wanted = int(new_value)
                if wanted not in element.settable:
                    logging.error("%s cannot be set to %s", name,
                                  value.get_sel_name(wanted) if element.kind == 'sel'
                                  else wanted)
                    return -1
                return self._command("SPCH %d %d" % (element.number, element.settable[wanted]),
                                     "set %s" % name)

            for number, _prefix, _description, _key in AXES:
                if value is self.axis_values[number][0]:
                    return self._command("SPAP %d %d" % (number, int(new_value)),
                                         "move %s" % name)

            if value is self.grating_pos:
                return self._command("SPAP %d %d" % (GRATING, int(new_value)),
                                     "move the grating")

            if value is self.grating_angle:
                steps = grating_angle_to_steps(float(new_value))
                logging.info("Grating angle %.4f deg = %d steps", float(new_value), steps)
                return self._command("SPAP %d %d" % (GRATING, steps), "move the grating")

            for number, _prefix, _description in METERS:
                if value is self.meter_values[number][0]:
                    verb = "SSTE" if new_value else "SSPE"
                    return self._command("%s %d" % (verb, number),
                                         "start/stop exposure meter %d" % number)

            return FilterMixin.on_value_changed_from_client(self, value, old_value, new_value)

        except Exception as exc:
            logging.error("Error handling change of %s: %s", value.name, exc, exc_info=True)
            return -1

    def _command(self, cmd: str, what: str) -> int:
        """Send a value-change command.  The answer comes later; 0 = queued."""
        if not self._allow_command(what):
            return -1
        self._execute(cmd)
        return 0

    # ------------------------------------------------------------- execution

    def _allow_command(self, what: str) -> bool:
        if self.readonly:
            logging.error("Cannot %s: running read-only", what)
            self.last_error.value = "read-only: refused to %s" % what
            return False
        if self.comm is None or not self.comm.is_connected():
            logging.error("Cannot %s: spectrograph server not responding", what)
            return False
        return True

    def _execute(self, cmd: str, on_done=None, getter: bool = False):
        """
        Queue spectrograph_execute(cmd).  on_done(ok, message) runs on the
        poller thread; a set command is ok when answered "1", a getter
        (getter=True) whenever it is answered at all.
        """
        def result(ok, answer):
            answer_text = str(answer).strip()
            if ok and not getter and not accepted(answer_text):
                ok = False
            if ok:
                logging.info("spectrograph_execute(%s) => %s", cmd, answer_text)
            else:
                message = "%s => %s" % (cmd, answer_text)
                logging.error("spectrograph_execute(%s) failed: %s", cmd, answer_text)
                self.last_error.value = message
            if on_done is not None:
                on_done(ok, answer_text)

        self.comm.call('spectrograph_execute', cmd, on_result=result)

    # ------------------------------------------------------- device plumbing

    def info(self):
        super().info()
        self.filter_info_update()


class SpecCommands:
    """
    Spectrograph commands that map onto no value.  Each is answered once
    spectrographd has accepted (or refused) it, not when it is queued.

        spcal <n>             calibrate (home) a focus axis - SPCA, n = 4, 5, 22
        spstop <n>            stop element n - SPST
        sprel <n> <steps>     relative move - SPRP, n = 4, 5, 13, 22
        spexec <command...>   any raw spectrographd command; the answer is
                              left in spexec_answer.  With --readonly only
                              the read commands (SPG*, SPCE, SPFE) pass.
    """

    READ_COMMANDS = ("SPGP", "SPGS", "SPCE", "SPFE")

    def __init__(self, device: Spec2m):
        self.device = device
        self.handlers = {
            "spcal": self.handle_spcal,
            "spstop": self.handle_spstop,
            "sprel": self.handle_sprel,
            "spexec": self.handle_spexec,
        }

    def get_commands(self):
        return list(self.handlers.keys())

    def can_handle(self, command):
        return command in self.handlers

    def needs_response_for(self, command):
        return True

    def handle(self, command, conn, params):
        if command in self.handlers:
            return self.handlers[command](conn, params)
        return False

    def _error(self, conn, message):
        self.device.network._send_error_response(conn, message)
        return False

    def _run(self, conn, cmd, what, getter=False, on_answer=None):
        """Send cmd and answer conn once spectrographd has replied."""
        device = self.device
        if getter and device.readonly:
            # read commands are harmless even when read-only
            if device.comm is None or not device.comm.is_connected():
                return self._error(conn, "spectrograph server not responding")
        elif not device._allow_command(what):
            return self._error(conn, "cannot %s (read-only or not connected)" % what)

        part = device.network.begin_part(conn)

        def done(ok, answer):
            if on_answer is not None:
                on_answer(answer)
            device.network.end_part(part, ok, "%s => %s" % (cmd, answer))

        device._execute(cmd, on_done=done, getter=getter)
        return True

    @staticmethod
    def _numbers(params, count):
        parts = params.split()
        if len(parts) != count:
            raise ValueError("expected %d numbers" % count)
        return [int(p) for p in parts]

    def handle_spcal(self, conn, params):
        try:
            (number,) = self._numbers(params, 1)
        except ValueError:
            return self._error(conn, "usage: spcal <element>")
        if number not in CALIBRATABLE:
            return self._error(conn, "spcal: element must be one of %s" % (CALIBRATABLE,))
        return self._run(conn, "SPCA %d" % number, "calibrate element %d" % number)

    def handle_spstop(self, conn, params):
        try:
            (number,) = self._numbers(params, 1)
        except ValueError:
            return self._error(conn, "usage: spstop <element>")
        if not 1 <= number <= 28:
            return self._error(conn, "spstop: element must be 1..28")
        return self._run(conn, "SPST %d" % number, "stop element %d" % number)

    def handle_sprel(self, conn, params):
        try:
            number, steps = self._numbers(params, 2)
        except ValueError:
            return self._error(conn, "usage: sprel <element> <steps>")
        if number not in RELATIVE:
            return self._error(conn, "sprel: element must be one of %s" % (RELATIVE,))
        return self._run(conn, "SPRP %d %d" % (number, steps),
                         "move element %d by %d" % (number, steps))

    def handle_spexec(self, conn, params):
        cmd = params.strip().strip('"').strip()
        if not cmd:
            return self._error(conn, "usage: spexec <spectrographd command>")
        getter = cmd.split()[0].upper() in self.READ_COMMANDS

        def keep(answer):
            self.device.spexec_answer.value = answer

        return self._run(conn, cmd, "execute '%s'" % cmd, getter=getter, on_answer=keep)


def main():
    return App(description='Coude/OES spectrograph (Perek 2m)').main(Spec2m)


if __name__ == "__main__":
    import sys
    sys.exit(main())

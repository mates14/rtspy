#!/usr/bin/env python3
"""
TOPTEC primary focus head at the Perek 2m telescope (Ondrejov)

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

The TOPTEC-built primary focus head carries, on two independent motorised
axes:

- the *focus* axis (0..8192 steps), which moves the whole head along the
  optical axis - a focuser in every sense;

- the *lateral* axis (0..15999 steps), which slides the pickup across the
  focal plane to select what light is fed where: the CCD700 and OES fibre
  couplings (object / comparison / flat, one or two fibres), or the G2
  photometric camera.  Mechanically this is a selector, so it is exposed
  here as a filter wheel.  Fibre couplings are insensitive to where the
  head sits along the optical axis, but the G1 pointing camera and the G2
  photometric camera do not share a focal plane, so lateral positions may
  carry a focus offset, applied through FOC_FILTEROFF.

Everything else the head reports - end switches, supply voltages, camera
power - is plain telemetry, exposed as ordinary values.

This driver is a client of Jan Fuchs' XML-RPC server (default port 6000),
the same one the `fiber_control_client.py` Qt panel (the "toptec window")
talks to.  Its whole method surface is:

    toptec_get_values()            -> dict of the telemetry below
    toptec_set_focus_position(n)   toptec_inc_focus_position()
                                   toptec_dec_focus_position()
    toptec_set_camera_position(n)  toptec_inc_camera_position()
                                   toptec_dec_camera_position()
    toptec_set_camera_power(mask)  bit 0 = G1, bit 1 = G2
    toptec_reset()

All XML-RPC traffic happens on a single background thread, so no network
round trip can stall the RTS2 event loop.  Movement completion is detected
from that thread's telemetry poll, the way filterd_ovis does it.
"""

import logging
import math
import queue
import threading
import time
import xmlrpc.client
from typing import Any, Dict, List, Optional

from rtspy.core.app import App
from rtspy.core.constants import DeviceType
from rtspy.core.device import Device
from rtspy.core.filterd import FilterMixin
from rtspy.core.focusd import FocuserMixin
from rtspy.core.value import ValueBool, ValueDouble, ValueInteger, ValueString

# Lateral positions as measured in 2021 (fiber_control_client.cfg [camera]),
# with the focus offsets implied by that file's [focus] g1/g2 pair: the fibre
# couplings and the G1 pointing camera share focus 4678, the G2 photometric
# camera focuses at 5350.  Override wholesale with --fibsel.
DEFAULT_FIBSEL = (
    "CCD700_object_1fiber:400:0,"
    "CCD700_object_2fiber:1050:0,"
    "OES_object:1700:0,"
    "CCD700_comp_1fiber:12965:0,"
    "CCD700_comp_2fiber:13615:0,"
    "OES_comp:14300:0,"
    "CCD700_flat_1fiber:14600:0,"
    "CCD700_flat_both_fibers:14930:0,"
    "CCD700_flat_2fiber:15250:0,"
    "OES_flat:15600:0,"
    "Photometric_G2:15625:672"
)


class ToptecCommunicator:
    """
    Owns the XML-RPC connection to the TOPTEC server.

    Every call to the server - telemetry poll and command alike - is made on
    this thread, so xmlrpc.client's non-thread-safe transport is only ever
    touched from one place and a hung server cannot block the device.
    Commands are queued by the device thread and drained here before each
    poll.
    """

    def __init__(self, host: str, port: int, poll_interval: float = 1.0):
        self.url = "http://%s:%d" % (host, port)
        self.poll_interval = poll_interval

        self.proxy: Optional[xmlrpc.client.ServerProxy] = None
        self.connected = False
        self.failure_reported = False
        self.status_callback = None

        self.commands: "queue.Queue[tuple]" = queue.Queue()
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self) -> bool:
        """Start the worker thread.  Does not wait for the first poll."""
        if self.thread and self.thread.is_alive():
            return True

        self.running = True
        self.thread = threading.Thread(target=self._loop, name="toptec-xmlrpc", daemon=True)
        self.thread.start()
        return True

    def stop(self):
        """Stop the worker thread."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=self.poll_interval * 3)
        self.thread = None

    def set_status_callback(self, callback):
        """Register the callable invoked with each successful telemetry poll."""
        self.status_callback = callback

    def is_connected(self) -> bool:
        return self.connected

    def call(self, method: str, *args):
        """Queue a server call.  Returns immediately; result is not collected."""
        self.commands.put((method, args))

    def _loop(self):
        while self.running:
            started = time.time()

            if self.proxy is None:
                # ServerProxy construction does no I/O, so this never blocks -
                # a dead server shows up on the first real call instead.
                self.proxy = xmlrpc.client.ServerProxy(self.url, allow_none=True)

            try:
                self._drain_commands()
                values = self.proxy.toptec_get_values()

                if not self.connected:
                    logging.info("TOPTEC server %s is responding", self.url)
                    self.connected = True
                    self.failure_reported = False

                if self.status_callback:
                    try:
                        self.status_callback(values)
                    except Exception as exc:
                        logging.error("Error in TOPTEC status callback: %s", exc, exc_info=True)

            except Exception as exc:
                # Report a lost server once, not once per poll - an
                # observatory left running against a dead box would
                # otherwise bury its log under a line a second.
                if not self.failure_reported:
                    logging.error("TOPTEC server %s unreachable: %s", self.url, exc)
                    self.failure_reported = True
                self.connected = False
                # Drop the proxy so the next iteration builds a fresh
                # transport rather than reusing a broken keep-alive socket.
                self.proxy = None
                if self.status_callback:
                    try:
                        self.status_callback(None)
                    except Exception as cb_exc:
                        logging.error("Error in TOPTEC status callback: %s", cb_exc, exc_info=True)

            elapsed = time.time() - started
            if self.running and elapsed < self.poll_interval:
                time.sleep(self.poll_interval - elapsed)

    def _drain_commands(self):
        """Execute every queued command, oldest first."""
        while True:
            try:
                method, args = self.commands.get_nowait()
            except queue.Empty:
                return

            try:
                getattr(self.proxy, method)(*args)
                logging.debug("TOPTEC %s%s", method, args)
            except Exception as exc:
                logging.error("TOPTEC %s%s failed: %s", method, args, exc)
                raise


class Toptec(Device, FilterMixin, FocuserMixin):
    """
    The TOPTEC head as a single RTS2 device: focuser on the focus axis,
    filter wheel on the lateral axis, telemetry as plain values.
    """

    def setup_config(self, config):
        """Register configuration for both mixins and the TOPTEC itself."""
        self.setup_filter_config(config)
        self.setup_focuser_config(config)

        config.add_argument('--toptec-host', default='192.168.193.198',
                            help='TOPTEC XML-RPC server host', section='toptec')
        config.add_argument('--toptec-port', type=int, default=6000,
                            help='TOPTEC XML-RPC server port', section='toptec')
        config.add_argument('--poll-interval', type=float, default=1.0,
                            help='Telemetry poll interval in seconds', section='toptec')
        config.add_argument('--fibsel', default=DEFAULT_FIBSEL,
                            help='Lateral positions as comma-separated '
                                 'name:position[:focus_offset] triples',
                            section='toptec')
        config.add_argument('--fibsel-tolerance', type=int, default=3,
                            help='Lateral position tolerance in steps', section='toptec')
        config.add_argument('--fibsel-timeout', type=float, default=120.0,
                            help='Lateral movement timeout in seconds', section='toptec')
        config.add_argument('--focus-tolerance', type=float, default=2.0,
                            help='Focus position tolerance in steps', section='toptec')
        config.add_argument('--focus-timeout', type=float, default=120.0,
                            help='Focus movement timeout in seconds', section='toptec')
        config.add_argument('--cam-max', type=int, default=15999,
                            help='Maximum lateral axis position', section='toptec')

    def __init__(self, device_name="F0", port=0):
        super().__init__(device_name, DeviceType.FOCUS, port)

        self.focuser_type = "TOPTEC"

        self.comm: Optional[ToptecCommunicator] = None
        self.hw_lock = threading.RLock()
        self.fibsel = None

        # Lateral position table, parallel to the `filter` selection
        self.fibsel_positions: List[int] = []
        self.fibsel_offsets: List[float] = []
        self.fibsel_tolerance = 3
        self.fibsel_timeout = 120.0
        self.focus_tolerance = 2.0
        self.focus_timeout = 120.0

        # Movement bookkeeping, all touched under hw_lock
        self._initialised = False
        self._cam_target: Optional[int] = None
        self._cam_move_started: Optional[float] = None
        self._foc_move_started: Optional[float] = None
        self._pending_filteroff: Optional[float] = None

        self.set_state(self.STATE_IDLE | self.NOT_READY, "Connecting to TOPTEC")

    # ---------------------------------------------------------------- config

    def apply_config(self, config: Dict[str, Any]):
        super().apply_config(config)
        self.apply_filter_config(config)
        self.apply_focuser_config(config)

        self.toptec_host = config.get('toptec_host', '192.168.193.198')
        self.toptec_port = config.get('toptec_port', 6000)
        self.poll_interval = config.get('poll_interval', 1.0)
        self.fibsel_tolerance = config.get('fibsel_tolerance', 3)
        self.fibsel_timeout = config.get('fibsel_timeout', 120.0)
        self.focus_tolerance = config.get('focus_tolerance', 2.0)
        self.focus_timeout = config.get('focus_timeout', 120.0)
        self.cam_max = config.get('cam_max', 15999)

        self._parse_fibsel(config.get('fibsel', DEFAULT_FIBSEL))

        # The focus axis runs 0..8192; --foc-min/--foc-max may narrow it.
        if config.get('foc_min') is None or config.get('foc_max') is None:
            self.set_focus_extent(0.0, 8192.0)

        # Telemetry the head reports but the focuser/filter mixins have no
        # slot for.  Created here, after the mixins, so they register in a
        # sensible order.
        # The lateral position by name, for the FITS header.  It cannot be
        # the `filter` selection itself: C++ Camera creates its own value
        # called "filter" too (camd.cpp, camFilterVal), both map to the FITS
        # keyword FILTER, and the camera's wins - so the fibre feed, which is
        # the single most useful thing to record about a FLORES exposure,
        # silently loses to a camera that has no filter wheel at all.
        # FIBSEL is eight characters, so it is a plain FITS keyword rather
        # than a HIERARCH card, and collides with nothing.
        self.fibsel = ValueString(
            "FIBSEL", "fibre/camera feed selected by the lateral stage",
            write_to_fits=True)

        self.cam_pos = ValueInteger(
            "CAM_POS", "[steps] lateral (fibre selector) axis position",
            write_to_fits=True, writable=True)
        self.cam_tar = ValueInteger(
            "CAM_TAR", "[steps] lateral axis target position", write_to_fits=False)

        self.esw1a = ValueBool("ESW1A", "focus axis end switch A (true = at limit)",
                               write_to_fits=False)
        self.esw1b = ValueBool("ESW1B", "focus axis end switch B (true = at limit)",
                               write_to_fits=False)
        self.esw2a = ValueBool("ESW2A", "lateral axis end switch A (true = at limit)",
                               write_to_fits=False)
        self.esw2b = ValueBool("ESW2B", "lateral axis end switch B (true = at limit)",
                               write_to_fits=False)

        self.g1_power = ValueBool("G1_POWER", "G1 pointing camera power",
                                  write_to_fits=True, writable=True)
        self.g2_power = ValueBool("G2_POWER", "G2 photometric camera power",
                                  write_to_fits=True, writable=True)

        self.volt_33 = ValueDouble("VOLT_33", "[V] 3.3V supply", write_to_fits=False)
        self.volt_50 = ValueDouble("VOLT_50", "[V] 5V supply", write_to_fits=False)
        self.volt_120 = ValueDouble("VOLT_120", "[V] 12V supply", write_to_fits=False)

        self.toptec_conn = ValueBool("toptec_conn", "TOPTEC server is responding",
                                     write_to_fits=False)
        self.toptec_url = ValueString("toptec_url", "TOPTEC XML-RPC server URL",
                                      write_to_fits=False)
        self.toptec_url.value = "http://%s:%d" % (self.toptec_host, self.toptec_port)

    def _parse_fibsel(self, spec: str):
        """
        Parse the lateral position table.

        Format is comma-separated `name:position[:focus_offset]`, e.g.
        `OES_comp:14300,Photometric_G2:15625:672`.  Commas separate entries
        rather than colons because the field itself is colon-delimited, and
        because RTS2's own -F filter syntax is colon-separated - keeping the
        two apart avoids an ambiguity that would silently mis-split names.
        """
        names, positions, offsets = [], [], []

        for entry in spec.split(','):
            entry = entry.strip()
            if not entry:
                continue

            parts = entry.split(':')
            if len(parts) < 2:
                logging.error("Ignoring malformed --fibsel entry '%s' "
                              "(want name:position[:focus_offset])", entry)
                continue

            try:
                position = int(parts[1])
                offset = float(parts[2]) if len(parts) > 2 and parts[2] != '' else 0.0
            except ValueError:
                logging.error("Ignoring --fibsel entry '%s' with non-numeric "
                              "position or offset", entry)
                continue

            names.append(parts[0])
            positions.append(position)
            offsets.append(offset)

        if not names:
            logging.error("No usable --fibsel entries, falling back to built-in table")
            return self._parse_fibsel(DEFAULT_FIBSEL)

        self.fibsel_positions = positions
        self.fibsel_offsets = offsets
        self.set_filters(self.filter, ':'.join(names))

        logging.info("Lateral positions: %s",
                     ', '.join("%s=%d%s" % (n, p, "" if o == 0 else " (foc %+g)" % o)
                               for n, p, o in zip(names, positions, offsets)))

    def _register_device_commands(self):
        super()._register_device_commands()
        self._register_filter_commands()
        self._register_focuser_commands()
        self.network.command_registry.register_handler(ToptecCommands(self))

    # --------------------------------------------------------------- startup

    def start(self):
        super().start()

        self.foc_type.value = self.focuser_type

        self.comm = ToptecCommunicator(self.toptec_host, self.toptec_port,
                                       self.poll_interval)
        self.comm.set_status_callback(self._handle_status_update)
        self.comm.start()

        logging.info("TOPTEC device started, polling %s every %.1fs",
                     self.toptec_url.value, self.poll_interval)

    def stop(self):
        if self.comm:
            self.comm.stop()
            self.comm = None
        super().stop()

    # ------------------------------------------------------------- telemetry

    def _handle_status_update(self, values: Optional[Dict[str, Any]]):
        """
        Called from the communicator thread on every poll.

        `values` is the server's toptec_get_values() dict, or None when the
        server could not be reached.
        """
        if values is None:
            if self.toptec_conn.value:
                self.toptec_conn.value = False
                # ERROR_HW, not NOT_READY: a device that has once been up
                # should answer clients and refuse moves with an error,
                # rather than leave every client blocked waiting for a
                # readiness that may never come.
                self.set_state(self._state | self.ERROR_HW,
                               "TOPTEC server unreachable")
            return

        with self.hw_lock:
            if not self.toptec_conn.value:
                self.toptec_conn.value = True
                self.set_state(self._state & ~(self.ERROR_HW | self.NOT_READY),
                               "TOPTEC server responding")

            self.foc_pos.value = float(values['focus_position'])
            self.cam_pos.value = int(values['cameras_position'])

            self.esw1a.value = bool(values['esw1a'])
            self.esw1b.value = bool(values['esw1b'])
            self.esw2a.value = bool(values['esw2a'])
            self.esw2b.value = bool(values['esw2b'])

            self.g1_power.value = bool(values['g1'])
            self.g2_power.value = bool(values['g2'])

            # The server reports supply voltages in tenths of a volt.
            self.volt_33.value = values['voltage_33'] / 10.0
            self.volt_50.value = values['voltage_50'] / 10.0
            self.volt_120.value = values['voltage_120'] / 10.0

            self.infotime.value = time.time()

            self._initialise_focus()
            self._check_cam_movement()
            self._check_focus_movement()

    def _initialise_focus(self):
        """
        Settle FOC_DEF/FOC_TAR against the head's real position, once.

        The stock Focusd.start() does this by calling info() at startup, but
        the head is only reachable from the poll thread and may not be up
        when the daemon is - so it is done on the first telemetry instead.
        Call under hw_lock.
        """
        if self._initialised:
            return
        self._initialised = True

        if math.isnan(self.foc_def.value):
            # No --start-position: whatever focus the head is at is the
            # default, which is what an observer restarting the daemon
            # mid-night expects.
            self.foc_def.value = self.foc_pos.value
            self.foc_tar.value = self.foc_pos.value
            self.update_offsets_extent()
            logging.info("Adopted current focus %g as FOC_DEF", self.foc_pos.value)
        else:
            logging.info("Moving to configured start position %g", self.foc_def.value)
            self.set_position(self.foc_def.value + self.foc_filteroff.value
                              + self.foc_foff.value + self.foc_toff.value
                              + self.tc_offset())

        # Adopt whichever lateral position the head is parked at.
        self.filter.value = self.get_filter_num()
        self._sync_fibsel()

    def _check_cam_movement(self):
        """Complete or time out a lateral move.  Call under hw_lock."""
        if self._cam_target is None:
            return

        if abs(self.cam_pos.value - self._cam_target) <= self.fibsel_tolerance:
            logging.info("Lateral axis reached %d (target %d)",
                         self.cam_pos.value, self._cam_target)
            self._cam_target = None
            self._cam_move_started = None

            if self._target_filter is None:
                # A raw CAM_POS move.  If it happened to land on a
                # configured position, adopt it - otherwise `filter` and
                # FOC_FILTEROFF would go on describing wherever the head
                # was before, which is worse than saying nothing.
                matched = self._match_position(self.cam_pos.value)
                if matched is not None:
                    logging.info("Raw move landed on %s, adopting it",
                                 self.filter.get_sel_name(matched))
                    self.filter.value = matched
                    self._pending_filteroff = self.fibsel_offsets[matched]

            # Raise the focus block before dropping the filter one, so an
            # exposure cannot slip into the gap between the two moves.
            self._apply_filter_focus_offset()
            self.movement_completed()
            self._sync_fibsel()
            return

        if (self._cam_move_started is not None
                and time.time() - self._cam_move_started > self.fibsel_timeout):
            logging.error("Lateral axis timed out after %.0fs at %d (target %d)",
                          self.fibsel_timeout, self.cam_pos.value, self._cam_target)
            self._cam_target = None
            self._cam_move_started = None
            self._pending_filteroff = None
            self.movement_in_progress = False
            self._target_filter = None
            self.set_state(self._state | self.ERROR_HW, "lateral movement timed out",
                           self.set_bop_exposure('filter', False))

    def _apply_filter_focus_offset(self):
        """
        Push the newly selected lateral position's focus offset into
        FOC_FILTEROFF, re-focusing if it actually changed.  Call under
        hw_lock, before movement_completed().
        """
        offset = self._pending_filteroff
        self._pending_filteroff = None

        if offset is None or offset == self.foc_filteroff.value:
            return

        logging.info("Lateral position implies focus offset %+g (was %+g)",
                     offset, self.foc_filteroff.value)
        self.foc_filteroff.value = offset
        self.set_position(self.foc_def.value + offset
                          + self.foc_foff.value + self.foc_toff.value
                          + self.tc_offset())

    def _check_focus_movement(self):
        """Complete or time out a focus move.  Call under hw_lock."""
        if not (self._state & self.FOC_FOCUSING) or self._foc_move_started is None:
            return

        if self._target_position is not None and \
                abs(self.foc_pos.value - self._target_position) <= self.focus_tolerance:
            self._foc_move_started = None
            self.end_focusing()
            return

        if (self._foc_move_started is not None
                and time.time() - self._foc_move_started > self.focus_timeout):
            logging.error("Focus axis timed out after %.0fs at %g (target %s)",
                          self.focus_timeout, self.foc_pos.value, self._target_position)
            self._foc_move_started = None
            self._target_position = None
            self._movement_in_progress = False
            self.set_state(self._state & ~self.FOC_FOCUSING | self.ERROR_HW,
                           "focus movement timed out",
                           self.set_bop_exposure('focus', False))

    # ------------------------------------------------- FocuserMixin contract

    def set_to(self, position: float) -> int:
        """Start a focus move.  Completion is detected by the poll thread."""
        if not self._require_connection("move focus"):
            return -1

        target = int(round(position))
        if not 0 <= target <= 8192:
            logging.error("Focus target %d outside the head's 0..8192 range", target)
            return -1

        self._foc_move_started = time.time()
        self.comm.call('toptec_set_focus_position', target)
        logging.info("Moving focus to %d", target)
        return 0

    def is_at_start_position(self) -> bool:
        # The head has no home position to return to - FOC_DEF is wherever
        # the observer last decided focus should be.
        return False

    def focuser_info_update(self):
        # foc_pos is maintained by the poll thread; nothing to fetch here.
        pass

    def tc_offset(self) -> float:
        return 0.0

    # -------------------------------------------------- FilterMixin contract

    def set_filter_num(self, new_filter) -> int:
        """Start a lateral move to the given selection index."""
        if not self._require_connection("move lateral axis"):
            return -1

        if not 0 <= new_filter < len(self.fibsel_positions):
            logging.error("Lateral position index %s out of range", new_filter)
            return -1

        target = self.fibsel_positions[new_filter]

        with self.hw_lock:
            self._cam_target = target
            self._cam_move_started = time.time()
            self._pending_filteroff = self.fibsel_offsets[new_filter]
            self.cam_tar.value = target

        self.comm.call('toptec_set_camera_position', target)
        logging.info("Moving lateral axis to %d (%s)",
                     target, self.filter.get_sel_name(new_filter))
        return 0

    def get_filter_num(self) -> int:
        """
        Report which lateral position the head is actually parked at.

        When it sits somewhere that matches no configured position - which
        CAM_POS lets an observer do deliberately - the last known selection
        is kept rather than snapping to the nearest entry, so `filter` never
        claims a position the head is not at.
        """
        matched = self._match_position(self.cam_pos.value)
        if matched is not None:
            return matched
        return self.filter.value or 0

    def _match_position(self, position: Optional[int]) -> Optional[int]:
        """Selection index the given lateral position corresponds to, if any."""
        if position is None:
            return None
        for idx, configured in enumerate(self.fibsel_positions):
            if abs(position - configured) <= self.fibsel_tolerance:
                return idx
        return None

    # ----------------------------------------------------- TOPTEC's own bits

    def reset_head(self) -> int:
        """The control panel's Reset button."""
        if not self._require_connection("reset"):
            return -1
        logging.info("Resetting TOPTEC head")
        self.comm.call('toptec_reset')
        return 0

    def nudge(self, axis: str, direction: str) -> int:
        """
        The control panel's +/- buttons: one controller step on either axis.

        These bypass FOC_TAR/`filter` because the hardware moves by its own
        increment and never reports what that increment was - the poll picks
        the new position up like any other change.
        """
        if not self._require_connection("nudge %s" % axis):
            return -1

        method = "toptec_%s_%s_position" % (
            'inc' if direction == 'up' else 'dec',
            'focus' if axis == 'focus' else 'camera')
        self.comm.call(method)
        logging.info("Nudging %s axis %s", axis, direction)
        return 0

    def _set_camera_power(self, g1: bool, g2: bool) -> int:
        """Set both camera power rails in the single call the server offers."""
        if not self._require_connection("set camera power"):
            return -1

        mask = (1 if g1 else 0) | (2 if g2 else 0)
        self.comm.call('toptec_set_camera_power', mask)
        logging.info("Camera power: G1 %s, G2 %s",
                     "ON" if g1 else "OFF", "ON" if g2 else "OFF")
        return 0

    def _require_connection(self, what: str) -> bool:
        if self.comm is None or not self.comm.is_connected():
            logging.error("Cannot %s: TOPTEC server not responding", what)
            return False
        return True

    # ------------------------------------------------------- device plumbing

    def info(self):
        super().info()
        self.filter_info_update()
        self.focuser_info_update()
        self._sync_fibsel()

    def _sync_fibsel(self):
        """Keep the FITS-facing name in step with the selection."""
        if self.fibsel is None:
            return
        name = self.filter.get_sel_name()
        if name and self.fibsel.value != name:
            self.fibsel.value = name

    def on_value_changed_from_client(self, value, old_value, new_value):
        try:
            if value.name == "CAM_POS":
                # A deliberate move to a raw position: no preset, so no
                # focus offset is implied and `filter` stays where it was
                # until get_filter_num sees the head arrive somewhere known.
                if not self._require_connection("set lateral position"):
                    return -1
                target = int(new_value)
                if not 0 <= target <= self.cam_max:
                    logging.error("Lateral target %d outside 0..%d",
                                  target, self.cam_max)
                    return -1
                with self.hw_lock:
                    self._cam_target = target
                    self._cam_move_started = time.time()
                    self._pending_filteroff = None
                    self.cam_tar.value = target
                    self.movement_in_progress = True
                    self.set_state(self._state | self.FILTERD_MOVE,
                                   "lateral move started",
                                   self.set_bop_exposure('filter', True))
                self.comm.call('toptec_set_camera_position', target)
                return 0

            if value.name == "G1_POWER":
                return self._set_camera_power(bool(new_value), self.g2_power.value)

            if value.name == "G2_POWER":
                return self._set_camera_power(self.g1_power.value, bool(new_value))

            filter_result = FilterMixin.on_value_changed_from_client(
                self, value, old_value, new_value)
            focuser_result = FocuserMixin.on_value_changed_from_client(
                self, value, old_value, new_value)
            return min(filter_result, focuser_result)

        except Exception as exc:
            logging.error("Error handling change of %s: %s", value.name, exc,
                          exc_info=True)
            return -1


class ToptecCommands:
    """The control panel's buttons that map onto no RTS2 value."""

    def __init__(self, device: Toptec):
        self.device = device
        self.handlers = {
            "reset": self.handle_reset,
            "focus_up": lambda conn, params: self.handle_nudge(conn, 'focus', 'up'),
            "focus_down": lambda conn, params: self.handle_nudge(conn, 'focus', 'down'),
            "cam_up": lambda conn, params: self.handle_nudge(conn, 'cam', 'up'),
            "cam_down": lambda conn, params: self.handle_nudge(conn, 'cam', 'down'),
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

    def handle_reset(self, conn, params):
        if self.device.reset_head() == 0:
            self.device.network._send_ok_response(conn)
            return True
        self.device.network._send_error_response(conn, "Cannot reset TOPTEC head")
        return False

    def handle_nudge(self, conn, axis, direction):
        if self.device.nudge(axis, direction) == 0:
            self.device.network._send_ok_response(conn)
            return True
        self.device.network._send_error_response(
            conn, "Cannot nudge %s axis %s" % (axis, direction))
        return False


def main():
    return App(description='TOPTEC primary focus head (Perek 2m)').main(Toptec)


if __name__ == "__main__":
    import sys
    sys.exit(main())

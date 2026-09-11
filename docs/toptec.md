# TOPTEC primary focus head (Perek 2m)

`rtspy/drivers/toptec.py` - an RTS2 device for the TOPTEC-built primary
focus head of the Perek 2m telescope at Ondrejov, i.e. the hardware behind
the "toptec window" of Jan Fuchs' `fiber_control_client.py` Qt panel.

## What it is

The head has two independent motorised axes, and they are not the same kind
of thing:

- **focus** (0..8192 steps) moves the whole head along the optical axis.
  This is a focuser: it blocks exposures while it moves, it takes `move`,
  and it carries the full RTS2 offset stack (`FOC_DEF` + `FOC_FILTEROFF` +
  `FOC_FOFF` + `FOC_TOFF`).

- **lateral** (0..15999 steps) slides the pickup across the focal plane to
  select what is fed where - the CCD700 and OES fibre couplings, or the G2
  photometric camera. Mechanically this is a selector, so it is a filter
  wheel. Fibre couplings do not care where the head sits along the optical
  axis; the G1 pointing camera and the G2 photometric camera do not share a
  focal plane, so a lateral position may carry a focus offset.

The device is therefore both at once - `Toptec(Device, FilterMixin,
FocuserMixin)`, the same shape as `filterd_ovis.py` - and registers on the
bus as `DeviceType.FOCUS`.

Everything else the head reports (end switches, supply voltages, camera
power) is plain telemetry.

## Running it

    rtspy-focusd-toptec -d TOPTEC --server <centrald> --toptec-host 192.168.193.198

It backgrounds itself, takes `/var/run/rts2_TOPTEC` and reports startup failure
through its exit status, exactly as a C++ RTS2 daemon does - so
`rts2-start TOPTEC` / `rts2-stop TOPTEC` / `systemctl start rts2@TOPTEC`
drive it directly, with no shell wrapper. Add `-i` to keep it on the
terminal. See `docs/daemonising-rtspy.md`.

The matching line in `/etc/rts2/devices`:

    focusd  toptec  TOPTEC  --toptec-host 192.168.193.198

The line names no program. `rts2-start` resolves `focusd toptec` by looking
for `rts2-focusd-toptec` first and `rtspy-focusd-toptec` second, so nothing
in rtspy has to claim an `rts2-` name.

Useful options:

| option | default | meaning |
|---|---|---|
| `--toptec-host` / `--toptec-port` | `192.168.193.198` / `6000` | XML-RPC server |
| `--poll-interval` | `1.0` | telemetry poll, seconds |
| `--fibsel` | see below | lateral position table |
| `--fibsel-tolerance` | `3` | steps within which a position counts as reached |
| `--fibsel-timeout` / `--focus-timeout` | `120` | movement timeouts, seconds |
| `--start-position` | *(none)* | focus to move to at startup; without it the head's current focus becomes `FOC_DEF` |

All of these can also come from an ini file section `[toptec]` - see
`rtspy/core/config.py` for the search path.

## The lateral position table

`--fibsel` is a comma-separated list of `name:position[:focus_offset]`:

    --fibsel "OES_object:1700,OES_comp:14300,Photometric_G2:15625:672"

Commas separate entries because the entries themselves are colon-delimited.
The built-in default is the 2021 `fiber_control_client.cfg` `[camera]`
table, with the one focus offset that file's `[focus]` section implies:
`g1 = 4678` for the fibre couplings and the pointing camera, `g2 = 5350`
for the photometric camera, hence `+672` on `Photometric_G2` and zero
everywhere else.

**These numbers are from 2021 and should be re-measured before the device
is trusted in production.** They are defaults, not constants - nothing in
the driver depends on their values.

Selecting a position moves the lateral axis and, on arrival, pushes that
position's offset into `FOC_FILTEROFF`, which re-focuses. The focus block
is raised before the lateral block is dropped, so no exposure can start in
the gap between the two moves.

`CAM_POS` is writable for a deliberate move to a raw step count, the way
the Qt panel's spin box allows. Such a move implies no focus offset - but
if it happens to land on a configured position, the device adopts that
position, so `filter` and `FOC_FILTEROFF` never go on describing somewhere
the head no longer is.

## Values

| value | FITS | meaning |
|---|---|---|
| `FOC_POS` `FOC_TAR` `FOC_DEF` | yes | focus axis, standard RTS2 focuser set |
| `FOC_FILTEROFF` `FOC_FOFF` `FOC_TOFF` | yes | offset stack; `FOC_FILTEROFF` is driven by the lateral position |
| `filter` | see below | lateral position as an RTS2 selection - the control value, used by scripts |
| `FIBSEL` | yes | lateral position **by name**, for the FITS header |
| `CAM_POS` | yes | lateral axis in steps, writable |
| `CAM_TAR` | no | lateral target |
| `G1_POWER` `G2_POWER` | yes | camera power rails, writable |
| `ESW1A` `ESW1B` `ESW2A` `ESW2B` | no | end switches, true = at limit |
| `VOLT_33` `VOLT_50` `VOLT_120` | no | supply voltages in volts (the server reports tenths) |
| `toptec_conn` `toptec_url` | no | link health and where it points |

Commands beyond the focuser/filter standard ones: `reset` (the panel's
Reset button) and `focus_up` / `focus_down` / `cam_up` / `cam_down` (its
`+`/`-` buttons, one controller step each - the hardware never reports what
its increment is, so these bypass `FOC_TAR`/`filter` and the poll picks the
new position up like any other change).

### Why FIBSEL exists, and what should replace it

`filter` cannot be the value that carries the fibre name into the header.
C++ `Camera` creates its own value called `filter` too (`camd.cpp`,
`camFilterVal`), both map to the FITS keyword `FILTER`, and the camera wins -
so the fibre feed loses to a camera that has no filter wheel at all. Verified
on real data: the card read `FILTER = 'UNK'` with the camera's comment.

The *standard* RTS2 answer is not a bespoke keyword. A camera given
`--wheeldev <dev>` creates `FILTA`, `FILTB`, ... - each a `ValueSelection`
with `write_to_fits`, so RTS2 already writes the filter **name**. That is the
right long-term home for this, and it scales to the guiding and photometric
cameras when they come under RTS2 control.

**It does not work yet, and enabling it is actively harmful.** Tried on
FLORES: the camera created `wheelA = "TOPTEC"` and `FILTA`, the *name list*
propagated correctly over the wire - but the index stayed pinned at 0, so an
exposure taken with the head at `Photometric_G2` recorded
`FILTA = 'CCD700_object_1fiber'`. A header that confidently states the wrong
fibre is worse than one that says nothing. The cause is in the ported base
tree: `ClientFilterCamera` was deferred during the camd port, so
`Camera::createOtherType()` falls through for `DEVICE_TYPE_FW` and nothing
ever updates `FILTA` from the wheel.

So `--wheeldev TOPTEC` is **not** configured, and `FIBSEL` carries the truth
for now. Once `ClientFilterCamera` is ported, `FILTA` becomes canonical and
`FIBSEL` can be retired - or kept as a duplicate under a name that does not
depend on which camera the head is feeding.

## Getting the values into FITS headers

Because this device registers as `DEVICE_TYPE_FOCUS`, RTS2's image writer
handles it with `DevClientFocusImage`, which writes a focuser's values into
an image **only if the camera names that focuser** (`rts2image::Image::
getFocuserName()` must match the device name). A sensor-typed device is
written unconditionally; a focuser is not.

So the camera has to be told this device is its focuser, or none of
`FOC_POS`, `FIBSEL`, `CAM_POS`, `G1_POWER` ... will appear in the header.
That is the one piece of wiring the driver cannot do for itself:

    camd    fli     FLI     -c -30 --focdev TOPTEC

Confirmed on real FLORES data - an exposure taken with the head at
`Photometric_G2` recorded:

    FOC_NAME= 'TOPTEC  '           / name of focuser
    FOC_POS =                5422. / focuser position
    FOC_DEF =                4750. / default target value
    HIERARCH FOC_FILTEROFF =  672. / offset related to actual filter
    FIBSEL  = 'Photometric_G2'     / fibre/camera feed selected by the lateral stage
    CAM_POS =                15625 / [steps] lateral (fibre selector) axis position
    G1_POWER=                    F / G1 pointing camera power
    G2_POWER=                    F / G2 photometric camera power

`FOC_FILTEROFF` lands as a `HIERARCH` card because it is longer than the
eight characters a plain FITS keyword allows - normal, not a fault.

## Link handling

All XML-RPC traffic runs on one background thread, so no network round trip
can stall the RTS2 event loop, and a hung server cannot wedge the device.
Commands are queued and drained before each poll.

A server that is down at startup is not fatal: the device comes up, stays
`NOT_READY`, and adopts the head's real focus as `FOC_DEF` on the first
successful poll. A server lost later sets `ERROR_HW` (not `NOT_READY`, so
clients still get answered and moves fail with a clear error rather than
blocking forever), logs once rather than once per poll, and recovers on its
own when the server returns.

## Testing without the hardware

`tools/toptec_sim.py` is a stand-in for Fuky's XML-RPC server - the same
method surface, with both axes travelling at a settable rate:

    tools/toptec_sim.py 16000 &
    rtspy-focusd-toptec -d TOPTEC -i --toptec-host 127.0.0.1 --toptec-port 16000 \
        --lock-prefix /tmp/rts2_

Enough to exercise moves, the focus offset, camera power, the nudges,
reset, and the server going away and coming back.

## Not covered

The Qt panel drives three more servers, none of which are this head:

- the Quido box on `:6001` - relays 1 COMP and 2 FLAT are the calibration
  lamps, plus switchboard temperature. Metadata-relevant and scriptable;
  natural next device, sensor-typed.
- the iodine cell on the spectrograph server `:8888` (`SPCH 26 1`/`2`,
  `SPGS 26`). Belongs with the spectrograph, not the focus head.
- telescope parking on `:9999`. An RTS2 teld/dome concern for the 2m mount.

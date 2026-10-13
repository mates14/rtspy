# Perek 2m control daemons - protocol reference

Reverse-engineered (October 2026) from Jan Fuchs' clients:

- `observe-client.jar` r630 (Java/Swing "Observe", 2013), decompiled with CFR
- `observe_batch` (`observe_client.py`, PyQt5 batch observing, 2020-2024) and its logs
- `fiber_control_client.py` (the "toptec window", 2021)

## Architecture

Every subsystem is a separate **XML-RPC** daemon (plain HTTP, path `/`, no auth).
The Java "Observe" client goes through an HTTPS/REST proxy
(`https://alhena:443/observe`, Jersey + Basic auth + an IP allow-list), but that
proxy only forwards `functionName(functionParams)` to the daemons below over
XML-RPC (`http://host:port/RPC2`). `observe_batch` skips the proxy and calls the
daemons directly, which is what an RTS2 driver should do too.

| daemon | host | port | method prefix |
|---|---|---|---|
| telescoped (ASCOL TCS) | alhena = 192.168.193.195 | 9999 | `telescope_*` |
| spectrographd | alhena = 192.168.193.195 | 8888 | `spectrograph_*` |
| CCD700 exposure | alhena | 5002 | `expose_*` |
| OES exposure | almisan | 5001 | `expose_*` |
| G2 photometry camera | 192.168.193.198 | 7002 | own API, see below |
| TOPTEC head | 192.168.193.198 | 6000 | `toptec_*` (see `toptec.md`) |
| Quido relays | 192.168.193.198 | 6001 | `quido_*` |

The jar also knows a CCD400 (`observe.xmlrpc.ccd400.*`); that camera is gone.

Most commands answer the string `"1"` when they are accepted. That means *accepted*,
not *done*: completion has to be found by polling.

---

## Telescope - `telescope_*` (ASCOL)

- `telescope_execute(cmd: str) -> str` sends one raw ASCOL command line; it returns `"1"` if accepted.
- `telescope_set_coordinates({"ra": "HHMMSS.ss", "dec": "+DDMMSS.ss", "position": 0|1, "object": str}) -> 1` presets the
  target (newer API, used by observe_batch; the Java client used `TSRA` instead).
  `position` 0 = east, 1 = west.
- `telescope_info() -> dict`, every value a string:

| key | content |
|---|---|
| `ut` | `"yyyy-MM-dd HH:mm:ss"` UTC |
| `glst` | 10 space-separated ints, see below |
| `trrd` | current `"HHMMSS.ss ±DDMMSS.s P"`, P = 0 east / 1 west |
| `tsra` | last preset target `"ra dec P"` (same format) |
| `trhd` | hour axis and declination axis positions, degrees `"ha da"` |
| `trgv` | guide corrections `"ra dec"` (arcsec) |
| `trus` | user speeds `"ra dec"` |
| `dopo` | dome azimuth, degrees |
| `fopo` | telescope (secondary) focus position |
| `trcs` | pointing model number |
| `object` | object name of the last preset |

### GLST

Field *i* holds a raw value *r*; the state is `TABLE[r+1]`, so -1 means UNKNOWN.

| idx | subsystem | r = 0, 1, 2, ... |
|---|---|---|
| 0 | oil | OFF, START1 (N2 pressure check), START2 (pumps), START3 (pressure stabilisation), ON, OFF_DELAY |
| 1 | telescope | INIT, OFF, OFF_WAIT, STOP, **TRACK (4)**, OFF_REQ, SS_CLU1, SS_SLEW, SS_DECC2, SS_CLU2, SS_DECC3, SS_CLU3, ST_DECC1, ST_CLU1, ST_SLEW, ST_DECC2, ST_CLU2, ST_DECC3, ST_CLU3 (SS_* = slew to HA/DA "source", ST_* = slew to RA/Dec "star") |
| 2 | HA axis | STOP, **POSITION (1)**, CA_CLU1, CA_FAST, CA_FASTBR, CA_CLU2, CA_SLOW, MO_BR, MO_CLU1, MO_FAST, MO_FASTBR, MO_CLU2, MO_SLOW, MO_SLOWEST |
| 3 | Dec axis | as HA, plus CENM_SLOWBR, CENM_CLU3, CENM_CEN, CENM_BR, CENM_CLU4, CENA_SLOWBR, CENA_CLU3, CENA_CEN, CENA_BR, CENA_CLU4 (manual/automatic centring) |
| 4 | focus | OFF, STOP, PLUS, MINUS, SLEW, CAL1, CAL2 |
| 5 | dome | OFF, STOP, PLUS, MINUS, SLEW_PLUS, SLEW_MINUS, **AUTO_STOP (6)**, AUTO_PLUS, AUTO_MINUS, CALIBRATION |
| 6 | dome slit | UNDEF, OPENING, CLOSING, OPEN, **CLOSE (4)** |
| 7 | tube flap | UNDEF, OPENING, CLOSING, OPEN, CLOSE |
| 8 | mirror flap | UNDEF, OPENING, CLOSING, OPEN, CLOSE |
| 9 | bitfield | b0 HA, b1 Dec, b2 dome, b3 focus, b4 aberration, b5 precession, b6 refraction, b7 model, b8 guide mode |

observe_batch counts a slew as finished when `glst[1] == 4` (TRACK) and `glst[5] == 6`
(dome AUTO_STOP), and `trrd` matches the target to within 10" with the right side.
Parking counts as finished when `glst[1..3] == 3 1 1`.

### ASCOL commands seen in the clients

| cmd | meaning |
|---|---|
| `TEON 0/1` | telescope off/on |
| `OION 0/1` | oil off/on |
| `TETR 0/1` | tracking off/on |
| `TSRA ra dec pos` | set RA/Dec target (`HHMMSS.ss ±DDMMSS.ss 0/1`) |
| `TGRA 1` / `TGRA 0` | go to RA/Dec / stop slew |
| `TSHA ha da` | set hour-axis/dec-axis target, degrees (HA -180..330, DA -90..270) |
| `TGHA 1` | go to HA/DA |
| `TEHC 0/1`, `TEDC 0/1` | HA / Dec axis calibration |
| `TSGV ra dec` | set guide corrections (ints) |
| `TSCA`, `TSCP`, `TSCR`, `TSCM` `0/1` | aberration, precession, refraction, model corrections |
| `TSCS n` | select pointing model |
| `TSGM 0/1` | guide mode |
| `DOSO 0/1` | dome slit close/open |
| `FTOC 0/1` | tube flap close/open |
| `FMOC 0/1` | mirror flap close/open |
| `DOSA az` + `DOGA` | dome set absolute + go |
| `DOSR d` + `DOGR` | dome set relative + go |
| `DOAM` / `DOCA` / `DOST` | dome automatic / calibrate / stop |
| `FOSA x` + `FOGA` | telescope focus set absolute + go |
| `FOSR d` + `FOGR` | telescope focus set relative + go |
| `FOCA` / `FOST` | focus calibrate / stop |

Park sequence (fiber_control_client): `DOST, TETR 0, DOSO 0, FTOC 0, FMOC 0, DOSA 0,
DOGA, TSHA 10 80, TGHA 1, TEHC 1, TEDC 1, TSHA 90 45, TGHA 1, TEON 0, OION 0`. It waits
on the GLST after each of `TGHA 1`, `TEHC 1`, `TEDC 1` and `DOSO 0`.

---

## Spectrograph - `spectrograph_*`

- `spectrograph_execute(cmd: str) -> str` returns `"1"` if a set command was accepted; a get command returns its value.
- `spectrograph_info() -> dict` has keys `GLST`, `SPGP_4`, `SPGP_5`, `SPGP_13`, `SPCE_14`,
  `SPFE_14`, `SPCE_24`, `SPFE_24`, `SPGP_22`, `SPGS_19`, `SPGS_20` (strings; strip them).

Elements are numbered from 1 in commands; `GLST` field *n-1* is the state of element *n*.

| cmd | meaning |
|---|---|
| `SPCH n v` | change a discrete element to v (0 = stop) |
| `SPAP n v` / `SPRP n v` | absolute / relative position |
| `SPST n` | stop |
| `SPCA n` | calibrate (home) |
| `SPGP n` / `SPGS n` | get position / get state |
| `SSTE n` / `SSPE n` | start / stop an exposure meter |
| `SPCE n` / `SPFE n` | exposure meter counts / frequency |

| n | element | GLST states (others = position) |
|---|---|---|
| 1 | dichroic mirror | 0 stop, 5 moving, 6 timeout |
| 2 | **spectral (order) filter**, positions 1-5 | 0 stop, 6 moving, 7 timeout |
| 3 | Coude collimator | 0 stop, 1 open, 2 closed, 3 open left, 4 open right, 5 moving, 6 timeout |
| 4 | camera 700 focus (`SPGP 4`) | 0 stop, 1 moving |
| 5 | camera 400 focus (`SPGP 5`) | 0 stop, 1 moving |
| 6 | star / calibration | 1 star, 2 calibration, 0 stop, 3 moving, 4 timeout |
| 7 | Coude / OES | 1 Coude, 2 OES, 0 stop, 3 moving, 4 timeout |
| 8 | flat lamp | 0 off, else on |
| 9 | comparison lamp | 0 off, else on |
| 10 | exposure meter shutter | 1 open, 2 closed, 0/3/4 stop/moving/timeout |
| 11 | camera 700 shutter | same |
| 12 | camera 400 shutter | same |
| 13 | grating (`SPGP 13`) | 0 stop, 1 moving, 2 timeout |
| 14 | exposure meter (`SPCE/SPFE 14`) | 0 stopped, else counting |
| 15 | Coude slit camera filter | 0 stop, 6 moving, 7 timeout |
| 16 | corrector 700 | 0 unknown, 1 in, 2 out |
| 17 | corrector 400 | same |
| 18-20 | (unused; 19 and 20 are still polled) | |
| 21 | OES collimator | as 3 |
| 22 | OES focus (`SPGP 22`) | 0 stop, 1 moving |
| 23 | OES exposure meter shutter | as 10 |
| 24 | OES exposure meter (`SPCE/SPFE 24`) | as 14 |
| 25 | (unused) | |
| 26 | iodine cell | **set 1 = remove, 2 = insert; read 1 = inserted, 2 = removed**, 0 stop, 3 moving, 4 alarm |
| 27 | Coude slit camera power | 0 off, 1 on |
| 28 | OES slit camera power | 0 off, 1 on |

Grating, in steps ↔ angle in degrees (two separately fitted lines, not exact inverses):

    steps = round(-205.294 * deg + 12667.1)      # used for SPAP 13
    deg   = -0.00487106 * steps + 61.7024        # used for display

The CCD700 spectral range presets (grating angle + spectral filter + flat/comp
times) are in `observe_client.cfg` as the `[ccd700_grating_angle_*]` sections.

---

## Exposure daemons (CCD700, OES) - `expose_*`

Each reply is a string `"+OK ..."` or `"-ERR ..."`.

| method | notes |
|---|---|
| `expose_info() -> dict` | `state`, `elapsed_time`, `full_time`, `expose_number`, `expose_count`, `ccd_temp` (float), `filename`, `path`, `archive`, `archive_path`, `paths`, `archive_paths`, `instrument` |
| `expose_set(key, value)` | keys: `READOUT_SPEED` (ccd700 `50kHz`, oes `100kHz`), `GAIN` (ccd700 `high`, oes `default`), `PATH` (output dir), `ARCHIVE` (`1`/`0`) |
| `expose_get(key)` | `READOUT_SPEEDS`, `GAINS`; the answer looks like `+OK GAINS = a,b,... / comment` |
| `expose_set_key(key, value, comment)` | FITS header card, e.g. `IMAGETYP` (zero/dark/flat/comp/object), `OBJECT`, `OBSERVER`, OES also `SPECFILT`, `SLITHEIG`; an empty value gives `-ERR unknown value` |
| `expose_get_key(key)` | `+OK KEY = value / comment` |
| `expose_start(time, count, meter)` | time in s; meter in counts (`Mcounts * 1e6`), or `-1` = no meter; with a meter, time is `-1` = run until the meter limit. Returns `-ERR expose already running` if busy |
| `expose_time_update(t)`, `expose_meter_update(m)` | change the limits while exposing |
| `expose_abort()` | discard the exposure |
| `expose_readout()` | end the exposure early and read it out |

`state` moves through `ready` → `prepare expose` → `expose` → `finish expose` → `readout` → `ready`.
The CCD temperature counts as in alarm outside -150..-98 °C.
The daemon writes the FITS file itself to `PATH` (`/data/CCD700.RAW/INCOMING`,
`/data/OES/INCOMING`, `.../TECH` for technical frames). Image data never comes over XML-RPC.

## G2 photometric camera (192.168.193.198:7002)

`set_filter(n)`, `start_exposure(time, ?, count, ?, imagetyp, object, observers)`
(observe_batch passes `(t, 0, n, 0, ...)`), `get_status() -> {"status": 1 = ready, ...}`.
FITS files go to `/data/photometry/INCOMING`.

## Quido relay board (192.168.193.198:6001)

`quido_set_relay(n, 0/1) -> True`: relay 1 = comparison lamp, 2 = flat lamp (fibre calibration
at the primary focus, used with the TOPTEC comp/flat positions).
`quido_get_relays() -> [r1, r2, r3, r4]`, `quido_get_temperature()`.

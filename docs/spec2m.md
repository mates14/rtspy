# Coude/OES spectrograph (Perek 2m)

`rtspy/drivers/spec2m.py` is an RTS2 device for the spectrograph room of the
Perek 2m at Ondrejov. It is a client of Jan Fuchs' spectrographd (alhena:8888,
XML-RPC), the server behind the spectrograph panel of his Observe client. The
protocol is in `perek2m-protocol.md`.

## What it is

The **spectral (order sorting) filter**, element 2, decides where the light
goes: position 4 feeds FLORES, the other positions the CCD700. So it becomes
the device's filter wheel (`DeviceType.FW`), and a camera can use it with
`--wheeldev SPEC`. The default names are `1:2:3:FLORES:5`. Renaming them with
`-F` changes only the labels: selection index *i* is always position *i+1*.

Every other element is a plain value, writable when spectrographd takes a
command for it.

## Behaviour

- **The driver never commands anything on its own.** The spectrograph is shared
  with observers using Fuky's clients, so the driver does not move anything at
  startup, and has no default filter unless `--default-filter` is given. When
  someone else moves the spectral filter, `filter` follows it.
- **`--readonly`** turns the driver into a monitor. It refuses every write and
  command except the read-only `spexec SPG*/SPCE/SPFE`. Use this while the
  telescope is still run by hand.
- **Exposures are blocked while anything moves** (`BOP_EXPOSURE`), no matter who
  moved it. That covers the dichroic, collimators, star/calibration,
  Coude/OES, shutters, slit camera filter, iodine cell, focus axes, grating
  and spectral filter.
- A `filter` move is answered when GLST shows the new position. It fails when
  the element reports *timeout* (state 7), when the move takes longer than
  `--filter-timeout` (default 60 s), or when spectrographd refuses the command.
- Any other write is answered as soon as the command is queued. spectrographd
  answers only "accepted", so a write that turns out to be refused is logged
  and left in `last_error`, and the next poll restores the real value.
- When the server is unreachable, the device goes to `ERROR_HW` and refuses
  moves. When the server comes back, the device recovers by itself.

## Values

| value | FITS | meaning |
|---|---|---|
| `filter` | no | spectral filter as an RTS2 selection, the control value |
| `SPECFILT` | yes | spectral filter by name, or `moving`/`timeout`/`stop` |
| `DICHROIC` | yes | dichroic mirror, positions 1-4 |
| `COLLIM` `OESCOLL` | yes | Coude / OES collimator: open, closed, open left, open right |
| `STARCAL` | yes | star / calibration feed |
| `COUDEOES` | yes | Coude / OES feed |
| `EMSHUT` `OESEMSHT` | yes | exposure meter shutters (Coude, OES) |
| `SHUT700` `SHUT400` | yes | camera shutters |
| `SLCFILT` | yes | Coude slit camera filter, 1-5 |
| `CORR700` `CORR400` | yes | camera correctors, in/out (read-only) |
| `IODINE` | yes | iodine cell: *inserted* / *removed*. You set the state you want to read back; the driver handles the server's inverted command codes |
| `SLCAMPWR` `OESSLPWR` | no | slit camera power |
| `FOC700` `FOC400` `FOCOES` | yes | camera focus positions in steps, writable (`SPAP`) |
| `FOC700ST` ... | no | focus axis stop/moving |
| `GRATANG` | yes | grating angle in degrees, writable |
| `GRATPOS` | yes | grating position in steps, writable |
| `GRATST` | no | grating stop/moving/timeout |
| `EMRUN` `OESEMRUN` | no | exposure meter counting, writable (`SSTE`/`SSPE`) |
| `EMCNT` `OESEMCNT` | yes | exposure meter counts |
| `EMFRQ` `OESEMFRQ` | no | exposure meter count rate |
| `GLST` `SPGS19` `SPGS20` | no | raw telemetry |
| `spec_conn` `spec_url` `spec_readonly` `last_error` `spexec_answer` | no | housekeeping |

Every selection has an extra entry, `unknown`, for raw states outside the
known table.

Grating angle and steps are converted with Fuky's two fitted lines
(`steps = round(-205.294 deg + 12667.1)`, `deg = -0.00487106 steps + 61.7024`).
They are not exact inverses: a requested 35.0° reads back as 34.9992°.

## Commands

| command | does |
|---|---|
| `filter <n>` | spectral filter to index n, answered when it arrives |
| `spcal <n>` | calibrate (home) focus axis n = 4, 5, 22 (`SPCA`) |
| `spstop <n>` | stop element n (`SPST`) |
| `sprel <n> <steps>` | relative move of n = 4, 5, 13, 22 (`SPRP`) |
| `spexec <cmd>` | any raw spectrographd command; the answer goes to `spexec_answer` |

These are answered once spectrographd has replied, not when they are queued.

## Running it

    rtspy-filterd-spec2m -d SPEC --server <centrald> --spec-host 192.168.193.195 [--readonly]

The matching line in `/etc/rts2/devices`:

    filterd  spec2m  SPEC  --spec-host 192.168.193.195

## Testing without the hardware

    tools/spec2m_sim.py 18888 &
    rtspy-filterd-spec2m -d SPEC -i --spec-host 127.0.0.1 --spec-port 18888 \
        --lock-prefix /tmp/rts2_

The simulator listens only on 127.0.0.1. It models motion through the moving
states (2 s for discrete elements, 500 steps/s for axes), and the iodine cell
inversion. To test a move made from Fuky's panel, call
`spectrograph_execute("SPCH 2 1")` on it directly.

## Left out on purpose

Elements 8 and 9, the spectrograph flat and comparison lamps (the "Flat
field" / "Comp" rows of the ObserveClient Spectrograph tab), have no values.
Those lamps are permanently gone. The working fibre lamps are TOPTEC's
`FIBCOMP` and `FIBFLAT`.

## Not covered (yet)

- What element 19, element 20 and the slit camera filter positions mean.
- The exact element range of `SPCA`: only 4, 5 and 22 are known from the
  clients. `spexec` reaches anything else.
- Real-hardware timing. The simulator's movement times are guesses.

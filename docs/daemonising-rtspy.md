# Daemonising rtspy

*Design analysis — 11 Sep 2026 · **implemented**, see "What was built" at the end*

What it takes for a Python RTS2 driver to start, lock, background and stop
exactly the way a C++ one does, so `rts2-start`, `rts2-stop` and systemd can
drive it without a shell wrapper in the middle.

Every divergence and every prototype result below was executed, not inferred.

Sources read: `base/kernel/src/daemon.cpp`, `base/kernel/src/app.cpp`,
`base/kernel/src/multidev.cpp`, `base/packaging/rts2-start.in`,
`base/packaging/rts2@.service.in`, `base/packaging/rts2-rsyslog.conf`,
`rtspy/core/{app,device,config,netman,value}.py`.

---

## Root cause of the duplicate instances

**`rts2-start` and the rtspy wrapper disagree about what a lock file is.**

The rewritten `rts2-start` decides whether a daemon is running by taking an
`flock` on `/var/run/rts2_<name>` — deliberately, because a PID can go stale
and a kernel lock cannot. An rtspy daemon never takes that lock; the wrapper
only writes a PID into the file. So the flock always succeeds, `is_running()`
always answers *not running*, and `rts2-start` launches another copy — every
time it is asked.

Nothing downstream catches it either. The rtspy device binds an ephemeral port
with `SO_REUSEADDR`, so two instances never collide on a socket; they both
register with centrald and both answer to the same device name.

---

## 1. The interface — what the launcher actually requires

This is not a matter of taste. `rts2-start`, `rts2-stop` and `rts2@.service`
form a fixed contract, and anything that wants to be driven by them has to
satisfy it.

| Obligation | What is required |
|---|---|
| **program name** | `rts2-<family>-<type>` found on `PATH`. A `devices` line `focusd toptec F0 [opts]` is invoked as `rts2-focusd-toptec -d F0 [opts]`. |
| **exit 0** | The daemon is up — or is still initialising past `--daemonize-timeout` and has been left running. |
| **exit 255** | The lock is held by another instance. `RTS2_EXIT_ALREADY_RUNNING`. |
| **exit 254** | The lock file cannot be used at all — permissions, missing directory. `RTS2_EXIT_LOCK_ERROR`. Kept distinct from 255 precisely so "already running" stops being a lie. |
| **any other** | Startup failed. `rts2-start` captures stderr and prints it under the failure line. |
| **lock file** | `/var/run/rts2_<device>`, opened `O_RDWR\|O_CREAT` 0666 under umask 022, then `flock(LOCK_EX\|LOCK_NB)` held for the process lifetime. The PID is written into it. |
| **"is it running"** | Answered by the flock, never the PID. The PID is read only to decide what to kill. |
| **fork** | The parent must not exit until the child has finished initialising — that is what makes the exit status meaningful, and what `Type=forking` plus `PIDFile=` assume. |
| **readiness** | Declared after init, values and `beforeRun()` — everything that can fail. Centrald registration is explicitly *not* part of it. |
| **stdio** | Kept open until readiness, so anything logged while initialising reaches the terminal that started the daemon; redirected to `/dev/null` only after. |
| **`-i`** | No fork, no console detach — the operator runs `rts2-camd-andor -i` directly. The lock is still taken and the PID still written, so `-i` is **not** a way around mutual exclusion: starting one interactively while it already runs still exits 255. |
| **stop** | `SIGTERM`, up to 10 s, then `SIGKILL` — and it refuses to kill at all unless `/proc/<pid>/comm` equals the first 15 characters of the program name. |
| **syslog** | Ident `rts2`, so the rsyslog rule routes every daemon into `/var/log/rts2.log`. |

---

## 2. Gap analysis — where rtspy diverges today

Nine divergences, each checked against the running code. Two fail loudly, one
fails silently, and the rest quietly make the launcher's reports untrue.

### 01 — No lock is ever taken · *causes duplicates*

No rtspy daemon calls `flock`. Mutual exclusion lives entirely in the shell
wrapper's PID check, which `rts2-start` does not consult and systemd does not
know about.

### 02 — The wrapper's PID logic can lose a daemon · *causes duplicates*

Check-then-write with no atomicity, so two concurrent starts both see a free
slot. Worse, the PID is discovered *after* launch with
`pgrep -P "$SUDO_PID" -f "$PYTHON_SCRIPT"` following a fixed `sleep 0.5` — if
that comes back empty the wrapper exits with an error while the daemon it just
started keeps running, now with nothing recording it at all. The next start
adds a second one.

### 03 — No daemonisation, so the exit status means nothing · *reports success on failure*

rtspy runs in the foreground; the wrapper backgrounds it with `nohup … &`. The
status therefore reports whether the shell managed to fork, never whether the
device came up. A driver that dies in `start()` a second later still reports
success.

### 04 — `-i` is a hard error · *no interactive run*

Running a driver on the terminal the way you would run `rts2-camd-andor -i` is
simply unavailable for any rtspy daemon — argparse rejects the flag before
anything runs.

```
$ rts2-focusd-toptec -d F0 -i --debug
error: unrecognized arguments: -i
```

### 05 — Two option spellings disagree with C++ · *one fails silently*

`--local-port` does not exist in rtspy, which calls it `-P/--port`. That one at
least fails loudly. `--server` is the dangerous one: C++ takes `host:port` in a
single argument, rtspy takes a bare hostname plus a separate `--server-port`,
and it accepts the combined form without complaint — storing the whole string
as the hostname and then trying to resolve it.

```
$ rts2-focusd-toptec -d F0 --local-port 1234
error: unrecognized arguments: --local-port 1234

$ rts2-focusd-toptec -d F0 --server localhost:18617 --show-config
  server      = localhost:18617    # taken as a hostname
  server_port = 617                # the :18617 is ignored
```

Any other C++ option appearing in a `devices` line — `--modefile`,
`--valuefile`, `--noauth`, `--localhost` — aborts startup the same way `-i`
does.

### 06 — No signal handlers at all · *unclean shutdown*

There is not one `signal.` call anywhere in rtspy. `rts2-stop` sends `SIGTERM`,
Python's default action terminates immediately, and `App.run()`'s
`finally: device.stop()` never runs — hardware threads are not stopped, serial
ports and sockets are not closed. Only Ctrl-C, which raises `KeyboardInterrupt`,
shuts down cleanly today.

### 07 — Syslog goes somewhere else · *operational*

C++ daemons `openlog("rts2", LOG_PID, LOG_DAEMON)`, and the shipped rsyslog
rule matches on exactly that programname to route them into
`/var/log/rts2.log`. rtspy uses the default ident and `LOG_LOCAL0`, so its
lines miss the site log and land in a separate rotating file instead.

### 08 — The wrapper hardcodes one deployment · *operational*

`RUN_USER="mates"` and `LOG_FILE=/home/mates/rtspy.log` are baked into the
shell script. C++ expresses the same thing as `--run-as`, decided per daemon in
the config.

### 09 — The wrapper renames the process out from under `rts2-stop` · *latent*

Worth stating because it is the trap the current design walks toward, not one
it has hit. `rts2-stop` compares `/proc/<pid>/comm` against the program name
before killing anything. Running `rts2-focusd-toptec` as a wrapper that execs
`rtspy-focusd-toptec` makes those two names differ, and the kill is refused.
Dropping the wrapper fixes it for free — see §3.

---

## 3. Feasibility — all of it works in plain Python

Before proposing a rewrite of rtspy's startup I built the whole contract as a
standalone prototype — lock, fork, self-pipe handshake, timeout, PID file — in
about 90 lines of stdlib, and drove it with the same shell functions
`rts2-start` uses. No `prctl`, no `setproctitle`, no third-party daemon
library.

| Path exercised | Result | |
|---|---|---|
| normal start | returns in 0.036 s, exit 0, daemon in background | pass |
| start while running | exit 255 | pass |
| failure during init | stderr reaches the terminal, real exit status 12, nothing left running | pass |
| init slower than timeout | "still initialising after 2 s", exit 0, daemon comes up anyway | pass |
| `-i` interactive | stays in the foreground, no fork | pass |
| SIGTERM | process ends, kernel drops the lock, file goes stale-free | pass |
| Python flock ↔ shell `flock -n -x 9` | agree in both directions, before and after exit | pass |
| `/proc/pid/comm` check | matches the program name — see below | pass |

One result is worth calling out because it **removes** work rather than adding
it. I expected `comm` to read `python3` for a Python entry point, which would
have meant a `prctl(PR_SET_NAME)` call to satisfy `rts2-stop`. It does not:
Linux takes `comm` from the *shebang script's* own name, not the interpreter's.
A console script installed as `rts2-focusd-toptec` reports `rts2-focusd-top` —
exactly the 15 characters `rts2-stop` compares against. Installing the Python
entry point under the `rts2-` name and deleting the shell wrapper is therefore
both simpler and more correct than what we have now.

---

## 4. Proposed design — startup order

Mirrors C++ step for step. Steps that decide the exit status are marked `*`;
the one hazard in the current code is marked `!`.

### C++ RTS2 today — `Daemon::init` → `run`

```
 1   parse options
 2 * checkLockFile           -> 255 / 254
 3 * doDaemonize             -- fork
 4   switchUser              (--run-as)
 5   lockFile                -- write PID
 6   setupAutoRestart
 7   bind + listen
 8   initValues, initHardware
 9   beforeRun
10 * daemonizeReady          -- 1 byte, drop console
11   run loop
```

The fork sits before any thread a driver might create — deliberately, because
forking after libusb or a vendor SDK has spun up threads hands the child locked
mutexes with no owners.

### rtspy today — `App.parse_args` → `create_device` → `run`

```
 1   parse args
 2   (no lock)
 3   (no fork)
 4   (no --run-as)
 5   (no PID file)
 6   construct device
 7   resolve config
 8   network.start           -- bind, 2 threads
 9   device.start            -- driver threads
10   (no readiness signal)
11   run loop (sleep 10)
12 ! wrapper forks here, after threads
```

Backgrounding happens outside the process entirely, after everything is already
up and threaded — which is why nothing about the outcome can be reported.

### Proposed — `rtspy/core/daemon.py`

```
 1   parse args
 2   resolve config standalone
 3 * check_lock              -> 255 / 254
 4 * do_daemonize            -- fork
 5   drop privileges         (--run-as)
 6   write PID into lock fd
 7   install SIGTERM / SIGINT / SIGHUP
 8   construct device, apply config
 9   network.start           -- bind, threads
10   device.start            -- hardware
11 * daemonize_ready         -- 1 byte, drop console
12   run loop -> device.stop
```

Step 2 is the enabler: config must resolve before the device exists, so the
lock name and `-i` are known while the process is still single-threaded.

---

## The handshake

The fork is early — before threads, before the socket, before any hardware.
What makes the exit status meaningful is not *when* the fork happens but the
pipe held across it: the parent, the process the shell is waiting on, does not
exit until the background daemon says it got all the way up. Same self-pipe as
C++, and the prototype behaves identically.

```
  rts2-focusd-toptec -d F0            <- the process rts2-start waits on
        |
        +-- check_lock()  ------------  held by another instance -> exit 255, never forks
        |
        +-- pipe() ; fork()
        |
   PARENT                               CHILD
   close(w)                             close(r) ; setsid()
   poll(r, --daemonize-timeout)         write PID into the lock fd
        |                               drop privileges (--run-as)
        |                               install SIGTERM / SIGINT / SIGHUP
        |                               construct device, apply config
        |                               bind + start threads
        |                               device.start()   <- stderr still on the terminal
        |                                     |
        | <--------- 1 byte -----------------+  daemonize_ready()
        |                                     |  stdio -> /dev/null
   exit 0                                run loop
```

| What the parent's poll sees | Meaning | Parent exits |
|---|---|---|
| **one byte** | The daemon finished initialising and is running. | **0** — "started" |
| **EOF, no byte** | The child died before reporting. Its stderr already reached the terminal; the parent reaps it for the real status. | **the child's own status** — "FAILED (exit N)" |
| **nothing, timed out** | Still initialising after 120 s. Not treated as failure — slow hardware is not broken hardware. | **0**, with a note, child left running |

Two properties fall out of this order that matter for the problem actually
being hit:

- The lock is tested **before** the fork, so "already running" costs no process
  at all and cannot itself race.
- Because the lock lives on the open file *description*, the child inherits the
  very same lock across the fork — the parent's copy closing on exit does not
  release it. Verified: the shell's `flock` probe reported the daemon running
  immediately after the parent returned.

---

## The one structural change

**The fork has to happen before any thread starts, and today the code makes
that impossible.**

`App.create_device()` resolves the configuration, constructs the device, starts
the NetworkManager's two threads and runs the driver's `start()` — all in one
call. The device name and lock prefix are only known after the first of those,
so as written there is no point at which we both know what to lock and have not
yet spawned threads.

The fix is already half-built. `DeviceConfig.register_options()` resolves a
class's whole argument set from a `cls.__new__(cls)` instance without running
`__init__` — so configuration can be resolved with no device and no threads in
existence. Splitting that resolution out of `create_device()` is what lets the
lock and the fork move ahead of everything else, and it is the only change in
this work that reaches into code other drivers share.

---

## 5. Decisions — all settled

All four are decided and built. Recorded here as rationale, not as open
questions.

### A — option compatibility · **chosen: aliases**

A `devices` line is written for whatever binary it names, and the centrald
config's shared options go to every daemon.

- **[chosen] Accept C++ spellings as aliases, keep rtspy's own.**
  `--local-port` aliases `--port`; `--server` learns to split `host:port`; the
  C++-only options that mean nothing here are accepted and ignored with a
  one-line warning. Unknown options still fail loudly.
- Switch to C++ spellings outright, rtspy's current names becoming deprecated
  aliases. Cleaner long-term, breaks every existing invocation.
- Ignore all unrecognised options silently so a `devices` line can never fail.
  Makes typos invisible — I would not.

### B — where it lives · **chosen: core/daemon.py + App.main()**

The daemon machinery is not device-specific.

- **[chosen] New `rtspy/core/daemon.py`, driven from `App`,** with a single
  `App.main(DeviceClass)` replacing the eight lines of identical boilerplate
  every driver's `main()` repeats today. Drivers change by one line each.
- Standalone module, each driver calls it itself. No change to `App`, but every
  driver has to get the order right on its own.

### C — how much of the C++ surface · **chosen: minimum + `--run-as`**

- **`--run-as` — built.** It is what the wrapper hardcoded, so it became
  load-bearing the moment the wrapper went.
- **`--autorestart` — accepted, not implemented.** Wanted eventually, but
  deliberately not allowed to slow this down. It parses, and says so.
- **`SIGHUP` — caught, logs that reload is not implemented.** Better than the
  default action, which would kill the daemon outright.
- **`--valuefile`, `--modefile`, `--autosave`, `--defaults`, `--noauth`,
  `--notcheck`, `--localhost` — accepted, inert,** and reported at startup so
  an ignored option can be found rather than wondered about. Real structural
  work, deferred.

### D — cutover · **chosen: wrapper deleted**

The wrapper goes; the Python app backgrounds itself and installs under the
`rts2-` name directly. Nothing is stranded mid-upgrade, since both names exist
today — but the running daemons have to be stopped with the current tooling
*before* the new package lands, or their locks end up held by processes the new
tooling will not recognise.

---

## 6. Scope

**In scope**

- New `rtspy/core/daemon.py` — lock, fork, handshake, PID, privileges, signals
- `rtspy/core/app.py` — split config resolution out of `create_device`; add
  `App.main()`
- `rtspy/core/config.py` — the C++ option aliases and `host:port` parsing
- Syslog through the `syslog` module with ident `rts2`
- `pyproject.toml` / `setup.py` — entry points renamed, wrappers dropped
- Each driver's `main()` reduced to one call

**Not in scope**

- Anything in the C++ tree — the contract is already there and already correct
- The wire protocol, values, or device logic
- The dead `idle()` path (separate, already flagged)
- `valuetool`'s broken write mode (separate, already flagged)
- MultiDev-style several-devices-per-process, unless wanted now

---

## What was built

Implemented 11 Sep 2026, in this order.

### `rtspy/core/daemon.py` (new)

The whole contract, stdlib only:

- `LockFile` — `flock(LOCK_EX|LOCK_NB)` on `/var/run/rts2_<device>`, creating
  the directory if a tmpfs reboot removed it (that case is a lock *error*, 254,
  never "already running").
- `do_daemonize()` / `daemonize_ready()` / `_wait_for_child()` — the fork and
  the self-pipe handshake, with the timeout behaviour above. Warns if it is
  ever called with threads already running.
- `drop_privileges()` — `--run-as user[.group]`, via `initgroups` + `setgid` +
  `setuid`. A failure is a failed startup; carrying on as root would be worse.
- `ShutdownRequest` — SIGTERM/SIGINT set an event the main thread waits on,
  SIGHUP logs that reload is not implemented, SIGPIPE ignored. A second
  terminating signal while already shutting down exits immediately.
- `Rts2SyslogHandler` — libc `syslog(3)` under ident `rts2`, so rtspy lines
  land in `/var/log/rts2.log` beside the C++ ones. Python's own
  `SysLogHandler` cannot set an ident, which is why this goes through the
  `syslog` module.

### `rtspy/core/app.py`

- `App.main(device_class)` — the whole sequence from §4, returning a process
  exit status. A driver's `main()` is now one line.
- `App.run()` waits on the shutdown event instead of `sleep(10)`, so SIGTERM
  reaches `device.stop()`.
- Console logging moved from stdout to **stderr**: that is what `rts2-start`
  captures and prints under a failure line.
- `_setup_early_logging()` for the pre-fork stage, so lock and privilege
  errors are visible at all.
- `RTS2LogFormatter.device_name` — pinned from the config, so lines logged
  before any `Device` exists still carry the device name instead of `UNKNOWN`.

### `rtspy/core/config.py`

- `ConfigArgument` honours an explicit `dest`, so adding a longer alias cannot
  silently rename the config key.
- `--local-port` accepted as an alias of `-P/--port`.
- `--server` accepts `host:port` and splits it; the port inside `--server`
  wins over `--server-port`.
- New daemon options: `-i/--interactive`, `--lock-prefix`, `--run-as`,
  `--daemonize-timeout`.
- `COMPAT_ARGUMENTS` — the C++-only options, accepted and inert, reported at
  startup by `unimplemented_options()`.
- `build_registry()` / `resolve_config()` — configuration resolved with no
  device and no threads, which is what lets the lock and fork come first.
  `process_args()` takes the already-resolved pair so nothing is parsed twice.

### Packaging

Entry points renamed `rtspy-*` → `rts2-*` (`rts2-focusd-toptec`,
`rts2-filterd-ovis`, `rts2-focusd-dummy`, `rts2-filterd-dummy`,
`rts2-sensor-temp`). The per-driver shell wrappers in `rtspy/scripts/` are
deleted; `rts2-gcnkafka`, `rts2-queuer` and `rts2-observe` remain, as they are
not daemons.

All five drivers migrated to `App.main()`.

### Verified against real centrald + a hardware simulator

| Case | Result |
|---|---|
| normal start | returns in 0.14 s, exit 0, backgrounded with no wrapper |
| lock held | `is_running()` (the real `rts2-start` flock probe) says YES; pid in file |
| `comm` | `rts2-focusd-top` — exactly what `rts2-stop` compares against |
| duplicate start | exit 255, still exactly one process |
| `-i` while running | exit 255 — interactive is not a loophole |
| `-i` otherwise | stays in foreground |
| SIGTERM | `device.stop()` runs, lock released |
| failed init (port in use) | **exit 12**, reason on stderr, nothing left running |
| `--server sulafat:9617` | `server=sulafat`, `server_port=9617` |
| `--local-port 4242` | `port=4242` |
| compat options | warned about individually, startup unaffected |
| un-migrated driver | still runs exactly as before |

### Left for later

- `--autorestart` — accepted, not implemented.
- `SIGHUP` reload — caught and logged, not implemented.
- `--valuefile` / `--modefile` / `--autosave` / `--defaults` — accepted, inert.
- A MultiDev equivalent (several devices in one process, one lock).
- `rtspy/scripts/rts2-queuer` and `rts2-gcnkafka` are services rather than
  devices; they still start the old way.

---

## Deployed at FLORES — 11 Sep 2026

Installed with `pip3 install -e . --break-system-packages --no-deps` as root
(`--no-deps` matters: pyproject declares astropy/numpy/pandas/gcn-kafka, none
of which the driver path needs). Verified against the **real TOPTEC head** at
192.168.193.198:6000 and the live centrald.

| Case | Result |
|---|---|
| `rts2-start TOPTEC` | `started`, exit 0, backgrounded, no wrapper |
| `rts2-start TOPTEC` again | `already running`, exit 255, **still one process** |
| `rts2-stop TOPTEC` | `killed`; log shows SIGTERM → `Stopping device TOPTEC` |
| restart | clean |
| lock | `/var/run/rts2_TOPTEC`, pid inside, `comm` = `rts2-focusd-top` |
| syslog | lines appear in `/var/log/rts2.log` under ident `rts2` |
| real hardware | FOC_POS 4700, CAM_POS 400, `filter=0` = `CCD700_object_1fiber` |

The 2021 fibre table's first entry is confirmed against live hardware: the head
was parked at 400 and the driver named it correctly. Focus 4700 against the
2021 `g1 = 4678` is 22 steps, consistent with a refocus rather than a stale
table. The 12 V rail reads 13.7 V - that is the supply, not a conversion bug.

### Two things the deployment turned up

**1. FLORES runs the classic `rts2-start`, not the rewritten one.** The
packaged `rts2-base` (Jul 2026) hardcodes `RTS2_BIN_PREFIX=/usr/bin` and does
not search `PATH`, so console scripts in `/usr/local/bin` are invisible to it;
`rts2-stop` kills whatever pid is in the lock file with no flock or `comm`
check. Bridged for now with a symlink:

    /usr/bin/rts2-focusd-toptec -> /usr/local/bin/rts2-focusd-toptec

Worth noting that the classic script works correctly with these daemons anyway:
it distinguishes only 255 / 0 / other, which is exactly "already running" /
"started" / "failed" - and now that exit 0 genuinely means the device came up,
its report is truthful without the script changing at all. Upgrading to the
rewritten `rts2-start` would additionally give PATH lookup, flock-based status
and the `comm` guard.

**2. Entry-point names can shadow the packaged C++ drivers.** Renaming every
rtspy console script to `rts2-*` made `rts2-focusd-dummy` and
`rts2-filterd-dummy` collide with the C++ drivers of the same name in
`/usr/bin` - and since `/usr/local/bin` precedes `/usr/bin`, rtspy's test
dummies silently shadowed the real ones. Fixed by keeping exactly those two as
`rtspy-focusd-dummy` / `rtspy-filterd-dummy`; the drivers with no C++
counterpart (`toptec`, `ovis`, `sensor-temp`) keep their `rts2-` names. Any
future rtspy driver that duplicates a C++ one needs the same care.

### Camera link

`/etc/rts2/devices` now reads:

    camd    fli     FLI     -c -30 --focdev TOPTEC
    focusd  toptec  TOPTEC  --toptec-host 192.168.193.198

`FLI.focuser` reads `"TOPTEC"`, which is the condition `DevClientFocusImage`
requires before it writes the focuser's values into an image. Linking only the
FLI is a deliberate simplification - the head serves the guiding camera G1 and
the photometric camera G2 as well, and that will need revisiting when those are
under RTS2 control.

### Still unproven on real hardware

No movement has been commanded. Focus and lateral moves, camera power and the
`+`/`-` nudges have only been exercised against the simulator. Metadata landing
in an actual FITS header also remains untested, since that needs an exposure.

# Reconnection: what recovers, what does not

How rtspy recovers lost links to centrald and to other devices, what is
known to fail, and what may. State of 2026-10-20 [MJ]. Update
this file when any of it changes.

## Background: what a registration is

- **The registration belongs to the TCP connection.** When centrald drops
  a connection it removes the device and sends `delete_device <id> <name>`
  to everybody. RTS2 has no way to resume a session.
- **Ids are never reused.** Each connection centrald accepts gets the next
  number, so reconnecting to the *same* centrald gives a new id and key,
  as a new centrald does.
- **A duplicate name is refused:** `name X already registered with id N`.
  centrald keeps a connection it has not noticed is dead until its own
  timeout drops it.
- **Device-to-device links** are authorized once, through centrald, when
  they are set up, and do not depend on centrald afterwards. Both C++ and
  rtspy keep them over a centrald reconnect.
- **The key** is one per requester connection on centrald. rtspy asks for
  it once, after `registered_as` (`key <own name>`), and uses it for every
  `auth` it sends.

## What recovers by itself

### The centrald link (`netman.py`)

- `_check_centrald()` runs in the network loop and connects whenever
  there is no centrald connection: at start (also when centrald is not up
  yet), after centrald restarts, and after the link drops.
- Timing: 2 s after losing a registered link (a restarted centrald is
  usually listening again by then), then 5 s doubling to 60 s (C++
  retries every 60 s). One warning for the loss and one for the first
  failed attempt, then DEBUG; one INFO when registered again.
- A refused `register` (e.g. "already registered") closes the link, which
  is then retried. A `register` with no answer closes it after 60 s
  (`clean_stale_connections`, centrald not `AUTH_OK`).
- On a loss (`_centrald_lost()`):
  - `entities` is cleared: every id came from the old centrald, and the
    new one announces every device again;
  - incoming links waiting for an `authorize` that went to the dead link
    are dropped, and their peers connect again;
  - device links that are already authorized stay up.
- On `registered_as` the same path as at start runs: state and BOP sent
  (`Device._on_centrald_connected`), a new key requested. The interest
  loop then resumes, since it waits for an `AUTH_OK` centrald link.

### Device links we open (the interest loop)

- `_interest_manager_loop()` connects to every device of interest that
  centrald has announced. It backs off per device, 5 s doubling to 300 s.
  The retry count resets when the link is authorized, not on the TCP
  connect, so a peer that accepts but never authorizes us also backs off.
- **Nothing goes out before the link is `AUTH_OK`:**
  - the queue holds every command until then
    (`Connection._process_command_queue`);
  - only the handshake (`register`, `auth`) goes out first, by
    `Connection.send_handshake`;
  - a command queued for longer than 60 s fails with "timed out in queue".
- `_check_handshakes()`: a link we opened that is not authorized after
  30 s, because its `auth` was lost, never answered or never sent, is
  dropped and reopened. C++ retries its own handshake after 30 s too.
- An authorized link that drops ("ready for immediate reconnection") is
  opened again in about a second, or as soon as the device registers
  again.
### A peer that hangs with its socket open

This covers a stopped or deadlocked process, a host that vanished without
resetting the connection, and a network cut.
- Any link, centrald's included, with nothing *received* for twice
  `--connection-timeout` (default 300 s, so 600 s, as C++) is dropped by
  `clean_stale_connections`, which runs once a minute. It then recovers as
  above.
- The keepalive (`T ready` after `connection_timeout`/4 idle) makes a live
  peer answer `T OK`, so a quiet but healthy link is not dropped.
- Before 2026-10-20 the timeout counted our own sends too. The keepalive
  then kept a hung peer's link up for ever, and only a TCP error (roughly
  15-20 min) closed it. `--connection-timeout` also never reached the
  connections.
- For the centrald link over the network (SPECP on primula registering
  with flores), a cut is now noticed after at most about 11 min. Lower
  `--connection-timeout` to notice it sooner.

### Devices that leave

- `delete_device <id> <name>` forgets that registration
  (`handle_delete_device`, matched on id and name), so a device that left
  is not tried until it registers again, and then at its new port. Links
  to it are left alone, as in C++.

### Incoming links

These are links from clients and other devices to us. Recovering them is
the peer's job:
- since 2026-10-20 (rts2ng `a5a58aa`, `668ab32`), C++ retries a lost
  handshake on a new socket after 30 s, up to every 300 s;
- C++ also reconnects a dropped link after 10 s, up to every 300 s;
- rtspy peers do the same through the interest loop.

An incoming link still waiting for authorization is dropped only when the
centrald link is lost.

### Tested (2026-10-20)

Setup: rts2ng centrald, `rts2-camd-dummy`, `rtspy-watcher` watching it,
`rts2-sendcmd`.
- **centrald not running at start:** registered as soon as it came up.
- **`kill -9` of centrald, restarted 10 s later:** registered again
  within 2 s of the restart. A C++ client authorized to the watcher
  through the new centrald, and the link to the camera stayed up.
- **A fake device that accepts connections and never answers `auth`:**
  - it received only `auth`, nothing else;
  - the link was dropped after 30 s and retried every 10, 20, 40 s;
  - one warning.
- **Camera restart:** link up again about 5 s after the camera came back.
  With `delete_device` handled: no attempts while it was gone, and
  authorized at the first attempt when it registered again.
- **Camera hung (SIGSTOP), `--connection-timeout 20`:**
  - its link was dropped after 59 s with nothing received;
  - new links to it were accepted by its kernel but never authorized, so
    each was dropped after 30 s, with backoff;
  - after SIGCONT, authorized at the next attempt;
  - the centrald link was never dropped.

## Known to fail

- **A command sent on a link that then drops never gets its callback.**
  `close()` neither fails the pending command nor clears the queue. This
  cannot block anything, so it costs only a log line:
  - Every link is a new `Connection` with its own queue, so a lost command
    cannot hold up the next link's commands. C++ keeps one object for the
    centrald link, and there it did (rts2ng `d272f7e`).
  - The callbacks (`queue_selector._on_executor_command_result`,
    `grbd._on_*_result`) only log, and nothing waits for them. The
    handshake callbacks (`_complete_device_authorization`,
    `_handle_registration_result`) act only on the dead link.
  - Replies we owe a peer on a dead link go nowhere. `send()` on a closed
    link returns False, and `run_queued` for a removed link is ignored.
  - Device waiters are freed by the hardware, not by the client. The
    filter/focus waiter is taken when the wheel stops, or replaced by the
    next request. The TOPTEC relay waiters time out.
  - What is lost: the result of an executor command that was cut off is
    not logged.
- **"already registered" after a host crash or a long network cut.**
  - centrald still holds our old link and refuses the new `register`.
    We retry every 60 s.
  - It does not happen when only our process restarts: the kernel closes
    the old socket, and centrald sees it at once.
  - centrald drops the old link the first time it sends on it, because
    our side answers with a reset. It sends state changes and the other
    devices' announcements; on a quiet link it sends `T ready` after
    300 s, and it drops a silent link after 600 s.
  - So it heals itself within about 5 minutes, usually sooner. Shortening
    it would need centrald to let a new registration replace an old one
    under the same name. Two copies started by mistake would then fight
    over the name, so it is left as it is.
- **No metadata to centrald.** rtspy never sent any, neither before nor
  now: no `E` lines. Value changes (`V`) do go to centrald, as to every
  `AUTH_OK` link, and C++ centrald drops them silently because it does not
  know the values (`Connection::commandValue` returns -2, which is not
  answered). State and BOP go as in C++. C++ also pushes all metadata
  (`sendMetaInfo`); nothing is known to use it.
- **Only one centrald.** There is one `--server`; a device cannot
  register with two, as the BART sensors do in C++. The architecture
  allows it (`get_associated_centrald_connection`), but the change would
  be extensive, so it is left for later. Do not make it harder.

## May fail (not seen, not tested)

- **Symmetric links.** A peer that connects to us often has two
  connections of the same name. Code that picks a connection by name
  (`queue_selector._send_executor_command`, grbd) could take the wrong
  one:
  - an outgoing link not yet authorized holds the command until it is,
    or until the 30 s drop;
  - an incoming one is named only after it is authorized (`this_device`),
    so it is usable when it is found.
- **`authorization_ok` is matched on the id alone**, in `AUTH_PENDING`.
  - With one centrald that is enough.
  - The warning `authorization_ok for non-pending id:92` (QUEUE,
    2026-10-14) is still unexplained.
  - The same warning also appears, falsely, when the id is our own
    centrald link's.
- **Clients:** `valuetool` and the daemons (`watcher`, `queue_selector`,
  `grbd`) use the same `NetworkManager`, so they recover the same way.
  Only `watcher` was tested.

## History: the zombie EXEC link (lascaux, 2026-10-14/15)

- The C++ executor crashed at 21:05, in a nested `oneRunLoop()` inside
  `waitmask`; this is fixed in rts2ng. `--autorestart` brought it back.
- QUEUE connected to the new executor and sent `now` 3 s later.
- The executor answered "unknow command" every 5 s until it was restarted
  by hand at 03:27.
- The executor's DEBUG log showed that link in `CONN_CONNECTED`, as a
  `DEVICE_SERVER`, with no name: it never processed an `auth` on it. The
  link was never authorized, not authorized and then lost.
- Why QUEUE sent `now` without a successful `auth` is not known. What
  allowed it:
  - the queue's `AUTH_OK` gate was commented out;
  - any `+`/`-` line clears the pending command.
- Today: the gate holds `now` until `auth` succeeds, and the 30 s
  handshake check drops such a link and reconnects. The six hours would
  have been under a minute.

## C++ (rts2ng) for comparison

`base/kernel`, as of 2026-10-20:
- **centrald link:** retried every 60 s for ever. A `register` with no
  answer drops the link after 30 s, and the command lost with it is
  failed (`failSentCommand`), so it cannot hold up the next `register`.
- **Handshake:** the side that started it retries on a new socket after
  30 s, up to every 300 s.
- **Dropped device links:** reconnected after 10 s, up to every 300 s.
  `delete_device` deletes the address and stops the reconnects.
- **After registering:** state and every value with its metadata are
  pushed to centrald.
- **Open, reported to JS on 2026-10-20:**
  - centrald counts the bad weather of a connection that is not
    registered (`Centrald::weatherChanged`). This is how a BART sensor's
    half-open link sent D50 to standby on 2026-10-18.
  - The 60 s re-register keeps `required_devices` missing, so centrald
    reports bad weather, for up to a minute after a centrald restart.

## Related, not about reconnection

- `valuetool -w` is broken: it calls
  `NetworkManager.send_command_to_device`, which does not exist.
- `valuetool -r/-R` cuts string values at the first space.

# Reconnection: what recovers, what does not

How rtspy recovers lost links to centrald and to other devices, what is
known to fail, and what may. State of `711046e` (2026-10-20) [MJ]. Update
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

## Known to fail

- **A peer that hangs with its TCP connection open is never detected.**
  This covers a stopped or deadlocked process, and a host that vanished
  without resetting the connection.
  - `last_activity` is updated by our own sends too, so the keepalive
    (`T ready` every `connection_timeout`/4 = 75 s) keeps the 600 s idle
    timeout from ever firing, whether the peer answers or not.
  - Commands to the peer time out one by one after 60 s, but the link
    stays.
  - Only a TCP error closes it: a reset, or the kernel's retransmission
    timeout, roughly 15-20 min after the peer stops acknowledging.
  - For the centrald link over the network (SPECP on primula registering
    with flores), a network cut is therefore noticed only after that
    time, and re-registration starts only then.
  - Fix: time out on data *received* (`T OK` counts), as C++ does.
- **`delete_device` is ignored** (`commands.py`, `handle_ignore`).
  - A device that restarted is reached at its old port first, which
    costs one refused attempt, a few seconds.
  - A device that is gone for good stays in `entities`, and the interest
    loop keeps trying it, up to every 300 s, until the centrald link
    drops.
- **A command sent on a link that then drops never gets its callback.**
  - `close()` neither fails the pending command nor clears the queue, so
    a caller waiting for the result waits for ever, unless it has its own
    timeout.
  - Replies we owe a peer on a dead link (`defer_response`, the filter
    waiter) are simply never sent, which is harmless.
- **After "already registered", recovery can take up to centrald's own
  timeout.**
  - This happens when the link died only on our side, or in the network.
  - centrald keeps the old registration until it times the old link out
    (C++ `connectionTimeout`, 300 s, enforced after twice that).
  - We retry every 60 s and get in once it does.
- **Values and metadata are not sent to centrald**, neither at start nor
  after re-registering, only state and BOP. C++ sends all of them
  (`sendMetaInfo`). Nothing is known to depend on it.
- **Only one centrald.** There is one `--server`; a device cannot
  register with two, as the BART sensors do in C++.

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

## Related, not about reconnection

- `valuetool -w` is broken: it calls
  `NetworkManager.send_command_to_device`, which does not exist.
- `valuetool -r/-R` cuts string values at the first space.

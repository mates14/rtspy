#!/usr/bin/env python3
"""Simulator of Jan Fuchs' 2m spectrographd XML-RPC server, for driver smoke tests."""
import sys, time, threading
from xmlrpc.server import SimpleXMLRPCServer

MOVE_TIME = 2.0      # seconds a discrete element takes to change
STEP_RATE = 500.0    # steps per second, focus axes and grating

# element -> (moving raw state, settable values) for discrete elements
DISCRETE = {
    1: (5, range(0, 5)), 2: (6, range(0, 6)), 3: (5, range(0, 5)),
    6: (3, range(0, 3)), 7: (3, range(0, 3)), 10: (3, range(0, 3)),
    11: (3, range(0, 3)), 12: (3, range(0, 3)), 15: (6, range(0, 6)),
    21: (5, range(0, 5)), 23: (3, range(0, 3)), 26: (3, range(0, 3)),
}
SWITCHES = (8, 9, 27, 28)            # 0/1, instant
AXES = {4: 3000.0, 5: 3000.0, 13: 6457.0, 22: 2500.0}   # position axes
METERS = (14, 24)


class Spectrograph:
    def __init__(self):
        self.lock = threading.RLock()
        self.state = [0] * 28
        for n, v in {1: 1, 2: 1, 3: 1, 6: 1, 7: 1, 10: 2, 11: 2, 12: 2, 15: 1,
                     16: 1, 17: 2, 21: 1, 23: 2, 26: 2, 27: 1, 28: 0}.items():
            self.state[n - 1] = v
        self.pending = {}                       # element -> (value, done_at)
        self.pos = dict(AXES)
        self.target = dict(AXES)
        self.count = {14: 0.0, 24: 0.0}
        self.last = time.time()
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            time.sleep(0.05)
            with self.lock:
                now = time.time()
                dt = now - self.last
                self.last = now
                for n, (value, done_at) in list(self.pending.items()):
                    if now >= done_at:
                        self.state[n - 1] = value
                        del self.pending[n]
                for n in AXES:
                    cur, tar = self.pos[n], self.target[n]
                    d = dt * STEP_RATE
                    if abs(tar - cur) <= d:
                        self.pos[n] = tar
                        if self.state[n - 1] == 1:
                            self.state[n - 1] = 0
                    else:
                        self.pos[n] = cur + (d if tar > cur else -d)
                for n in METERS:
                    if self.state[n - 1]:
                        self.count[n] += dt * 1234.0

    def spectrograph_info(self):
        with self.lock:
            info = {
                'GLST': " ".join(str(x) for x in self.state) + " ",
                'SPGS_19': " 0", 'SPGS_20': " 0",
            }
            for n in (4, 5, 13, 22):
                info['SPGP_%d' % n] = " %d" % round(self.pos[n])
            for n in METERS:
                info['SPCE_%d' % n] = " %d" % self.count[n]
                info['SPFE_%d' % n] = " %d" % (1234 if self.state[n - 1] else 0)
            return info

    def spectrograph_execute(self, cmd):
        print("SIM: %s" % cmd, flush=True)
        parts = cmd.split()
        try:
            op, n, args = parts[0], int(parts[1]), [int(float(a)) for a in parts[2:]]
        except (IndexError, ValueError):
            return "0"
        with self.lock:
            if op == "SPCH" and n in DISCRETE and len(args) == 1:
                moving, allowed = DISCRETE[n]
                v = args[0]
                if v not in allowed:
                    return "0"
                if n == 26 and v in (1, 2):
                    v = 3 - v           # set 1 = remove (reads 2), 2 = insert (reads 1)
                if v == 0:
                    self.pending.pop(n, None)
                    self.state[n - 1] = 0
                elif self.state[n - 1] != v:
                    self.state[n - 1] = moving
                    self.pending[n] = (v, time.time() + MOVE_TIME)
                return "1"
            if op == "SPCH" and n in SWITCHES and len(args) == 1 and args[0] in (0, 1):
                self.state[n - 1] = args[0]
                return "1"
            if op in ("SPAP", "SPRP") and n in AXES and len(args) == 1:
                self.target[n] = args[0] if op == "SPAP" else self.target[n] + args[0]
                self.state[n - 1] = 1
                return "1"
            if op == "SPST" and not args:
                if n in AXES:
                    self.target[n] = self.pos[n]
                self.pending.pop(n, None)
                self.state[n - 1] = 0
                return "1"
            if op == "SPCA" and n in AXES and not args:
                self.target[n] = 0
                self.state[n - 1] = 1
                return "1"
            if op in ("SSTE", "SSPE") and n in METERS and not args:
                self.state[n - 1] = 1 if op == "SSTE" else 0
                if op == "SSTE":
                    self.count[n] = 0.0
                return "1"
            if op == "SPGP" and n in AXES:
                return " %d" % round(self.pos[n])
            if op == "SPGS":
                return " %d" % self.state[n - 1]
            if op == "SPCE" and n in METERS:
                return " %d" % self.count[n]
            if op == "SPFE" and n in METERS:
                return " %d" % (1234 if self.state[n - 1] else 0)
        return "0"


port = int(sys.argv[1]) if len(sys.argv) > 1 else 8888
server = SimpleXMLRPCServer(('127.0.0.1', port), logRequests=False, allow_none=True)
server.register_instance(Spectrograph())
print("SIM: spectrograph simulator on 127.0.0.1:%d" % port, flush=True)
server.serve_forever()

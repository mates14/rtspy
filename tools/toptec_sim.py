#!/usr/bin/env python3
"""Simulator of Jan Fuchs' TOPTEC XML-RPC server, for driver smoke tests."""
import sys, time, threading
from xmlrpc.server import SimpleXMLRPCServer

STEP_RATE = 3000.0   # steps per second, both axes

class Head:
    def __init__(self):
        self.lock = threading.RLock()
        self.focus = 4678.0
        self.focus_tar = 4678.0
        self.cam = 400.0
        self.cam_tar = 400.0
        self.g1 = 1
        self.g2 = 0
        self.last = time.time()
        threading.Thread(target=self._move, daemon=True).start()

    def _move(self):
        while True:
            time.sleep(0.05)
            with self.lock:
                now = time.time()
                d = (now - self.last) * STEP_RATE
                self.last = now
                for name in ('focus', 'cam'):
                    cur = getattr(self, name)
                    tar = getattr(self, name + '_tar')
                    if abs(tar - cur) <= d:
                        setattr(self, name, tar)
                    else:
                        setattr(self, name, cur + (d if tar > cur else -d))

    def toptec_get_values(self):
        with self.lock:
            return {
                'focus_position': int(round(self.focus)),
                'cameras_position': int(round(self.cam)),
                'esw1a': 0, 'esw1b': 0, 'esw2a': 0, 'esw2b': 0,
                'g1': self.g1, 'g2': self.g2,
                'voltage_33': 33, 'voltage_50': 50, 'voltage_120': 121,
            }

    def toptec_set_focus_position(self, n):
        with self.lock:
            self.focus_tar = float(n)
        print("SIM: focus -> %d" % n, flush=True)
        return 1

    def toptec_set_camera_position(self, n):
        with self.lock:
            self.cam_tar = float(n)
        print("SIM: cameras -> %d" % n, flush=True)
        return 1

    def toptec_inc_focus_position(self):
        with self.lock: self.focus_tar += 10
        print("SIM: focus++", flush=True); return 1

    def toptec_dec_focus_position(self):
        with self.lock: self.focus_tar -= 10
        print("SIM: focus--", flush=True); return 1

    def toptec_inc_camera_position(self):
        with self.lock: self.cam_tar += 10
        print("SIM: cameras++", flush=True); return 1

    def toptec_dec_camera_position(self):
        with self.lock: self.cam_tar -= 10
        print("SIM: cameras--", flush=True); return 1

    def toptec_set_camera_power(self, mask):
        with self.lock:
            self.g1 = 1 if mask & 1 else 0
            self.g2 = 1 if mask & 2 else 0
        print("SIM: camera power mask %d" % mask, flush=True)
        return 1

    def toptec_reset(self):
        print("SIM: reset", flush=True); return 1

port = int(sys.argv[1]) if len(sys.argv) > 1 else 6000
server = SimpleXMLRPCServer(('127.0.0.1', port), logRequests=False, allow_none=True)
server.register_instance(Head())
print("SIM: TOPTEC simulator on 127.0.0.1:%d" % port, flush=True)
server.serve_forever()

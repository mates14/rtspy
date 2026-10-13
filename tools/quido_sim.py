#!/usr/bin/env python3
"""Simulator of the Quido relay XML-RPC server (fibre lamps), for driver smoke tests."""
import sys
from xmlrpc.server import SimpleXMLRPCServer


class Quido:
    def __init__(self):
        self.relays = [0, 0, 0, 1]      # relay 4 (tube PC) normally on

    def quido_get_relays(self):
        return list(self.relays)

    def quido_get_temperature(self):
        return 21.5

    def quido_set_relay(self, number, value):
        print("SIM: relay %d -> %d" % (number, value), flush=True)
        if not 1 <= number <= len(self.relays):
            return False
        self.relays[number - 1] = 1 if value else 0
        return True


port = int(sys.argv[1]) if len(sys.argv) > 1 else 6001
server = SimpleXMLRPCServer(('127.0.0.1', port), logRequests=False, allow_none=True)
server.register_instance(Quido())
print("SIM: Quido simulator on 127.0.0.1:%d" % port, flush=True)
server.serve_forever()

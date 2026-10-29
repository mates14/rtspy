#!/usr/bin/python3
"""
COND - observing conditions from the photometric pipeline, published to RTS2.

Reads the newest pipeline ECSVs (*-df.ecsv, header only) of the local cameras
and the cond.json snapshots of other COND instances (other telescopes, the
Makak sky monitor), and publishes per camera/filter, normalised to 1 s:

    <p>_zp1   zeropoint for 1 s [mag]           MAGZERO - 2.5 log10(EXPTIME)
    <p>_sky   sky surface brightness [mag/"2]   from BGSIGMA (or MEDIAN), own zeropoint
    <p>_bgv   sky variance rate [ADU^2/s/px]    (BGSIGMA^2 - floor) / EXPTIME
    <p>_fwhm  image FWHM [px]
    <p>_X, <p>_alt, <p>_az                      pointing of that frame (az north-based)
    <p>_tr    transparency [mag]                zp1 - (zpref - k (X - 1)), if zpref is configured
    <p>_age   seconds since the end of that exposure

and the freshest transparency over all shown sources as transp/transp_src.
<p> is the camera plus a short filter name, e.g. C1_r for C1/Sloan_r.

It writes its own local sources into a cond.json snapshot (atomically), for
the web and for the COND of the other systems, and optionally pushes it
(--push, e.g. Makak on morpheus rsyncs it to lascaux).  It does no modelling: the
exposure-time calculation and the transfer between cameras/filters belong to
the consumers (asarina snrexp).

A camera is described by --camera, one entry per camera, ';'-separated:
    C1:floor=76:bgk=0.70:pix=3.12             sky from BGSIGMA
    MK:sky=median:pix=56.8:alt=65.8:az=359.3  sky from MEDIAN (bias-subtracted); a fixed
                                              camera's pointing (its headers have none)
and a reference zeropoint per camera/filter by --zpref:
    C1/Sloan_r=18.38:0.16,C1/Sloan_i=17.53:0.12   (zp1 at airmass 1 : extinction)
"""

import glob
import json
import logging
import math
import os
import re
import shlex
import subprocess
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from rtspy.core.value import ValueDouble, ValueString
from rtspy.core.device import Device
from rtspy.core.constants import DeviceType
from rtspy.core.config import DeviceConfig
from rtspy.core.app import App

_META = re.compile(r'^# - \{([A-Za-z0-9_\-]+): (.*)\}\s*$')
_KEYS = {'CCD_NAME', 'FILTER', 'EXPTIME', 'MAGZERO', 'BGSIGMA', 'MEDIAN', 'FWHM', 'AIRMASS',
         'TEL_ALT', 'TEL_AZ', 'DATE-OBS', 'JD', 'PHQUAL', 'TELESCOP'}
_FILTER_SHORT = {'Sloan_g': 'g', 'Sloan_r': 'r', 'Sloan_i': 'i', 'Sloan_z': 'z'}
_FIELDS = ('zp1', 'sky', 'bgv', 'fwhm', 'X', 'alt', 'az', 'tr', 'age')


def read_header(path: str) -> Dict[str, Any]:
    """The ECSV meta keys COND needs; stops at the first table line."""
    h = {}
    with open(path, errors='replace') as f:
        for line in f:
            if not line.startswith('#'):
                h['_complete'] = True
                break
            m = _META.match(line)
            if m and m.group(1) in _KEYS:
                v = m.group(2).strip().strip("'")
                try:
                    v = float(v)
                except ValueError:
                    pass
                h[m.group(1)] = v
    return h


def parse_cameras(spec: str) -> Dict[str, Dict[str, Any]]:
    """'C1:floor=76:bgk=0.70:pix=3.12;MK:sky=median:pix=56.8' -> {C1: {...}, MK: {...}}"""
    cams = {}
    for entry in filter(None, (e.strip() for e in (spec or '').split(';'))):
        name, *opts = entry.split(':')
        d = {'floor': 0.0, 'bgk': 1.0, 'pix': 1.0, 'sky': 'bgsigma'}
        for o in opts:
            k, v = o.split('=', 1)
            d[k.strip()] = v.strip() if k.strip() == 'sky' else float(v)
        cams[name.strip()] = d
    return cams


def parse_zpref(spec: str) -> Dict[str, tuple]:
    """'C1/Sloan_r=18.38:0.16,...' -> {'C1/Sloan_r': (18.38, 0.16)}"""
    out = {}
    for entry in filter(None, (e.strip() for e in (spec or '').split(','))):
        key, v = entry.split('=', 1)
        zp, _, k = v.partition(':')
        out[key.strip()] = (float(zp), float(k) if k else 0.0)
    return out


def prefix(key: str) -> str:
    """'C1/Sloan_r' -> 'C1_r' (RTS2 value names, recvals.value_name <= 25 chars)"""
    cam, _, filt = key.partition('/')
    return f'{cam}_{_FILTER_SHORT.get(filt, filt)}'


def night_dir(root: str, fmt: str, t: float) -> str:
    """Directory of the night containing t (UT - 12 h, as the pipeline names it)."""
    return os.path.join(root, datetime.fromtimestamp(t - 43200, tz=timezone.utc).strftime(fmt))


def end_time(h: Dict[str, Any]) -> float:
    """Unix time of the end of the exposure."""
    if isinstance(h.get('DATE-OBS'), str):
        t = datetime.fromisoformat(h['DATE-OBS']).replace(tzinfo=timezone.utc).timestamp()
    else:
        t = (h['JD'] - 2440587.5) * 86400
    return t + h['EXPTIME']


def conditions(h: Dict[str, Any], cam: Dict[str, Any], zpref: Optional[tuple]) -> Dict[str, Any]:
    """One frame's conditions, normalised to 1 s; None if unusable."""
    t = h.get('EXPTIME', 0)
    if h.get('PHQUAL', 'OK') != 'OK' or not t or t < 1 or 'MAGZERO' not in h:
        return None
    zp1 = h['MAGZERO'] - 2.5 * math.log10(t)
    if cam['sky'] == 'median':
        rate = h.get('MEDIAN', math.nan) / t                       # ADU/s/px
        bgv = math.nan
    else:
        bgv = max(h.get('BGSIGMA', math.nan)**2 - cam['floor'], 0.0) / t
        rate = bgv / cam['bgk']
    sky = zp1 - 2.5 * math.log10(rate / cam['pix']**2) if rate > 0 else math.nan
    alt, az = cam.get('alt', h.get('TEL_ALT', math.nan)), cam.get('az')
    if az is None:
        az = (h.get('TEL_AZ', math.nan) + 180) % 360             # RTS2 az is south-based
    X = 1 / math.sin(math.radians(cam['alt'])) if 'alt' in cam else h.get('AIRMASS', math.nan)
    tr = zp1 - (zpref[0] - zpref[1] * (X - 1)) if zpref else math.nan
    return dict(zp1=zp1, sky=sky, bgv=bgv, fwhm=h.get('FWHM', math.nan), X=X, alt=alt, az=az,
                tr=tr, tend=end_time(h), exptime=t)


class EcsvScanner:
    """Newest usable frame per camera/filter in the night directories, parsing each file once."""

    def __init__(self, root, date_fmt, cameras, zpref, camera_name=None, retry_age=300):
        self.root, self.date_fmt = root, date_fmt
        self.cameras, self.zpref = cameras, zpref
        self.camera_name = camera_name
        self.retry_age = retry_age
        self.seen = set()                    # parsed files (usable or not)
        self.latest: Dict[str, Dict[str, Any]] = {}

    def scan(self, now=None):
        now = now or time.time()
        dirs = {night_dir(self.root, self.date_fmt, now), night_dir(self.root, self.date_fmt, now - 6 * 3600)}
        for d in sorted(dirs):
            for path in sorted(glob.glob(os.path.join(d, '*.ecsv'))):
                if path in self.seen:
                    continue
                try:
                    h = read_header(path)
                except OSError:
                    continue
                if not h.get('_complete'):
                    if now - os.path.getmtime(path) > self.retry_age:
                        self.seen.add(path)   # stays incomplete: give up on it
                    continue
                self.seen.add(path)
                cam = self.camera_name or h.get('CCD_NAME')
                if cam not in self.cameras or not h.get('FILTER'):
                    continue
                key = f'{cam}/{h["FILTER"]}'
                try:
                    c = conditions(h, self.cameras[cam], self.zpref.get(key))
                except (KeyError, ValueError, TypeError) as e:
                    logging.debug(f'{path}: {e}')
                    continue
                if c and (key not in self.latest or c['tend'] >= self.latest[key]['tend']):
                    c['file'] = os.path.basename(path)
                    self.latest[key] = c
        return self.latest


def fetch_snapshot(url: str, timeout: float = 5.0) -> Dict[str, Dict[str, Any]]:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode()).get('sources', {})


def write_snapshot(path: str, sources: Dict[str, Dict[str, Any]], host: str):
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    clean = {k: {f: (None if isinstance(v, float) and math.isnan(v) else v) for f, v in s.items()}
             for k, s in sources.items()}
    with tempfile.NamedTemporaryFile('w', dir=d, delete=False, suffix='.tmp') as f:
        json.dump(dict(host=host, written=time.time(), sources=clean), f, indent=1)
    os.chmod(f.name, 0o644)
    os.replace(f.name, path)


class Cond(Device, DeviceConfig):
    """Observing conditions from pipeline ECSVs and other COND snapshots."""

    def setup_config(self, config):
        config.add_argument('--ecsv-dir', default='~/phdb-rt', help='real-time ECSV root', section='cond')
        config.add_argument('--ecsv-date-fmt', default='%y/%m%d', help='night subdirectory format', section='cond')
        config.add_argument('--camera', default='', help="cameras, e.g. 'C1:floor=76:bgk=0.70:pix=3.12;C2:...'", section='cond')
        config.add_argument('--camera-name', help='use this camera name for all local frames (Makak: MK)', section='cond')
        config.add_argument('--zpref', default='', help="'C1/Sloan_r=18.38:0.16,...' zp1 at X=1 : extinction", section='cond')
        config.add_argument('--show', default='', help="sources published as values, 'C1/Sloan_r,MK/N,...'", section='cond')
        config.add_argument('--remote', default='', help='cond.json URLs of other COND instances, comma-separated', section='cond')
        config.add_argument('--snapshot', default='', help='where to write this instance\'s cond.json', section='cond')
        config.add_argument('--push', default='', help="command run after a changed snapshot is written, {} = its path "
                            "(e.g. 'rsync -q {} lascaux.asu.cas.cz:')", section='cond')
        config.add_argument('--push-interval', type=float, default=30.0, help='minimum time between pushes [s]', section='cond')
        config.add_argument('--poll', type=float, default=10.0, help='poll interval [s]', section='cond')
        config.add_argument('--max-age', type=float, default=3600.0, help='older sources are not published [s]', section='cond')

    def __init__(self, device_name='COND', port=0):
        super().__init__(device_name, DeviceType.SENSOR, port)
        self.vals: Dict[str, Dict[str, ValueDouble]] = {}
        self.remote_err: Dict[str, str] = {}
        self.running = False
        self.thread = None
        self.set_state(self.STATE_IDLE, 'Initializing COND')

    def apply_config(self, config: Dict[str, Any]):
        self.poll = config.get('poll', 10.0)
        self.max_age = config.get('max_age', 3600.0)
        self.snapshot = os.path.expanduser(config.get('snapshot') or '')
        self.push = config.get('push') or ''
        self.push_interval = config.get('push_interval', 30.0)
        self.pushed = (0.0, None)          # time, newest frame pushed
        self.remotes = [u.strip() for u in (config.get('remote') or '').split(',') if u.strip()]
        self.scanner = EcsvScanner(os.path.expanduser(config.get('ecsv_dir', '~/phdb-rt')),
                                   config.get('ecsv_date_fmt', '%y/%m%d'),
                                   parse_cameras(config.get('camera', '')),
                                   parse_zpref(config.get('zpref', '')),
                                   config.get('camera_name'))
        # values must exist before the device registers with centrald
        for key in filter(None, (s.strip() for s in (config.get('show') or '').split(','))):
            p = prefix(key)
            self.vals[key] = {f: ValueDouble(f'{p}_{f}', f'{key} {f}', write_to_fits=False) for f in _FIELDS}
        self.transp = ValueDouble('transp', 'freshest transparency over shown sources [mag]', write_to_fits=True)
        self.transp_src = ValueString('transp_src', 'source of transp', write_to_fits=True)
        self.last_file = ValueString('last_file', 'newest local frame used', write_to_fits=False)
        logging.info(f'COND: cameras {list(self.scanner.cameras)}, showing {list(self.vals)}, remotes {self.remotes}')

    def start(self):
        super().start()
        self.running = True
        self.thread = threading.Thread(target=self._loop, name='CondPoll', daemon=True)
        self.thread.start()
        self.set_ready('COND ready')

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        super().stop()

    def _loop(self):
        while self.running:
            try:
                self.update()
            except Exception as e:
                logging.exception(f'COND update failed: {e}')
            time.sleep(self.poll)

    def _push(self, now, newest):
        cmd = shlex.split(self.push.replace('{}', shlex.quote(self.snapshot)))
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if r.returncode:
                logging.warning(f'COND: push failed ({r.returncode}): {r.stderr.strip()[:200]}')
        except (OSError, subprocess.TimeoutExpired) as e:
            logging.warning(f'COND: push failed: {e}')
        self.pushed = (now, newest)

    def update(self, now=None):
        now = now or time.time()
        local = dict(self.scanner.scan(now))
        if self.snapshot and local:
            write_snapshot(self.snapshot, local, os.uname().nodename)
            newest = max(c['tend'] for c in local.values())
            if self.push and newest != self.pushed[1] and now - self.pushed[0] >= self.push_interval:
                self._push(now, newest)
        sources = {}
        for url in self.remotes:
            try:
                sources.update(fetch_snapshot(url))
                if self.remote_err.pop(url, None):
                    logging.info(f'COND: {url} reachable again')
            except Exception as e:
                if url not in self.remote_err:           # log the change, not every poll
                    logging.warning(f'COND: {url}: {e}')
                self.remote_err[url] = str(e)
        sources.update(local)          # local frames win over a stale remote copy
        best = None
        for key, vals in self.vals.items():
            s = sources.get(key)
            age = now - s['tend'] if s else math.nan
            fresh = s is not None and age <= self.max_age
            for f, v in vals.items():
                x = age if f == 'age' else (s.get(f) if fresh else None)
                v.value = math.nan if x is None or not fresh else float(x)
            if fresh and s.get('tr') is not None and not math.isnan(s['tr']) and (best is None or age < best[1]):
                best = (key, age, s['tr'])
        self.transp.value = best[2] if best else math.nan
        self.transp_src.value = best[0] if best else ''
        if local:
            self.last_file.value = max(local.values(), key=lambda c: c['tend'])['file']
        return sources


def main():
    """Entry point - see rtspy/core/daemon.py for the startup contract."""
    return App(description='Observing conditions from the photometric pipeline').main(Cond)


if __name__ == '__main__':
    main()

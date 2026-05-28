#!/usr/bin/env python3
"""
Adaptive exposure calculator and observation executor for camera C0.

Reads recent photometric pipeline output (stat.txt), predicts current
zeropoint and sky background with zp_predict / bg_predict, computes the
required exposure time for (magnitude, SNR, filter), rounds to a practical
plan, and optionally executes on C0 via RTS2.

Data paths (override via environment variables):
    RTS2_STAT_FILE       — path to the pipeline stat.txt
    RTS2_BGNOISE_MODEL   — path to the trained background noise model

Usage (calculation only):
    observe.py --magnitude 18.5 --snr 10
    observe.py -m 18.5 -s 10 --filter Sloan_i --fwhm 3.5

Usage (also execute on C0):
    observe.py -m 18.5 -s 10 --execute
"""

import sys
import os
import time
import argparse
import numpy as np
import pandas as pd

from rtspy.observe.zp_predict import predict_zeropoint, FILTER_PARAMS
from rtspy.observe.bg_predict import predict_background, _DEFAULT_MODEL
from rtspy.observe.expcalc import (
    calculate_exptime, predict_performance,
    snr_to_magerror, magerror_to_snr,
    GAIN, RN,
)

# RTS2 is optional — needed only for --execute
try:
    sys.path.insert(0, '/home/mates/src/rts2/python')
    from rts2.scriptcomm import Rts2Comm
    HAS_RTS2 = True
except ImportError:
    HAS_RTS2 = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STAT_FILE    = os.environ.get('RTS2_STAT_FILE', '/data/rts2/stat.txt')
MODEL_FILE   = _DEFAULT_MODEL
WINDOW_MIN   = 15.0    # look-back window for recent conditions
DEFAULT_FWHM = 3.0     # pixels; typical for this telescope

# Filter name → camera filter wheel token
FILTER_TOKEN = {
    'Sloan_g': 'g',
    'Sloan_r': 'r',
    'Sloan_i': 'i',
    'Sloan_z': 'z',
    'N':       'N',
}

# Practical exposure plans (n_exposures, single_exptime_s).
# Ordered by increasing total time.
NICE_PLANS = [
    (1,  10), (1,  20), (2,  20), (1,  60), 
    (1, 120), (2, 120), (3, 120),
    (1, 300), (2, 300), (3, 300), (4, 300), (5, 300),
    (6, 300), (7, 300), (8, 300), (9, 300), (10,300), (12,300),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def current_jd() -> float:
    return 2440587.5 + time.time() / 86400.0


def sky_1s_from_bgnoise(bgnoise_1s: float) -> float:
    """Convert bgnoise_1s [ADU] to sky_1s [photons/s/pixel] for the exposure calculator."""
    return max(GAIN**2 * bgnoise_1s**2 - GAIN**2 * RN**2, 0.5)


def choose_plan(required_s: float):
    """
    Return the first plan whose total time ≥ required_s.
    Returns (n_exposures, exptime_s, total_s).
    """
    for n, t in NICE_PLANS:
        if n * t >= required_s:
            return n, t, n * t
    n, t = NICE_PLANS[-1]
    return n, t, n * t

# ---------------------------------------------------------------------------
# Condition estimation
# ---------------------------------------------------------------------------

def load_recent(stat_file: str = STAT_FILE, window_min: float = WINDOW_MIN):
    """Return (recent_df, last_row) from stat.txt."""
    data = pd.read_csv(
        stat_file, sep=r'\s+', header=None,
        names=['jd', 'exposure', 'zeropoint', 'bgnoise', 'maglim',
               'airmass', 'moon_alt', 'sun_alt', 'filter', 'image'],
    )
    data = data[data['exposure'] > 0].copy()
    data['zp_1s']      = data['zeropoint'] - 2.5 * np.log10(data['exposure'])
    data['bgnoise_1s'] = data['bgnoise'] / np.sqrt(data['exposure'])

    jd_now  = current_jd()
    t_start = jd_now - window_min / 1440.0
    recent  = data[data['jd'] >= t_start].sort_values('jd').reset_index(drop=True)
    last    = data.sort_values('jd').iloc[-1] if len(data) else None
    return recent, last


def _zp_fallback(target_filter, airmass, k_r=0.15):
    """Calibrated Z0 at a typical extinction when no recent obs are available."""
    p     = FILTER_PARAMS.get(target_filter, {'Z0': 21.5, 'beta': 0.0})
    zp_1s = p['Z0'] - (k_r + p['beta']) * airmass
    return zp_1s, 0.3, 0


def predict_conditions(recent, last, target_filter, jd_now, airmass, sun_alt, moon_alt):
    """
    Predict (zp_1s, zp_unc, n_zp, bgnoise_1s, bg_src) for target_filter at jd_now.
    Falls back to calibrated defaults when no recent data exists.
    """
    if len(recent) >= 2:
        obs = list(zip(
            recent['jd'], recent['filter'],
            recent['zp_1s'],
            [0.1] * len(recent),
            recent['airmass'],
        ))
        zp_res = predict_zeropoint(
            obs,
            target_filters=target_filter,
            reference_time=jd_now,
            window_minutes=WINDOW_MIN,
            target_airmass=airmass,
        )
        if zp_res and target_filter in zp_res:
            r      = zp_res[target_filter]
            zp_1s  = r['zp_pred']
            zp_unc = r['uncertainty']
            n_zp   = r['n_obs_total']
        else:
            zp_1s, zp_unc, n_zp = _zp_fallback(target_filter, airmass)
    else:
        zp_1s, zp_unc, n_zp = _zp_fallback(target_filter, airmass)

    try:
        bgnoise_1s = predict_background(
            jd_now, sun_alt, moon_alt, airmass, target_filter, zp_1s,
            model_file=MODEL_FILE,
        )
        bg_src = 'ML'
    except Exception as e:
        bgnoise_1s = 5.0
        bg_src = f'default ({e})'

    return zp_1s, zp_unc, n_zp, bgnoise_1s, bg_src

# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------

def compute_plan(magnitude, snr, target_filter, fwhm, stat_file=STAT_FILE, verbose=True):
    """
    Compute an observation plan for (magnitude, snr) in target_filter.

    Returns (n_exp, exptime, total_s, zp_1s, bgnoise_1s) or None on failure.
    """
    jd_now = current_jd()

    try:
        recent, last = load_recent(stat_file)
    except Exception as e:
        print(f"Warning: could not read {stat_file}: {e}", file=sys.stderr)
        recent, last = pd.DataFrame(), None

    if last is not None:
        airmass  = float(last['airmass'])
        sun_alt  = float(last['sun_alt'])
        moon_alt = float(last['moon_alt'])
    else:
        airmass, sun_alt, moon_alt = 1.3, -30.0, -10.0

    zp_1s, zp_unc, n_zp, bgnoise_1s, bg_src = predict_conditions(
        recent, last, target_filter, jd_now, airmass, sun_alt, moon_alt
    )

    sky_1s        = sky_1s_from_bgnoise(bgnoise_1s)
    target_magerr = snr_to_magerror(snr)

    # Exposure calculator uses the convention  magzero_arg = zp_1s - 10
    required_s = calculate_exptime(magnitude, target_magerr, fwhm, zp_1s - 10.0, sky_1s)

    if np.isnan(required_s) or required_s <= 0:
        print("ERROR: Cannot compute exposure time — target may be too faint for current conditions.")
        return None

    n_exp, exptime, total_s = choose_plan(required_s)

    pred_magerr, pred_snr_single = predict_performance(magnitude, exptime, fwhm, zp_1s - 10.0, sky_1s)
    pred_snr_stack = pred_snr_single * np.sqrt(n_exp)

    if verbose:
        print()
        print(f"Conditions [{target_filter}, airmass={airmass:.2f}]")
        print(f"  JD           {jd_now:.5f}")
        print(f"  Sun / Moon   {sun_alt:+.1f}°  /  {moon_alt:+.1f}°")
        print(f"  ZP (1 s)     {zp_1s:.2f} ± {zp_unc:.2f} mag   ({n_zp} obs in window)")
        print(f"  bgnoise_1s   {bgnoise_1s:.2f} ADU  [{bg_src}]")
        print()
        print(f"Target")
        print(f"  Magnitude    {magnitude:.2f}")
        print(f"  Required SNR {snr:.1f}   →  mag error {target_magerr:.4f}")
        print(f"  FWHM         {fwhm:.1f} px")
        print()
        print(f"Result")
        print(f"  Needed       {required_s:.0f} s")
        print(f"  Plan         {n_exp}×{exptime} s   (total {total_s} s)")
        if n_exp == 1:
            print(f"  Expected SNR {pred_snr_single:.1f}")
        else:
            print(f"  Expected SNR {pred_snr_stack:.1f}  (stack)  /  {pred_snr_single:.1f}  (single frame)")

    return n_exp, exptime, total_s, zp_1s, bgnoise_1s

# ---------------------------------------------------------------------------
# RTS2 execution
# ---------------------------------------------------------------------------

class _ObserveC0(Rts2Comm):
    """Minimal RTS2 script: set filter + exposure, loop n_exp times on C0."""

    def __init__(self, n_exp, exptime, filter_token):
        Rts2Comm.__init__(self)
        self.n_exp        = n_exp
        self.exptime      = exptime
        self.filter_token = filter_token

    def run(self):
        self.setValue('SHUTTER', 'LIGHT')
        self.setValue('filter',   self.filter_token)
        self.setValue('exposure', self.exptime)

        for i in range(self.n_exp):
            self.log('I', f'observe: frame {i+1}/{self.n_exp}  '
                          f'({self.exptime} s, filter={self.filter_token})')
            image = self.exposure()
            self.process(image)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Compute (and optionally execute) an exposure plan for C0',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('-m', '--magnitude', type=float, required=True,
                        help='Target magnitude')
    parser.add_argument('-s', '--snr', type=float, required=True,
                        help='Required signal-to-noise ratio')
    parser.add_argument('-f', '--filter', dest='filter_name',
                        default='Sloan_r', choices=list(FILTER_PARAMS.keys()),
                        help='Observing filter')
    parser.add_argument('--fwhm', type=float, default=DEFAULT_FWHM,
                        help='PSF FWHM in pixels')
    parser.add_argument('--stat', default=STAT_FILE, metavar='FILE',
                        help='Path to stat.txt (or set RTS2_STAT_FILE)')
    parser.add_argument('--execute', action='store_true',
                        help='Execute the plan on C0 via RTS2 after calculation')

    args = parser.parse_args()

    result = compute_plan(args.magnitude, args.snr, args.filter_name,
                          args.fwhm, stat_file=args.stat)
    if result is None:
        sys.exit(1)

    n_exp, exptime, total_s, *_ = result

    if args.execute:
        if not HAS_RTS2:
            print('\nERROR: rts2.scriptcomm not available — cannot execute.', file=sys.stderr)
            sys.exit(1)
        token = FILTER_TOKEN.get(args.filter_name, args.filter_name)
        print(f'\nExecuting {n_exp}×{exptime} s on C0 (filter={token}) …')
        _ObserveC0(n_exp, exptime, token).run()
    else:
        token = FILTER_TOKEN.get(args.filter_name, args.filter_name)
        print()
        print(f'To execute manually, run RTS2 script:')
        filt_cmd = f'filter={token} ' if token else ''
        print(f'  {filt_cmd}for {n_exp} do E {exptime} done')
        print(f'Or re-run with --execute.')


if __name__ == '__main__':
    main()

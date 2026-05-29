"""
rtspy.observe — real-time limiting-magnitude estimation for camera C0.

Key functions:
    predict_zeropoint()   — estimate current 1s-ZP from recent pipeline output
    predict_background()  — estimate current 1s sky background noise (ML model)
    compute_plan()        — turn (magnitude, SNR, filter) into an exposure plan

Data paths:
    RTS2_STAT_FILE      — environment variable for the pipeline stat.txt
    RTS2_BGNOISE_MODEL  — environment variable for the trained background model
                          (default: this package directory / bgnoise_model.pkl)

Train the background model:
    python -m rtspy.observe.bg_predict /path/to/stat.txt [/path/to/model.pkl]

CLI:
    rtspy-observe -m 18.5 -s 10 [--filter Sloan_r] [--fwhm 3.0] [--execute]
"""

from rtspy.observe.zp_predict import predict_zeropoint, FILTER_PARAMS, SANITY_LIMITS
from rtspy.observe.bg_predict import (
    predict_background, predict_background_realtime, train_model,
    moon_illumination, night_fraction,
)
from rtspy.observe.observe import compute_plan, choose_plan, load_recent
from rtspy.observe.telescope import TelescopeConfig
from rtspy.observe.zpfit import fit_zeropoints
from rtspy.observe.apecalfit import fit_ape
from rtspy.observe.stat import (
    read_stat, write_stat_record, record_from_ecsv,
    night_from_jd, load_recent as stat_load_recent,
)

__all__ = [
    'predict_zeropoint',
    'FILTER_PARAMS',
    'SANITY_LIMITS',
    'predict_background',
    'predict_background_realtime',
    'train_model',
    'moon_illumination',
    'night_fraction',
    'compute_plan',
    'choose_plan',
    'load_recent',
    'TelescopeConfig',
    'fit_zeropoints',
    'fit_ape',
    'read_stat',
    'write_stat_record',
    'record_from_ecsv',
    'night_from_jd',
]

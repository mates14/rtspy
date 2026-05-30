"""
Per-telescope configuration for the observe package.

Load a config from YAML, then use TelescopeConfig to drive both the
exposure calculator and the zeropoint predictor with telescope-specific
hardware and photometric calibration constants.

Workflow for a new telescope
-----------------------------
1. Collect stat.txt from the photometric pipeline.
2. Run zpfit.py on stat.txt to get Z0 and beta per filter.
3. Measure (or look up) GAIN, RN from camera datasheet / dark frames.
4. Fit APE from a photon-noise calibration dataset (see expcalc.py).
5. Write a config YAML (use config_d50_c0.yaml as a template).
6. Train the background model:
       rtspy-observe-train --stat stat.txt --config mytelescope.yaml
7. Deploy the config YAML + bgnoise_model.pkl to the telescope.
   Set environment variable:
       export RTS2_OBSERVE_CONFIG=/path/to/mytelescope.yaml
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import yaml

from rtspy.observe.zp_predict import FILTER_PARAMS, SANITY_LIMITS
from rtspy.observe.expcalc import ExposureCalculator, GAIN, RN, APE

_DEFAULT_CONFIG = os.environ.get(
    'RTS2_OBSERVE_CONFIG',
    str(Path(__file__).parent / 'config_d50_c0.yaml'),
)


@dataclass
class TelescopeConfig:
    """
    Hardware and photometric calibration parameters for one camera.

    Attributes
    ----------
    gain         : CCD gain [e-/ADU]
    readnoise    : effective readout noise [electrons RMS]
    ape          : aperture growth curve parameter (fitted from photon noise data)
    default_fwhm : typical PSF FWHM [pixels]
    filter_params: {filter → {Z0, beta}} — from zpfit.py
    sanity_limits: {filter → {zp_min, zp_max}} — pre-screening bounds for zp_1s
    filter_token : {filter_name → camera filter wheel token}
    model_file   : path to trained bgnoise_model.pkl
    """

    gain:          float = GAIN
    readnoise:     float = RN
    ape:           float = APE
    default_fwhm:  float = 3.0
    plate_scale:   Optional[float] = None  # arcsec/pixel; None = unknown

    filter_params:  Dict = field(default_factory=lambda: dict(FILTER_PARAMS))
    sanity_limits:  Dict = field(default_factory=lambda: dict(SANITY_LIMITS))
    filter_token:   Dict = field(default_factory=lambda: {
        'Sloan_g': 'g', 'Sloan_r': 'r', 'Sloan_i': 'i', 'Sloan_z': 'z', 'N': 'N',
    })
    model_file: str = field(default_factory=lambda: os.environ.get(
        'RTS2_BGNOISE_MODEL',
        str(Path(__file__).parent / 'bgnoise_model.pkl'),
    ))

    def __post_init__(self):
        self._calc = ExposureCalculator(
            gain=self.gain, readnoise=self.readnoise, ape=self.ape,
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str) -> 'TelescopeConfig':
        """Load telescope configuration from a YAML file."""
        with open(path) as f:
            d = yaml.safe_load(f)

        cfg = cls.__new__(cls)
        cfg.gain         = float(d.get('gain',         GAIN))
        cfg.readnoise    = float(d.get('readnoise',    RN))
        cfg.ape          = float(d.get('ape',          APE))
        cfg.default_fwhm = float(d.get('default_fwhm', 3.0))
        cfg.plate_scale  = float(d['plate_scale']) if 'plate_scale' in d else None

        if 'filter_params' in d:
            cfg.filter_params = {
                f: {'Z0': float(v['Z0']), 'beta': float(v['beta'])}
                for f, v in d['filter_params'].items()
            }
        else:
            cfg.filter_params = dict(FILTER_PARAMS)

        if 'sanity_limits' in d:
            cfg.sanity_limits = {
                f: {'zp_min': float(v['zp_min']), 'zp_max': float(v['zp_max'])}
                for f, v in d['sanity_limits'].items()
            }
        else:
            cfg.sanity_limits = dict(SANITY_LIMITS)

        if 'filter_token' in d:
            cfg.filter_token = dict(d['filter_token'])
        else:
            cfg.filter_token = {
                'Sloan_g': 'g', 'Sloan_r': 'r', 'Sloan_i': 'i',
                'Sloan_z': 'z', 'N': 'N',
            }

        cfg.model_file = d.get('model_file', os.environ.get(
            'RTS2_BGNOISE_MODEL',
            str(Path(__file__).parent / 'bgnoise_model.pkl'),
        ))

        cfg._calc = ExposureCalculator(
            gain=cfg.gain, readnoise=cfg.readnoise, ape=cfg.ape,
        )
        return cfg

    @classmethod
    def load(cls, path: Optional[str] = None) -> 'TelescopeConfig':
        """
        Load config from path, or from $RTS2_OBSERVE_CONFIG, or
        fall back to the built-in D50/C0 config.
        """
        p = path or _DEFAULT_CONFIG
        if os.path.exists(p):
            return cls.from_yaml(p)
        return cls()   # built-in defaults

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def to_yaml(self, path: str) -> None:
        """Save this config to a YAML file (useful for generating templates)."""
        d = {
            'gain':         self.gain,
            'readnoise':    self.readnoise,
            'ape':          self.ape,
            'default_fwhm': self.default_fwhm,
            'filter_params': {
                f: {'Z0': v['Z0'], 'beta': v['beta']}
                for f, v in self.filter_params.items()
            },
            'sanity_limits': {
                f: {'zp_min': v['zp_min'], 'zp_max': v['zp_max']}
                for f, v in self.sanity_limits.items()
            },
            'filter_token': self.filter_token,
            'model_file':   self.model_file,
        }
        if self.plate_scale is not None:
            d['plate_scale'] = self.plate_scale
        with open(path, 'w') as fh:
            yaml.dump(d, fh, default_flow_style=False, sort_keys=False)

    # ------------------------------------------------------------------
    # Exposure calculator delegation
    # ------------------------------------------------------------------

    def sky_1s_from_bgnoise(self, bgnoise_1s: float) -> float:
        return self._calc.sky_1s_from_bgnoise(bgnoise_1s)

    def calculate_exptime(self, magnitude, magerror, fwhm, zp_1s, sky_1s) -> float:
        """magzero_arg convention: pass zp_1s directly (offset applied internally)."""
        return self._calc.calculate_exptime(magnitude, magerror, fwhm, zp_1s - 10.0, sky_1s)

    def predict_performance(self, magnitude, exptime, fwhm, zp_1s, sky_1s):
        """Return (magerror, snr) for the given exposure and conditions."""
        return self._calc.predict_performance(magnitude, exptime, fwhm, zp_1s - 10.0, sky_1s)

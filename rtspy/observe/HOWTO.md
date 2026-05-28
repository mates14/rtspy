# rtspy.observe — Adaptive Exposure Planner: HOWTO

This package predicts the exposure time required to reach a target SNR for a
given magnitude and filter, given the current photometric conditions at the
telescope.  It can optionally execute the resulting plan on camera C0 via RTS2.

---

## Quick start (D50/C0 already deployed)

```bash
export RTS2_STAT_FILE=/path/to/stat.txt    # pipeline photometric log

# Compute a plan (no execution)
rtspy-observe -m 18.5 -s 10
rtspy-observe -m 20.0 -s 10 --filter Sloan_r --fwhm 3.5

# Compute and execute on C0
rtspy-observe -m 18.5 -s 10 --execute
```

Output example:

```
Conditions [Sloan_r, airmass=1.23]
  JD           2460676.51234
  Sun / Moon   -35.2°  /  -12.1°
  ZP (1 s)     21.74 ± 0.06 mag   (14 obs in window)
  bgnoise_1s   4.87 ADU  [ML]

Target
  Magnitude    18.50
  Required SNR 10.0   →  mag error 0.1086
  FWHM         3.0 px

Result
  Needed       47 s
  Plan         1×60 s   (total 60 s)
  Expected SNR 12.4

To execute manually, run RTS2 script:
  filter=r for 1 do E 60 done
Or re-run with --execute.
```

---

## How it works

The planner combines three models:

**1. Zeropoint predictor** (`zp_predict.py`)
Real-time estimate of the current 1s-normalised photometric zeropoint.
Reads the last 15 minutes of `stat.txt`, converts all filter observations to a
common extinction coefficient `k_r(t)` via the physical model
`zp_1s = Z0_f − (k_r + β_f) · X`, fits a weighted linear trend, and
extrapolates to now.  Falls back to calibrated `Z0` values when no recent data
exist.

**2. Background noise predictor** (`bg_predict.py`)
Machine-learned estimate of the current 1s sky background noise (ADU).
A `HistGradientBoostingRegressor` is trained on historical `stat.txt` data
using sun altitude, moon altitude, moon illumination, night fraction, airmass,
filter, and zeropoint as features.  A real-time multiplicative correction is
applied from the most recent actual background measurements.

**3. Exposure calculator** (`expcalc.py`)
Given a 1s-normalised zeropoint, sky brightness, and FWHM, solves for the
exposure time that achieves the requested SNR using the photon+sky noise model
parameterised by `APE`, `GAIN`, and `RN`.

---

## Data files

| File | Description |
|------|-------------|
| `stat.txt` | Pipeline photometric log.  Set path via `RTS2_STAT_FILE` or `--stat`. |
| `bgnoise_model.pkl` | Trained background noise model.  Set path via `RTS2_BGNOISE_MODEL`. |
| `config_*.yaml` | Telescope configuration (one per camera).  Set path via `RTS2_OBSERVE_CONFIG` or `--config`. |

---

## Telescope configuration YAML

Every camera has its own YAML config.  See `config_d50_c0.yaml` as a
filled-in template.  The key sections:

```yaml
# Camera hardware — must be measured for each detector
gain: 0.81        # e-/ADU  (from CCD datasheet / photon transfer curve)
readnoise: 8.0    # electrons RMS  (from bias/dark frame analysis)
ape: 6.146        # aperture growth curve parameter  (fitted by apecalfit)
default_fwhm: 3.0 # pixels  (typical median seeing; co-calibrated with APE)

# Photometric calibration — fitted by zpfit on stat.txt
filter_params:
  Sloan_r: {Z0: 21.937985, beta:  0.000000}   # reference band
  Sloan_g: {Z0: 21.919920, beta:  0.099949}
  Sloan_i: {Z0: 21.394691, beta: -0.067270}
  Sloan_z: {Z0: 20.365853, beta: -0.056017}
  N:       {Z0: 22.986458, beta: -0.012176}

# Sanity limits for zp_1s (pre-screen corrupt pipeline output)
sanity_limits:
  Sloan_r: {zp_min: 16.8, zp_max: 22.0}
  ...

# Filter wheel tokens (filter name → RTS2 camera token)
filter_token:
  Sloan_r: r
  ...
```

**Z0** encodes instrument throughput + absolute calibration.  It drifts after
mirror re-coating, filter swaps, or detector replacement → re-run `zpfit`.

**β** encodes wavelength-dependent atmospheric extinction (Rayleigh + aerosol).
It is stable over years and rarely needs recalibration.

**APE** and **default_fwhm** are co-calibrated: APE is only meaningful relative
to the FWHM assumption.  Always re-fit both together with `apecalfit`.

---

## Deploying to a new telescope

### Step 1 — Measure hardware constants

Obtain `GAIN` and `RN` from the camera datasheet or from your own measurements:
- **GAIN** [e-/ADU]: from a photon transfer curve (PTC) or manufacturer spec.
- **RN** [electrons RMS]: from the standard deviation of bias/dark-subtracted
  bias frames, converted to electrons (`σ_ADU × GAIN`).

Start with the `config_d50_c0.yaml` template:

```bash
cp rtspy/observe/config_d50_c0.yaml config_mytelescope.yaml
# Edit gain, readnoise, and filter_token to match your camera.
# Leave ape, default_fwhm, filter_params as placeholders for now.
```

### Step 2 — Fit photometric calibration constants (Z0, β)

Collect at least a few weeks of `stat.txt` from the pipeline, covering a range
of airmasses and atmospheric conditions.

```bash
rtspy-observe-zpfit \
    --stat /path/to/stat.txt \
    --config config_mytelescope.yaml \
    --update-config
```

This writes the fitted `Z0` and `β` per filter back into the YAML.

**Notes:**
- The fit requires quasi-simultaneous multi-filter observations (pairs within
  ~14 min at similar airmass).  Nights with only one filter contribute nothing.
- `g`, `r`, `i` are fitted jointly (Stage 1); `z` and `N` are fitted
  separately using the `k_r(t)` time series from Stage 1 (Stage 2).
- `z`-band has extra scatter from unmodelled water vapour.  This is expected.
- Recalibrate after: filter swap, mirror re-coating, major optics change.

### Step 3 — Fit the exposure model parameter (APE)

Collect per-image ECSV catalogs produced by the photometric pipeline
(`*-df.ecsv` or `*-dft.ecsv` files).  Aim for at least 100 images covering a
range of sky conditions and magnitudes.

```bash
rtspy-observe-apecalfit \
    --data '/path/to/catalogs/*.ecsv' \
    --config config_mytelescope.yaml \
    --update-config
```

This writes `ape` and `default_fwhm` (= median header FWHM of the calibration
data) back into the YAML.

**Critical: APE and default_fwhm are co-calibrated.**
The fitter uses the per-image header `FWHM` keyword (the pipeline's estimate
of the PSF width for the whole frame).  The runtime uses `default_fwhm` from
the config as the fallback FWHM when `--fwhm` is not given.  These must be
consistent, which is why `--update-config` sets both simultaneously.

Use calibration data from **nights with typical seeing** for the site.  Data
from unusually good or bad nights will bias `default_fwhm` and therefore `APE`.

**Quality considerations:**
- Exclude images from nights with heavy clouds (pipeline may not produce
  reliable calibrations anyway).
- If the FWHM distribution is very wide, consider running the fitter with
  `--min-fwhm` and `--max-fwhm` to restrict to the typical range.

### Step 4 — Train the background noise model

```bash
rtspy-observe-train \
    --stat /path/to/stat.txt \
    --config config_mytelescope.yaml
```

The model is saved to the path specified in the config (`model_file` key, or
the package directory by default).  Training takes ~10–20 seconds on a typical
dataset of 100k–200k observations.

The held-out test results printed at the end show the expected prediction
accuracy.  A median relative error of 10–15% is typical.

**Notes:**
- More training data → better model.  A month of data (~50k observations) is
  a reasonable minimum.  Several months is better.
- The model captures moon × cirrus multiplicative effects (cirrus amplifies
  moonlight scatter), time-of-night trends in airglow, and filter-specific sky
  SED differences.
- Re-train periodically as conditions and instrument sensitivity change.

### Step 5 — Deploy

Copy the config YAML and trained model to the telescope:

```bash
scp config_mytelescope.yaml telescope:/etc/rts2/
scp bgnoise_model.pkl       telescope:/etc/rts2/
```

On the telescope, set environment variables (add to the RTS2 startup script or
`.bashrc`):

```bash
export RTS2_STAT_FILE=/var/log/rts2/stat.txt
export RTS2_OBSERVE_CONFIG=/etc/rts2/config_mytelescope.yaml
```

Test in calculation-only mode before going live:

```bash
rtspy-observe -m 18.5 -s 10
rtspy-observe -m 20.0 -s 10 --filter Sloan_g
```

When satisfied:

```bash
rtspy-observe -m 18.5 -s 10 --execute
```

---

## Recalibration schedule

| What changed | Action |
|--------------|--------|
| Mirror re-coating | Re-run `zpfit` (Z0 drifts; β stable) |
| Filter replacement | Re-run `zpfit` (both Z0 and β may change) |
| Detector replacement | Re-run `apecalfit` (new GAIN, RN, APE) and `zpfit` |
| Reduction pipeline update (aperture radius changed) | Re-run `apecalfit` |
| Significant instrument throughput change (>0.1 mag) | Re-run `zpfit` |
| Periodic (every 6–12 months) | Re-run `train` to keep background model fresh |

Cross-filter ZP prediction will silently degrade if Z0 drifts in only one
filter.  Symptom: single-filter predictions are fine but cross-filter ones show
a systematic offset.  Fix: re-run `zpfit`.

---

## Physical background

### Why ZP transfers between filters but sky brightness does not

The zeropoint model uses a single shared extinction coefficient `k_r(t)` for
all filters.  Per-filter differences (`β_f`) are stable (atmospheric physics).
This means a ZP measurement in `r` contains strong information about the ZP in
`g` or `i`.

Sky brightness has no shared scalar.  The sky SED is a mixture of OH airglow
(spiky, near-IR), moonlight (grey-ish), Rayleigh-scattered airglow (blue),
twilight (solar), and light pollution.  The ratio of these components changes
with time, moon phase, and airmass.  A bright sky in `g` tells you almost
nothing about the sky in `z`.  This is why the background model is ML-based
rather than formula-based.

### Filter-by-filter ZP reliability

| Filter | Reliability | Reason |
|--------|------------|--------|
| g, r, i | High | β dominated by Rayleigh + aerosols; both stable at this site |
| z | Limited | k_H2O(t) unmodelled; adds ±0.05–0.15 mag/airmass scatter |
| N (unfiltered) | Medium | Good precision (more photons), variable accuracy (effective λ shifts with source colour) |

### Exposure model (APE)

The `break_magnitude` separates the photon-noise regime from the background-
noise regime:

```
break_mag = −2.5·log10(APE · π/4 · FWHM² · (BGSIGMA·GAIN)²) + 10
```

Stars fainter than `break_mag` are sky-noise limited; brighter ones are photon-
noise limited.  The smooth transition function `sbl` interpolates between the
two regimes with slopes 0.2 (photon) and 0.4 (sky) in log–log space.

---

## Troubleshooting

**"Warning: could not read stat.txt"**
Check `RTS2_STAT_FILE` or `--stat`.  The planner falls back to calibrated Z0
values with a 0.3 mag uncertainty — still usable but less accurate.

**Predicted SNR much lower than achieved**
ZP may have drifted (Z0 needs updating) or the background model may be stale.
Re-run `zpfit` and `train`.

**Predicted SNR much higher than achieved**
Clouds or thin cirrus may have arrived.  The real-time ZP predictor will
normally catch this within one 15-minute window.  If persistent, check whether
the pipeline is producing correct ZP values in `stat.txt`.

**"filter X not in config"**
The filter name used on the command line must exactly match a key in
`filter_params` of the YAML.  Check casing: `Sloan_r` not `sloan_r`.

**apecalfit gives very high RMS (>0.15 dex)**
Either the calibration data is from atypical conditions (very bad seeing, heavy
cirrus) or the FWHM range is too wide.  Try restricting with `--min-fwhm` and
`--max-fwhm`.  Also check that `FLAGS=0` stars are present (some pipelines set
FLAGS > 0 for all objects).

**zpfit finds few pairs**
The pairing algorithm requires observations in two different filters within
`MAX_TIME_DIFF=0.01` days (~14 min) at similar airmass.  If the telescope
observes one filter per night, Stage 1 will have no data.  In that case only
`N`-band (if used) or a pre-existing calibration can anchor the cross-filter
predictions.

"""
What a device is and which code it runs: DRIVER, BUILD and LINKED.

Matches rts2ng's identity values. Computed once per process.

BUILD is, in order of preference:
  1. $RTSPY_BUILD_ID, for a packaging script to hand over the package
     version (do not let it git-describe a tree it has just modified, that
     gives a false "-dirty"),
  2. `git describe --always --dirty --tags` when rtspy runs from a git
     checkout (the usual editable install),
  3. the installed distribution's version.
LINKED is HEAD's commit time for a checkout, otherwise the install time
(mtime of the dist-info directory).
"""

import functools
import os
import subprocess
import sys
from typing import NamedTuple, Optional


class Identity(NamedTuple):
    driver: str
    build: str
    linked: float  # unix time, nan if unknown


def _driver_name() -> str:
    argv0 = sys.argv[0] if sys.argv and sys.argv[0] else ''
    base = os.path.basename(argv0)
    if base in ('', '-c', '__main__.py') or base.endswith('.py'):
        # python -m pkg.mod or python script.py: use the module name
        main = sys.modules.get('__main__')
        spec = getattr(main, '__spec__', None)
        if spec is not None and spec.name:
            name = spec.name
            if name.endswith('.__main__'):
                name = name[:-len('.__main__')]
            return name
        if base.endswith('.py'):
            return base[:-3]
        return 'python'
    return base


def _git(srcdir: str, *args: str) -> Optional[str]:
    try:
        out = subprocess.run(
            ['git', '-C', srcdir, *args],
            capture_output=True, text=True, timeout=5,
            # the daemon may run as a different user than owns the checkout
            env={**os.environ, 'GIT_CONFIG_COUNT': '1',
                 'GIT_CONFIG_KEY_0': 'safe.directory',
                 'GIT_CONFIG_VALUE_0': '*'},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def _checkout_root() -> Optional[str]:
    """The git checkout rtspy is imported from, if any."""
    pkgdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    d = os.path.dirname(pkgdir)
    while True:
        if os.path.exists(os.path.join(d, '.git')):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent


def _dist_info():
    try:
        from importlib import metadata
        dist = metadata.distribution('rtspy')
    except Exception:
        return None, float('nan')
    linked = float('nan')
    path = getattr(dist, '_path', None)
    if path is not None:
        try:
            linked = os.stat(path).st_mtime
        except OSError:
            pass
    return dist.version, linked


@functools.lru_cache(maxsize=None)
def get_identity() -> Identity:
    driver = _driver_name()
    build = None
    linked = float('nan')

    root = _checkout_root()
    if root:
        build = _git(root, 'describe', '--always', '--dirty', '--tags')
        ct = _git(root, 'log', '-1', '--format=%ct')
        if ct:
            try:
                linked = float(ct)
            except ValueError:
                pass

    env_build = os.environ.get('RTSPY_BUILD_ID')
    if env_build:
        build = env_build

    if not build or linked != linked:
        version, dist_linked = _dist_info()
        if not build:
            build = version or 'unknown'
        if linked != linked:
            linked = dist_linked

    return Identity(driver, build, linked)

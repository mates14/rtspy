from setuptools import setup

# This setup.py works alongside pyproject.toml to install shell script wrappers
# Shell scripts cannot be installed via pyproject.toml [project.scripts] alone
setup(
    scripts=[
        'rtspy/scripts/rtspy-gcnkafka',
        'rtspy/scripts/rtspy-queuer',
        'rtspy/scripts/rts2-observe',
        'rtspy/scripts/rtspy-rotate-log',
        'rtspy/scripts/rtspy-split-log',
    ]
)

from setuptools import setup

# This setup.py works alongside pyproject.toml to install shell script wrappers
# Shell scripts cannot be installed via pyproject.toml [project.scripts] alone
setup(
    scripts=[
        'rtspy/scripts/rts2-gcnkafka',
        'rtspy/scripts/rts2-queuer',
        'rtspy/scripts/rts2-filterd-ovis',
    ]
)

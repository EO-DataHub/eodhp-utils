# eodhp-utils

## Contents
Contains functions for:
- AWS
  - S3
- Pulsar

## Importing
This package may be used as follows:

1. Install with pip
```bash
pip install "git+https://github.com/UKEODHP/eodhp-utils.git"
```

If a specific version is required:
```bash
pip install "git+https://github.com/UKEODHP/eodhp-utils.git@va.b.c"
```

For use in pyproject.toml:
```python
dependencies = [
    "eodhp-utils @ git+https://github.com/EO-DataHub/eodhp-utils.git@va.b.c",
]
```

2. Import
```python
from eodhp_utils.pulsar.messages import generate_harvest_schema

print(generate_harvest_schema())
```


## Install locally via makefile

```commandline
make setup
```

This will create a virtual environment called `venv`, build `requirements.txt` and
`requirements-dev.txt` from `pyproject.toml` if they're out of date, install the Python
and Node dependencies and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.
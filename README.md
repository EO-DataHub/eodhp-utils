# eodhp-utils

eodhp-utils is a library used primarily by EODH components which communicate over Pulsar.

It contains:

- Definitions of the format of some of the Pulsar messages suitable for use as Pulsar
  Schema definitions. Harvest pipeline messages pre-date this library and are not
  included. These are in eodhp_utils.pulsar.messages.
- The 'Messagers' framework and the 'runner'. These handle the event loop and interaction
  with Pulsar, passing messages to caller-provided 'Messagers' and sending the results
  as new messages. Using this framework provides common CLI parameters, a multi-threading
  mechanism, etc. Read the docstrings in eodhp_utils.pulsar.messages, especially for
  Messager.
- The egress classifier which classifies IP addresses as within-region, cross-region or
  internet (ie, not AWS IP addresses). This is in eodhp_utils.aws.egress_classifier.

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

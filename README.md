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

## Pulsar configuration

`eodhp_utils.runner.get_pulsar_client()` and the `Runner` read these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PULSAR_URL` | `pulsar://pulsar-broker.pulsar:6650` | Broker URL, used when none is passed in. |
| `PULSAR_TOKEN_FILE` | unset | Path to a file containing a JWT, eg a mounted Secret. The file is read again on every new connection and every auth challenge, so a rotated Secret is picked up without a restart. |
| `PULSAR_TOKEN` | unset | A JWT given directly. Ignored if `PULSAR_TOKEN_FILE` is set. |
| `PULSAR_DEBUG_TOPIC` | `eodhp-utils-debugging` | Topic used for takeover messages. May be fully qualified, eg `persistent://public/billing/eodhp-utils-debugging`. |
| `PULSAR_TAKEOVER_ENABLED` | `true` | Set to `false` to stop the Runner subscribing to the debug topic, so it can't be paused by takeover messages. Running in takeover mode still sends them. |
| `PULSAR_DEAD_LETTER_TOPIC` | `dead-letter-<subscription name>` | Topic that messages are sent to after 3 redeliveries. May be fully qualified. |

If neither token variable is set, no authentication is used. If `PULSAR_TOKEN_FILE` is set
but the file is missing, unreadable or empty when the client is created, an exception is
raised so the service fails at startup. If the file later becomes unreadable, an error is
logged and an empty token is sent, which the broker rejects. The client then retries, reading
the file again each time.

Components that create their own `pulsar.Client` can use the same settings:

```python
import pulsar

from eodhp_utils.runner import pulsar_authentication

client = pulsar.Client(pulsar_url, authentication=pulsar_authentication())
```

Topics passed to the `Runner` may be short names (`billing-events`), `tenant/namespace/topic`
or fully qualified (`persistent://public/billing/billing-events`). To read from an old and a
new topic at the same time, pass the same Messager under both names:

```python
run(
    {
        "billing-events": messager,
        "persistent://public/billing/billing-events": messager,
    },
    "billing-ingester",
)
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

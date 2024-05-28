# eodhp-utils

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

2. Import
```python
from eodhp_utils.pulsar.messages import generate_harvest_schema

print(generate_harvest_schema())
```

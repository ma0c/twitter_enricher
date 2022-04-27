# Twitter Enricher

This is a simple twitter stream reader that enriches data from a Twitter account with 
the weather information about the tweet location and a sliding average of the temperature.

## Prerequisites

- Python 3.8+
- [Poetry](https://python-poetry.org/) (not required)
- [direnv](https://direnv.net/) (not required)

## Installation

```bash
poetry install
poetry shell
```

Not using poetry?

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Testing

Use pytest

```bash
pytest
```

Want to test an individual test?

```bash
pytest tests/test_playground.py::TestTwitterURLBuilder::test_get_url_without_extras
```

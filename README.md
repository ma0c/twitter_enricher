# Twitter Enricher

This is a simple twitter stream reader that enriches data from a Twitter account with 
the weather information about the tweet location and a sliding average of the temperature.

## Prerequisites

- Python 3.8+
- Twitter API Access token
- Weather API key
- [direnv](https://direnv.net/) (not required)

## Installation

```console
$ python -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

## Run

Make sure you have the following env variables in your shell

```shell
TWITTER_BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAA6..."
WEATHER_API_API_KEY="29..."
```

If you're using direnv use the following command to copy the example file and put the appropriate values

```console
$ cp envrc.example .envrc
$ direnv allow
```

To get the values check the following links:

- [Twitter Access](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api)
- [Weather Api access](https://www.weatherapi.com/docs/)

Then execute

```console
$ python playground.py --tr 10 --s 5
```

See all options using

```console
$ python playground.py --help
```

## Testing

Use pytest

```console
$ pytest
```


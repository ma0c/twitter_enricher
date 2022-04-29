from abc import ABC, abstractmethod
import argparse
from enum import Enum
import json
import logging
import os
from typing import List
import urllib.parse

LOGGER = logging.getLogger(__name__)


class TwitterStreamExpansions(str, Enum):
    POLL_IDS = "attachments.poll_ids"
    MEDIA_KEYS = "attachments.media_keys"
    AUTHOR_ID = "author_id"
    USERNAME = "entities.mentions.username"
    PLACE_ID = "geo.place_id"
    IN_REPLY_TO_USER_ID = "in_reply_to_user_id"
    REFERENCED_TWEET_ID = "referenced_tweets.id"
    REFERENCED_TWEET_AUTHOR_ID = "referenced_tweets.id.author_id"


class TwitterStreamPlaceFields(str, Enum):
    CONTAINED_WITHIN = "contained_within"
    COUNTRY = "country"
    COUNTRY_CODE = "country_code"
    FULL_NAME = "full_name"
    GEO = "geo"
    ID = "id"
    NAME = "name"
    PLACE_TYPE = "place_type"


class TwitterStreamFields(str, Enum):
    ATTACHMENTS = "attachments"
    AUTHOR_ID = "author_id"
    GEO = "geo"


class TwitterURLBuilder:
    ENVIRON_TWITTER_BEARER_TOKEN_NAME = "TWITTER_BEARER_TOKEN"
    STREAM_API_ENDPOINT = "https://api.twitter.com/2/tweets/sample/stream"

    def get_headers(self):
        bearer_token = os.environ.get(self.ENVIRON_TWITTER_BEARER_TOKEN_NAME)
        if not bearer_token:
            LOGGER.warning("Twitter Bearer Token not found")
            raise ValueError("Twitter Bearer Token not found")
        return {
            "Authorization": f"Bearer {bearer_token}"
        }

    def get_url(
            self,
            twitter_fields: List[TwitterStreamFields] = None,
            expansions: List[TwitterStreamExpansions] = None,
            place_fields: List[TwitterStreamPlaceFields] = None,

    ):
        query_params = dict()
        if twitter_fields:
            query_params["tweet.fields"] = ",".join(twitter_fields)
        if expansions:
            query_params["expansions"] = ",".join(expansions)
        if place_fields:
            query_params["place.fields"] = ",".join(place_fields)

        parsed_params = urllib.parse.urlencode(query_params)
        LOGGER.debug(f"Parsed params: {parsed_params}")
        return f"{self.STREAM_API_ENDPOINT}?{parsed_params}" if parsed_params else self.STREAM_API_ENDPOINT


class WeatherApiUrlBuilder:
    ENVIRON_WEATHER_API_API_NAME = "ENVIRON_WEATHER_API_API_NAME"
    WEATHER_API_ENDPOINT = "http://api.weatherapi.com/v1/current.json"

    def get_url(self, location):
        weather_api_api_key = os.environ.get(self.ENVIRON_WEATHER_API_API_NAME)
        if not weather_api_api_key:
            LOGGER.warning("Twitter Bearer Token not found")
            raise ValueError("Twitter Bearer Token not found")

        return f"?key={weather_api_api_key}&q={location}"


class HttpClient(ABC):
    @abstractmethod
    def get(self, url, headers):
        raise NotImplemented


class HTTPStreamClient(ABC):
    @abstractmethod
    def get(self, url, headers):
        raise NotImplemented

    @abstractmethod
    def close(self):
        raise NotImplemented


class RequestsHttpClient(HttpClient):
    def __init__(self):
        import requests
        self._client = requests
        self._response = None

    def get(self, url, headers):
        return self._client.get(url=url, headers=headers)


class RequestsHttpStreamClient(HTTPStreamClient):

    def __init__(self):
        import requests
        self._client = requests
        self._response = None

    def get(self, url, headers):
        self._response = self._client.get(url=url, headers=headers, stream=True)
        return self._response.iter_lines()

    def close(self):
        self._response.close()


class TwitterStreamProcessor:

    def __init__(self, client: HTTPStreamClient):
        self.client = client
        self.twitter_url_builder = TwitterURLBuilder()
        self.received_items = 0

    def process(self, n=None, **kwargs):
        """
        Tweet with information example:
{'data': {'geo': {'place_id': '01c060cf466c6ce3'},
          'id': '1519867438057213952',
          'text': '@StvrChild777 Made me sick'},
 'includes': {'places': [{'full_name': 'Long Beach, CA',
                          'geo': {'bbox': [-118.250227,
                                           33.732905,
                                           -118.0631938,
                                           33.885438],
                                  'properties': {},
                                  'type': 'Feature'},
                          'id': '01c060cf466c6ce3'}]}}


        :param n: Maximum number of tweets to read
        :param kwargs:
        :return:
        """
        stream = self.client.get(
            url=self.twitter_url_builder.get_url(
                twitter_fields=[TwitterStreamFields.GEO],
                expansions=[TwitterStreamExpansions.PLACE_ID],
                place_fields=[
                    TwitterStreamPlaceFields.GEO,
                    TwitterStreamPlaceFields.FULL_NAME,
                    TwitterStreamPlaceFields.PLACE_TYPE
                ]
            ),
            headers=self.twitter_url_builder.get_headers()
        )

        for element in stream:
            if element:
                self.received_items += 1
                json_value = json.loads(element)
                LOGGER.debug(json_value)

            if n and self.received_items > n:
                self.client.close()
                break


def main(**kwargs):
    requests_client = RequestsHttpStreamClient()
    twitter_processor = TwitterStreamProcessor(requests_client)
    twitter_processor.process(**kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Enrich twitter stream with weather information, use --n to specify the number of tweets to analyze"
    )

    parser.add_argument("-max_elements", "--n", type=int, help="Maximiun number of tweets to process")
    parser.add_argument("-v", default=False, action="store_true", help="Set the verbosity to the highest level")

    args = parser.parse_args()
    LOGGER.addHandler(logging.StreamHandler())
    if args.v:
        LOGGER.setLevel("DEBUG")

    main(**vars(args))



def centroid_calculator(geo_json):
    import geojson
    from turfpy.measurement import center
    from geojson.feature import Feature
    test = "{\"type\":\"Feature\",\"bbox\":[-118.250227,33.732905,-118.0631938,33.885438],\"properties\":{}}"
    geo_object = geojson.loads(test)
    center(geo_object)

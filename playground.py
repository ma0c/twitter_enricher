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
    ENVIRON_WEATHER_API_API_NAME = "WEATHER_API_API_KEY"
    WEATHER_API_ENDPOINT = "http://api.weatherapi.com/v1/current.json"

    def get_url(self, location):
        weather_api_api_key = os.environ.get(self.ENVIRON_WEATHER_API_API_NAME)
        if not weather_api_api_key:
            LOGGER.warning("Weather api key not found")
            raise ValueError("Weather api key not found")

        return f"{self.WEATHER_API_ENDPOINT}?key={weather_api_api_key}&q={location}"


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


def centroid_calculator(geo_json):
    if geo_json.get("bbox"):
        bbox = geo_json["bbox"]
        return (bbox[0]+bbox[2])/2, (bbox[1]+bbox[3])/2
    else:
        raise ValueError(f"Not implemented method for extracting centroid in {geo_json}")


class TwitterStreamProcessor:

    def __init__(self, stream_client: HTTPStreamClient, http_client: HttpClient):
        self.stream_client = stream_client
        self.http_client = http_client
        self.twitter_url_builder = TwitterURLBuilder()
        self.weather_api_url_builder = WeatherApiUrlBuilder()
        self.received_items = 0
        self.temperature_read_total_count = 0
        self.temperature_reads = list()

    def process(self, s, temp_f,  sliding_avg, n=None, tr=None, **kwargs):
        """
        Tweet with information example:
{
    'data': {
        'geo': {
            'place_id': '01c060cf466c6ce3'
        },
        'id': '1519867438057213952',
        'text': '@StvrChild777 Made me sick'
    },
    'includes': {
        'places': [
            {
                'full_name': 'Long Beach, CA',
                'geo': {
                    'bbox': [
                        -118.250227,
                        33.732905,
                        -118.0631938,
                        33.885438
                    ],
                'properties': {},
                'type': 'Feature'
                },
                'id': '01c060cf466c6ce3'
            }
        ]
    }
}
        :param s: Size of the sliding average
        :param temp_f: Location for storing the temperatures
        :param sliding_avg: Location for storing the sliding avg
        :param n: Maximum number of tweets to read
        :param tr: Maximum number of temperature reads
        :param kwargs:
        :return:
        """
        with open(temp_f, "w+") as temp_f_file, open(sliding_avg, "w+") as sliding_avg_file:
            stream = self.stream_client.get(
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
            try:
                for element in stream:
                    if element:
                        self.received_items += 1
                        json_value = json.loads(element)
                        LOGGER.debug(json_value)
                        if self.has_geo_information(json_value):
                            self.temperature_read_total_count += 1
                            geo_value = json_value["includes"]["places"][0]["geo"]
                            full_name = json_value["includes"]["places"][0]["full_name"]
                            LOGGER.debug(geo_value)
                            centroid = centroid_calculator(geo_value)
                            weather_url = self.weather_api_url_builder.get_url(f"{centroid[1]},{centroid[0]}")
                            weather_response = self.http_client.get(weather_url, {})
                            if weather_response.ok:
                                weather_response_json = weather_response.json()
                                current_weather = weather_response_json.get("current", dict()).get("temp_f")
                                if current_weather:

                                    LOGGER.info(f"Weather in {full_name} ({centroid}): {current_weather}")
                                    self.temperature_reads.append(current_weather)
                                    if len(self.temperature_reads) > s:
                                        # If we exceeded the size for sliding AVG we drop the first element
                                        self.temperature_reads.pop(0)
                                    current_sliding_avs = sum(self.temperature_reads) / len(self.temperature_reads)
                                    LOGGER.info(self.temperature_reads)
                                    temp_f_file.write(f"{current_weather}\n")
                                    sliding_avg_file.write(f"{current_sliding_avs}\n")
                                    print(f"Tweet location: {full_name}, temp_f: {current_weather}, avg of last {s}: {current_sliding_avs}")

                    if n and self.received_items >= n or tr and self.temperature_read_total_count >= tr:
                        self.stream_client.close()
                        break
            except KeyboardInterrupt:
                self.stream_client.close()

    @staticmethod
    def has_geo_information(twitter_data):
        return len(twitter_data.get("includes", dict()).get("places", list())) > 0


def main(**kwargs):
    requests_stream_client = RequestsHttpStreamClient()
    requests_http_client = RequestsHttpClient()
    twitter_processor = TwitterStreamProcessor(requests_stream_client, requests_http_client)
    twitter_processor.process(**kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Enrich twitter stream with weather information"
    )

    parser.add_argument("--n", type=int, help="Maximum number of tweets to process")
    parser.add_argument("--tr", type=int, help="Maximum number of temperature reads to process")
    parser.add_argument("--s", type=int, help="Sliding average size, beween 2 and 100", default=5)
    parser.add_argument("--v", default=False, action="store_true", help="Increase the verbosity level")
    parser.add_argument("--vv", default=False, action="store_true", help="Set the verbosity to the highest level")
    parser.add_argument("--temp_f", default="temp_f.txt", type=str, help="File location for the stream of temperatures")
    parser.add_argument("--sliding_avg", default="avg.txt", type=str, help="File location for the sliding average of temperatures")

    args = parser.parse_args()
    LOGGER.addHandler(logging.StreamHandler())

    if args.v:
        LOGGER.setLevel("INFO")
    if args.vv:
        LOGGER.setLevel("DEBUG")
    if not 2 <= args.s <= 100:
        raise ValueError(f"Sliding average size must be between 2 and 100, value given {args.s}")

    main(**vars(args))

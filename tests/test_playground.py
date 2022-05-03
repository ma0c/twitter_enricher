import json
import os
from unittest import TestCase, mock

from playground import (
    TwitterURLBuilder,
    TwitterStreamExpansions,
    TwitterStreamProcessor,
    RequestsHttpStreamClient,
    RequestsHttpClient,
    centroid_calculator
)


DUMMY_VALUE = "dummy_value"


class TestTwitterURLBuilder(TestCase):
    def setUp(self) -> None:
        self.builder = TwitterURLBuilder()

    def test_get_url_without_extras(self):
        assert self.builder.get_url() == TwitterURLBuilder.STREAM_API_ENDPOINT

    def test_get_url_with_expansions(self):

        assert self.builder.get_url(expansions=[TwitterStreamExpansions.PLACE_ID]) == \
               f"{TwitterURLBuilder.STREAM_API_ENDPOINT}?expansions=geo.place_id"

    @mock.patch.dict(os.environ, {TwitterURLBuilder.ENVIRON_TWITTER_BEARER_TOKEN_NAME: ""})
    def test_raise_on_no_bearer_token(self):
        with self.assertRaises(ValueError):
            self.builder.get_headers()

    @mock.patch.dict(os.environ, {TwitterURLBuilder.ENVIRON_TWITTER_BEARER_TOKEN_NAME: DUMMY_VALUE})
    def test_not_raises_on_bearer_token(self):
        assert self.builder.get_headers() == {"Authorization": f"Bearer {DUMMY_VALUE}"}


class TestTwitterStreamProcessor(TestCase):
    def setUp(self) -> None:
        self.stream_client = RequestsHttpStreamClient()
        self.client = RequestsHttpClient()
        self.processor = TwitterStreamProcessor(self.stream_client, self.client)

    def test_scoped_process(self):
        self.processor.process(10)

    def test_no_has_geo_information(self):
        assert not(
            self.processor.has_geo_information(
                {
                    "data": {
                        "geo": {},
                        "id": "1519867438057213952",
                        "text": "@StvrChild777 Made me sick"
                    }
                }
            )
        )

    def test_has_geo_information(self):
        assert (
            self.processor.has_geo_information(
                {
                    "data": {
                        "geo": {},
                        "id": "1519867438057213952",
                        "text": "@StvrChild777 Made me sick"
                    },
                    "includes": {
                        "places": [
                            {
                                "full_name": "Long Beach, CA",
                                "geo": {
                                    "bbox": [
                                        -118.250227,
                                        33.732905,
                                        -118.0631938,
                                        33.885438
                                    ],
                                    "properties": {},
                                    "type": "Feature"
                                },
                                "id": "01c060cf466c6ce3"
                            }
                        ]
                    }
                }
            )
        )



class TestCentroidCalculator(TestCase):

    def test_center_bbox(self):
        test_geo_json = "{\"type\":\"Feature\",\"bbox\":[-118.250227,33.732905,-118.0631938,33.885438],\"properties\":{}}"

        assert centroid_calculator(json.loads(test_geo_json)) == (-118.1567104, 33.809171500000005)

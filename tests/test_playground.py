import os
from unittest import TestCase, mock

from playground import TwitterURLBuilder, TwitterStreamExpansions, TwitterStreamProcessor, RequestsHttpStreamClient

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
        self.client = RequestsHttpStreamClient()
        self.processor = TwitterStreamProcessor(self.client)

    def test_scoped_process(self):
        self.processor.process(10)

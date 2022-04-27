from enum import Enum
import logging
import os
from typing import List
import urllib.parse

import requests

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

    def get_url(self, expansions: List[TwitterStreamExpansions] = None, place_fields: List[TwitterStreamPlaceFields] = None):
        query_params = dict()
        if expansions:
            query_params["expansions"] = ",".join(expansions)
        if place_fields:
            query_params["place_fields"] = ",".join(place_fields)

        parsed_params = urllib.parse.urlencode(query_params)

        return f"{self.STREAM_API_ENDPOINT}?{parsed_params}" if parsed_params else self.STREAM_API_ENDPOINT

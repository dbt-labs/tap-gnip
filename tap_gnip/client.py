import requests_oauthlib

from tap_framework.client import BaseClient


class GnipClient(BaseClient):

    def get_authorization(self):
        return requests_oauthlib.OAuth1(
            self.config.get('app_key'),
            self.config.get('app_secret'),
            self.config.get('access_token'),
            self.config.get('access_token_secret'))

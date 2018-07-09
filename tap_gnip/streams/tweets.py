import funcy
import singer
import dateutil.parser

from tap_framework.state import incorporate
from tap_framework.streams import BaseStream


LOGGER = singer.get_logger()


class TweetsStream(BaseStream):
    TABLE = 'tweets'
    KEY_PROPERTIES = ['id']
    API_METHOD = 'GET'
    REQUIRES = []

    def get_url(self):
        return 'https://api.twitter.com/1.1/statuses/user_timeline.json'

    def get_stream_data(self, data):
        return data

    def filter_keys(self, obj):
        to_return = super().filter_keys(obj)

        to_return['created_at'] = dateutil.parser.parse(
            to_return['created_at']).isoformat()
        to_return['user']['created_at'] = dateutil.parser.parse(
            to_return['user']['created_at']).isoformat()

        return to_return

    def sync_data(self):
        table = self.TABLE

        url = self.get_url()

        for handle in self.config.get('handles'):
            params = {
                'user_id': handle,
                'count': 200,
                'include_rts': 1,
            }
            has_more = True
            max_id = None

            while has_more:
                last_max_id = max_id

                result = self.client.make_request(
                    url, self.API_METHOD, params=params)

                data = self.get_stream_data(result)

                with singer.metrics.record_counter(endpoint=table) as counter:
                    for index, obj in enumerate(data):
                        LOGGER.debug("On {} of {}".format(index, len(data)))

                        processed = self.filter_keys(obj)

                        singer.write_records(
                            table,
                            [processed])

                        counter.increment()

                        if max_id is None:
                            max_id = obj.get('id')
                        else:
                            max_id = min(max_id, obj.get('id'))

                        params['max_id'] = max_id

                        self.state = incorporate(
                            self.state,
                            "tweet_engagements.{}".format(obj.get('id')),
                            'date',
                            processed.get('created_at'))

                for substream in self.substreams:
                    substream.state = self.state
                    for tweets in funcy.chunks(25, data):
                        substream.sync_data(
                            parent_ids=[tweet.get('id_str')
                                        for tweet in tweets])

                if last_max_id == max_id:
                    has_more = False

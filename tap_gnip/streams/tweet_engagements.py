from tap_framework.streams import ChildStream
from tap_framework.state import get_last_record_value_for_table, incorporate, \
    save_state

import datetime
import dateutil.parser
import pytz
import singer
import time

LOGGER = singer.get_logger()


class TweetEngagementsStream(ChildStream):
    TABLE = 'tweet_engagements'
    KEY_PROPERTIES = ['tweet_id', 'day', 'hour', 'engagement_type']
    API_METHOD = 'POST'
    REQUIRES = ['tweets']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_start = None

    def get_url(self):
        return 'https://data-api.twitter.com/insights/engagement/historical'

    def convert_date(self, day, hour):
        date = dateutil.parser.parse(day) + datetime.timedelta(hours=hour)
        return date.strftime('%Y-%m-%dT%H:%M:%SZ')

    def get_stream_data(self, data):
        to_return = []

        for tweet_id, tweet_data in data.get('hourly_stats').items():
            for engagement_type, engagement_data in tweet_data.items():
                for day, hours in engagement_data.items():
                    for hour, value in hours.items():
                        to_return.append({
                            'tweet_id': int(tweet_id),
                            'engagement_type': engagement_type,
                            'day': day,
                            'hour': int(hour),
                            'date': self.convert_date(day, int(hour)),
                            'value': int(value),
                        })

        return to_return

    def get_body(self, start, end, tweet_ids):
        return {
            "start": start.replace(
                minute=0, second=0, microsecond=0).isoformat(),
            "end": end.replace(
                minute=0, second=0, microsecond=0).isoformat(),
            "tweet_ids": tweet_ids,
            "engagement_types": [
                "impressions",
                "engagements",
                "url_clicks",
                "detail_expands"
            ],
            "groupings": {
                "hourly_stats": {
                    "group_by": [
                        "tweet.id",
                        "engagement.type",
                        "engagement.day",
                        "engagement.hour"
                    ]
                }
            }
        }

    def get_start_for_tweet_ids(self, tweet_ids):
        return min([
            get_last_record_value_for_table(
                self.state,
                'tweet_engagements.{}'.format(tweet_id))
            for tweet_id in tweet_ids]) - datetime.timedelta(hours=1)

    def sync_data(self, parent_ids=None):
        if parent_ids is None:
            raise RuntimeError('Cannot pull tweet engagement for {}'
                               .format(parent_ids))

        self.write_schema()

        start = self.get_start_for_tweet_ids(parent_ids)

        LOGGER.info("Pulling data from {} for a batch of 25 tweets"
                    .format(start))

        table = self.TABLE

        url = self.get_url()

        while True:
            start = self.get_start_for_tweet_ids(parent_ids)
            end = min(
                datetime.datetime.utcnow(),
                start + datetime.timedelta(weeks=4))

            if start > (end - datetime.timedelta(hours=3)):
                break

            body = self.get_body(start, end, parent_ids)

            self.request_start = datetime.datetime.utcnow()

            result = self.client.make_request(
                url, self.API_METHOD, body=body)

            data = self.get_stream_data(result)

            with singer.metrics.record_counter(endpoint=table) as counter:
                for index, obj in enumerate(data):
                    singer.write_records(
                        table,
                        [self.filter_keys(obj)])

                    self.state = incorporate(
                        self.state,
                        'tweet_engagements.{}'.format(obj.get('tweet_id')),
                        'date',
                        obj.get('date'))

                    counter.increment()

            save_state(self.state)

            max_sleep = 35
            sleep_seconds = min(
                max_sleep,
                ((self.request_start + datetime.timedelta(seconds=max_sleep)) -
                 datetime.datetime.utcnow()).seconds)

            if sleep_seconds > 0:
                LOGGER.info("Sleeping for {} seconds before making "
                            "next request".format(sleep_seconds))
                time.sleep(sleep_seconds)

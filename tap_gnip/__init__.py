import singer
import tap_framework
import tap_gnip.client
import tap_gnip.streams

LOGGER = singer.get_logger()


class GnipRunner(tap_framework.Runner):

    def get_streams_to_replicate(self):
        streams = []
        tweets_stream = None
        tweets_substreams = []

        for stream_catalog in self.catalog.streams:
            if not stream_catalog.schema.selected:
                LOGGER.info("'{}' is not marked selected, skipping."
                            .format(stream_catalog.stream))
                continue

            for available_stream in self.available_streams:
                if available_stream.matches_catalog(stream_catalog):
                    if not available_stream.requirements_met(self.catalog):
                        raise RuntimeError(
                            "{} requires that that the following are "
                            "selected: {}"
                            .format(stream_catalog.stream,
                                    ','.join(available_stream.REQUIRES)))

                    if available_stream.TABLE == 'tweets':
                        tweets_stream = available_stream(
                            self.config, self.state, stream_catalog,
                            self.client)

                    else:
                        tweets_substreams = [available_stream(
                            self.config, self.state, stream_catalog,
                            self.client)]

        if tweets_stream is not None:
            tweets_stream.substreams = tweets_substreams
            streams.append(tweets_stream)

        return streams


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(
        required_config_keys=['app_key', 'app_secret',
                              'access_token', 'access_token_secret',
                              'handles'])

    client = tap_gnip.client.GnipClient(args.config)
    runner = GnipRunner(
        args, client, tap_gnip.streams.AVAILABLE_STREAMS)

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == '__main__':
    main()

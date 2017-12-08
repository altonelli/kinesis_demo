from __future__ import print_function

import json
import time
import datetime
import logging

import boto
import argparse
from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException

from my_messages import poster
from my_messages.consumer import KinesisConsumer
from my_messages.constants import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')


class KinesisReadConsumer(KinesisConsumer):
    def __init__(self, stream_name, kinesis_connection, shard_id, iterator_type, consumer_time=40,
                 sleep_interval=0.5, name=None, group=None, args=(), kwargs={}):
        super(KinesisReadConsumer, self).__init__(stream_name=stream_name,
                                                  kinesis_connection=kinesis_connection,
                                                  shard_id=shard_id,
                                                  iterator_type=iterator_type,
                                                  consumer_time=consumer_time,
                                                  sleep_interval=sleep_interval,
                                                  name=name,
                                                  group=group,
                                                  args=args,
                                                  kwargs=kwargs)
        self.stream_name = stream_name
        self.kinesis_connection = kinesis_connection
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.consumer_time = consumer_time
        self.sleep_interval = sleep_interval
        self.total_records = 0

    def work(self):
        try:
            response = self.kinesis_connection.get_records(self.iterator, limit=25)
            if len(response['Records']) > 0:
                self.total_records += len(response['Records'])
                self.logger.info('\n+-> {1} Got {0} Consumer Records'.format(
                    len(response['Records']), self.name))
                for record in response['Records']:
                    self.read_record(record)
            self.iterator = response['NextShardIterator']
            time.sleep(self.sleep_interval)
        except ProvisionedThroughputExceededException as ptee:
            self.logger.error(ptee.message)
            time.sleep(5)

    def read_record(self, record):
        parsed_record = json.loads(record['Data'])
        self.logger.info("{0} said {1}".format(parsed_record['name'], parsed_record['message']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or connect to a Kinesis stream and create consumers
                       that read records''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
                        help='''the name of the Kinesis stream to either create or connect''')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='''the name of the Kinesis region to connect with [default: us-east-1]''')
    parser.add_argument('--consumer_time', type=int, default=40,
                        help='''the consumer's duration of operation in seconds [default: 30]''')
    parser.add_argument('--sleep_interval', type=float, default=0.2,
                        help='''the consumer's work loop sleep interval in seconds [default: 0.2]''')
    parser.add_argument('--iter_type', type=str, default=iter_type_latest,
                        help='''the consumer's work loop sleep interval in seconds [default: \'LATEST\']''')

    args = parser.parse_args()
    kinesis = boto.kinesis.connect_to_region(region_name=args.region)
    stream = kinesis.describe_stream(args.stream_name)
    logger.info(json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
    shards = stream['StreamDescription']['Shards']
    logger.info('# Shard Count: ' + str(len(shards)))

    consumers = []
    start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        consumer_name = 'shard_consumers:%s' % shard_id
        logger.info('#-> shardId: ' + str(shards[shard_id]['ShardId']))
        consumer = KinesisReadConsumer(
            stream_name=args.stream_name,
            kinesis_connection=kinesis,
            shard_id=shards[shard_id]['ShardId'],
            iterator_type=args.iter_type,
            consumer_time=args.consumer_time,
            sleep_interval=args.sleep_interval,
            name=consumer_name
        )
        consumer.daemon = True
        consumers.append(consumer)
        logger.info('#-> starting: ' + consumer_name)
        consumer.start()

    for consumer in consumers:
        consumer.join()

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    total_records = poster.sum_posts(consumers)
    print("\n")
    print("-=> Exiting Consumer Main <=-")
    print("  Total Records:", total_records)
    print("     Total Time:", duration)
    print("  Records / sec:", total_records / duration)
    print("  Consumer sleep interval:", args.sleep_interval)






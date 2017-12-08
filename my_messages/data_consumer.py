from __future__ import print_function

import json
import logging
import time
import datetime

import argparse
import boto
import pprint
from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException

from my_messages import poster
from my_messages.consumer import KinesisConsumer
from my_messages.constants import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')

class KinesisDataConsumer(KinesisConsumer):
    def __init__(self, stream_name, kinesis_connection, shard_id, iterator_type, consumer_time=40,
                 sleep_interval=.5, name=None, group=None, args=(), kwargs={}):
        super(KinesisDataConsumer, self).__init__(stream_name=stream_name,
                                                  kinesis_connection=kinesis_connection,
                                                  shard_id=shard_id,
                                                  iterator_type=iterator_type,
                                                  consumer_time=consumer_time,
                                                  sleep_interval=sleep_interval,
                                                  name=name,
                                                  group=group,
                                                  args=args,
                                                  kwargs=kwargs)
        self.total_records = 0
        self.author_data = {}
        self.message_data = {}

    def add_record_data(self, record):
        parsed_record = json.loads(record['Data'])
        self.logger.info('Consuming message {} by {} from {}'.format(parsed_record['message'],
                                                                     parsed_record['name'],
                                                                     self.name))
        author_count = self.author_data.get(parsed_record['name'])
        message_count = self.message_data.get(parsed_record['message'])
        if author_count:
            self.author_data[parsed_record['name']] += 1
        else:
            self.author_data[parsed_record['name']] = 1
        if message_count:
            self.message_data[parsed_record['message']] += 1
        else:
            self.message_data[parsed_record['message']] = 1

    def work(self):
        try:
            response = self.kinesis_connection.get_records(self.iterator, limit=25)
            if len(response['Records']) > 0:
                self.logger.info('\n+-> Thread {1} Got {0} Consumer Records'.format(
                    len(response['Records']), self.name))
                self.total_records += len(response['Records'])
                for record in response['Records']:
                    self.add_record_data(record)
            self.iterator = response['NextShardIterator']
            time.sleep(self.sleep_interval)
        except ProvisionedThroughputExceededException as ptee:
            self.logger.error(ptee.message)
            time.sleep(5)

def merge_consumer_dicts(consumers, attr):
    total_dict = {}
    for consumer in consumers:
        consumer_dict = getattr(consumer, attr)
        for key, value in consumer_dict.items():
            if key in total_dict:
                total_dict[key] += value
            else:
                total_dict[key] = value
    return total_dict

def calculate_sums(consumers):
    author_totals = merge_consumer_dicts(consumers, 'author_data')
    message_totals = merge_consumer_dicts(consumers, 'message_data')
    return (author_totals, message_totals)

def main():
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
                        help='''the consumer's work loop sleep interval in seconds [default: 0.1]''')
    parser.add_argument('--iter_type', type=str, default=iter_type_latest,
                        help='''the consumer's work loop sleep interval in seconds [default: \'LATEST\']''')

    args = parser.parse_args()
    kinesis = boto.kinesis.connect_to_region(region_name=args.region)
    stream = kinesis.describe_stream(args.stream_name)
    shards = stream['StreamDescription']['Shards']

    consumers = []
    start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        consumer_name = 'shard_data_consumer:{0}'.format(shard_id)
        logger.info('#-> shardId: ' + str(shards[shard_id]['ShardId']))
        data_consumer = KinesisDataConsumer(
            stream_name=args.stream_name,
            kinesis_connection=kinesis,
            shard_id=shards[shard_id]['ShardId'],
            iterator_type=args.iter_type,
            consumer_time=args.consumer_time,
            sleep_interval=args.sleep_interval,
            name=consumer_name
        )
        data_consumer.daemon = True
        consumers.append(data_consumer)
        logger.info('#-> starting: ' + consumer_name)
        data_consumer.start()

    for consumer in consumers:
        consumer.join()

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    (author_totals, message_totals) = calculate_sums(consumers)
    total_records = poster.sum_posts(consumers)
    print("Author Stats:")
    pprint.pprint(author_totals)
    print("Message Stats:")
    pprint.pprint(message_totals)
    print("\n")
    print("-=> Exiting Consumer Main <=-")
    print("     Total Time:", duration)
    print("  Total Records:", total_records)
    print("  Records / sec:", total_records / duration)
    print("  Consumer sleep interval:", args.sleep_interval)


if __name__ == '__main__':
    main()

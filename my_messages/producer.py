from __future__ import print_function

import boto
import argparse
import json
import threading
import logging
import time
import datetime
import pprint

from argparse import RawTextHelpFormatter
import random
from boto.kinesis.exceptions import ResourceNotFoundException

default_record = {'name': 'bot',
                  'message': 'hello_world'}

def get_or_create_stream(kinesis, stream_name, shard_count):
    stream = None
    try:
        stream = kinesis.describe_stream(stream_name)
        print(json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
    except ResourceNotFoundException as rnfe:
        while (stream is None) or ('ACTIVE' not in stream['StreamDescription']['StreamStatus']):
            if stream is None:
                print('Could not find ACTIVE stream: {0} trying to create.'.format(stream_name))
                kinesis.create_stream(stream_name, shard_count)
            else:
                print('Stream status: %s' % stream['StreamDescription']['StreamStatus'])
            time.sleep(1)
            stream = kinesis.describe_stream(stream_name)
    return stream

def sum_posts(kinesis_posters):
    """Sum all posts across an array of KinesisPosters
    """
    total_records = 0
    for poster in kinesis_posters:
        total_records += poster.total_records
    return total_records

class KinesisProducer(threading.Thread):
    def __init__(self, stream_name, partition_key, kinesis_connection, producer_time=30,
                 quiet=False, encryption_type=None, max_try=10, name=None, group=None, filename=None, args=(),
                 kwargs={}):
        super(KinesisProducer, self).__init__(name=name, group=group,
                                              args=args, kwargs=kwargs)
        self._pending_records = []
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.quiet = quiet
        self.producer_time = producer_time
        self.kinesis_connection = kinesis_connection
        self.logger = logging.getLogger(name)
        self.encryption_type = encryption_type
        self.max_try = max_try
        self._sample_record = json.dumps(default_record)
        self.total_records = 0

    def add_record(self, record):
        record_dict = {'Data': record,
                       # 'ExplicitHashKey': self.encryption_type,
                       'PartitionKey': self.partition_key}
        self._pending_records.append(record_dict)

    def put_record(self, record):
        response = self.kinesis_connection.put_record(stream_name=self.stream_name,
                                                      data=record,
                                                      partition_key=self.partition_key
                                                      )
                                                      # encryption_type=self.encryption_type)
        if not self.quiet:
            self.logger.info('+-> Put 1 Record.')


    def put_records(self, records):
        records_to_put = records
        successful_put_records = []
        retries = 0
        pprint.pprint(records_to_put[0])
        response = self.kinesis_connection.put_records(records=records_to_put,
                                                       stream_name=self.stream_name)
        while response['FailedRecordCount'] > 0 and retries < self.max_try:
            failed_records_to_retry = []
            response_records = response['Records']
            for idx in range(0, len(response_records)):
                record_to_put = records_to_put[idx]
                response_record = response_records[idx]
                if response_record.get('ErrorCode'):
                    failed_records_to_retry.append(record_to_put)
                else:
                    successful_put_records.append(response_record)
            records_to_put = failed_records_to_retry
            response = self.kinesis_connection.put_records(stream_name=self.stream_name,
                                                           records=records_to_put)
            retries += 1
        if self.quiet is False:
            print('-= put {0} records:'.format(len(successful_put_records)))
        return successful_put_records

    def put_all_records(self):
        records_to_put = self._pending_records
        self._pending_records = []
        put_records = self.put_records(records_to_put)
        self.total_records += len(put_records)
        return len(put_records)

    def work(self):
        for _ in range(500):
            self.add_record(self._sample_record)
        self.put_all_records()


    def run(self):
        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.producer_time)
        while finish > datetime.datetime.now():
            self.work()
            time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or attach to a Kinesis stream and put records in the stream''',
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument('stream_name',
                        help='''the name of the Kinesis stream to either connect with or create''')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='''the name of the Kinesis region to connect with [default: us-east-1]''')
    parser.add_argument('--shard_count', type=int, default=1,
                        help='''the number of shards to create in the stream, if creating [default: 1]''')
    parser.add_argument('--partition_key', default='KinesisDemo',
                        help='''the partition key to use when communicating records to the
                    stream [default: 'KinesisDemo-##']''')
    parser.add_argument('--producer_count', type=int, default=2,
                        help='''the number of producer threads [default: 2]''')
    parser.add_argument('--producer_time', type=int, default=30,
                        help='''how many seconds the producer threads should put records into
                    the stream [default: 30]''')
    parser.add_argument('--quiet', action='store_true', default=False,
                        help='''reduce console output to just initialization info''')
    parser.add_argument('--delete_stream', action='store_true', default=False,
                        help='''delete the Kinesis stream matching the given stream_name''')
    parser.add_argument('--describe_only', action='store_true', default=False,
                        help='''only describe the Kinesis stream matching the given stream_name''')

    producers = []
    args = parser.parse_args()
    kinesis = boto.kinesis.connect_to_region(region_name=args.region)
    if (args.delete_stream):
        kinesis.delete_stream(stream_name=args.stream_name)
    else:
        start_time = datetime.datetime.now()

        if args.describe_only is True:
            stream = kinesis.describe_stream(args.stream_name)
            print(json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
        else:
            stream = get_or_create_stream(kinesis, args.stream_name, args.shard_count)
            for pid in xrange(args.producer_count):
                producer_name = 'shard_producer:%s' % pid
                part_key = args.partition_key + '-' + str(pid)
                producer = KinesisProducer(stream_name=args.stream_name,
                                           partition_key=part_key,
                                           kinesis_connection=kinesis,
                                           producer_time=args.producer_time,
                                           name=producer_name,
                                           quiet=args.quiet)
                producer.daemon = True
                producers.append(producer)
                print('start: ', producer_name)
                producer.start()

            for producer in producers:
                producer.join()

        finish_time = datetime.datetime.now()
        duration = (finish_time - start_time).total_seconds()
        total_records = sum_posts(producers)
        print("-=> Exiting Producer Main <=-")
        print("  Total Records:", total_records)
        print("     Total Time:", duration)
        print("  Records / sec:", total_records / duration)
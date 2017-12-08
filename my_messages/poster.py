from __future__ import print_function

import boto
import argparse
import json
import threading
import time
import datetime

from argparse import RawTextHelpFormatter
import random
from boto.kinesis.exceptions import ResourceNotFoundException

data = [
        {'name': 'Arthur',
         'message': 'Arthur\'s default message'},
        {'name': 'Andy',
         'message': 'Andy\'s default message'},
        {'name': 'Min',
         'message': 'Min\'s default message'},
        ]

messages = ['hello', 'how are you?', 'goodbye', 'another message']


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

class KinesisPoster(threading.Thread):
    def __init__(self, stream_name, partition_key, kinesis, poster_time=30, quiet=False,
                 name=None, group=None, filename=None, args=(), kwargs={}):
        super(KinesisPoster, self).__init__(name=name, group=group,
                                            args=args, kwargs=kwargs)
        self._pending_records = []
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.quiet = quiet
        self.poster_time = poster_time
        self.total_records = 0
        self.kinesis = kinesis

    def add_random_record(self):
        random_message = messages[random.randint(0,3)]
        random_record = data[random.randint(0, 2)]
        random_record['message'] = random_message
        print('~> adding records:{0}'.format(random_record['name'], random_record['message']))
        self._pending_records.append(random_record)

    def put_all_records(self):
        precs = self._pending_records
        print('===> precs: {0}'.format(precs))
        self._pending_records = []
        self.put_records(precs)
        self.total_records += len(precs)
        return len(precs)

    def put_records(self, records):
        for record in records:
            json_record = json.dumps(record)
            response = self.kinesis.put_record(stream_name=self.stream_name,
                                               data=json_record,
                                               partition_key=self.partition_key)
            if self.quiet is False:
                print('-= put seqNum:', response['SequenceNumber'])

    def run(self):
        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.poster_time)
        while finish > datetime.datetime.now():
            for _ in range(4):
                self.add_random_record()
            self.put_all_records()
        if self.quiet is False:
            print(' Total Records Put:', self.total_records)


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
    parser.add_argument('--poster_count', type=int, default=2,
                        help='''the number of poster threads [default: 2]''')
    parser.add_argument('--poster_time', type=int, default=30,
                        help='''how many seconds the poster threads should put records into
                    the stream [default: 30]''')
    parser.add_argument('--quiet', action='store_true', default=False,
                        help='''reduce console output to just initialization info''')
    parser.add_argument('--delete_stream', action='store_true', default=False,
                        help='''delete the Kinesis stream matching the given stream_name''')
    parser.add_argument('--describe_only', action='store_true', default=False,
                        help='''only describe the Kinesis stream matching the given stream_name''')

    threads = []
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
            for pid in xrange(args.poster_count):
                poster_name = 'shard_poster:%s' % pid
                part_key = args.partition_key + '-' + str(pid)
                poster = KinesisPoster(stream_name=args.stream_name,
                                       partition_key=part_key,
                                       kinesis=kinesis,
                                       poster_time=args.poster_time,
                                       name=poster_name,
                                       quiet=args.quiet)
                poster.daemon = True
                threads.append(poster)
                print('start: ', poster_name)
                poster.start()

            for t in threads:
                t.join()

        finish_time = datetime.datetime.now()
        duration = (finish_time - start_time).total_seconds()
        total_records = sum_posts(threads)
        print("-=> Exiting Poster Main <=-")
        print("  Total Records:", total_records)
        print("     Total Time:", duration)
        print("  Records / sec:", total_records / duration)
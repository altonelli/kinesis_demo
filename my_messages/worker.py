from __future__ import print_function

import sys
import boto
import argparse
import json
import threading
import time
import datetime

from argparse import RawTextHelpFormatter
from boto.kinesis.exceptions import ProvisionedThroughputExceededException

from my_messages import poster

iter_type_at = 'AT_SEQUENCE_NUMBER'
iter_type_after = 'AFTER_SEQUENCE_NUMBER'
iter_type_trim = 'TRIM_HORIZON'
iter_type_latest = 'LATEST'

def echo_records(records):
    # print('===> records: {0}'.format(records))
    for record in records:
        msg_dict = json.loads(record['Data'])
        print('+--> {0} said \'{1}\''.format(msg_dict['name'], msg_dict['message']))

class KinesisWorker(threading.Thread):
    def __init__(self, stream_name, kinesis, shard_id, iterator_type, worker_time=40, sleep_interval=0.5,
                 name=None, group=None, echo=False, args=(), kwargs={}):
        super(KinesisWorker, self).__init__(name=name, group=group, args=args, kwargs=kwargs)
        self.stream_name=stream_name
        self.kinesis = kinesis
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.worker_time = worker_time
        self.sleep_interval = sleep_interval
        self.total_records = 0
        self.echo = echo

    def run(self):
        my_name = threading.current_thread().name
        print('+ KinesisWorker:', my_name)
        print('+-> working with iterator:', self.iterator_type)
        response = self.kinesis.get_shard_iterator(self.stream_name,
                                                   self.shard_id,
                                                   self.iterator_type)
        next_iterator = response['ShardIterator']
        print('+-> getting next records using iterator:', next_iterator)

        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.worker_time)
        while finish > datetime.datetime.now():
            try:
                response = self.kinesis.get_records(next_iterator, limit=25)
                self.total_records += len(response['Records'])

                if len(response['Records']) > 0:
                    print('\n+-> {1} Got {0} Worker Records'.format(
                        len(response['Records']), my_name))
                    if self.echo:
                        echo_records(response['Records'])
                else:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                next_iterator = response['NextShardIterator']
                time.sleep(self.sleep_interval)
            except ProvisionedThroughputExceededException as ptee:
                print(ptee.message)
                time.sleep(5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''Create or connect to a Kinesis stream and create workers
                       that read records''',
        formatter_class=RawTextHelpFormatter)
    parser.add_argument('stream_name',
                        help='''the name of the Kinesis stream to either create or connect''')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='''the name of the Kinesis region to connect with [default: us-east-1]''')
    parser.add_argument('--worker_time', type=int, default=30,
                        help='''the worker's duration of operation in seconds [default: 30]''')
    parser.add_argument('--sleep_interval', type=float, default=0.1,
                        help='''the worker's work loop sleep interval in seconds [default: 0.1]''')
    parser.add_argument('--echo', action='store_true', default=True,
                        help='''the worker should print formatted records to the console''')

    args = parser.parse_args()
    kinesis = boto.kinesis.connect_to_region(region_name=args.region)
    stream = kinesis.describe_stream(args.stream_name)
    print(json.dumps(stream, sort_keys=True, indent=2, separators=(',', ': ')))
    shards = stream['StreamDescription']['Shards']
    print('# Shard Count:', len(shards))

    threads = []
    start_time = datetime.datetime.now()
    for shard_id in xrange(len(shards)):
        worker_name = 'shard_workers:%s' % shard_id
        print('#-> shardId:', shards[shard_id]['ShardId'])
        worker = KinesisWorker(
            stream_name=args.stream_name,
            kinesis=kinesis,
            shard_id=shards[shard_id]['ShardId'],
            iterator_type=iter_type_trim,
            worker_time=args.worker_time,
            sleep_interval=args.sleep_interval,
            echo=args.echo,
            name=worker_name
        )
        worker.daemon = True
        threads.append(worker)
        print('#-> starting: ', worker_name)
        worker.start()

    for t in threads:
        t.join()

    finish_time = datetime.datetime.now()
    duration = (finish_time - start_time).total_seconds()
    total_records = poster.sum_posts(threads)
    print("\n")
    print("-=> Exiting Worker Main <=-")
    print("  Total Records:", total_records)
    print("     Total Time:", duration)
    print("  Records / sec:", total_records / duration)
    print("  Worker sleep interval:", args.sleep_interval)







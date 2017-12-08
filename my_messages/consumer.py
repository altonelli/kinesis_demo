from __future__ import print_function

import threading
import time
import datetime
import logging

from boto.kinesis.exceptions import ProvisionedThroughputExceededException


class KinesisConsumer(threading.Thread):
    def __init__(self, stream_name, kinesis_connection, shard_id, iterator_type, consumer_time=40,
                 sleep_interval=0.5, name=None, group=None, args=(), kwargs={}):
        super(KinesisConsumer, self).__init__(name=name, group=group, args=args, kwargs=kwargs)
        self.stream_name = stream_name
        self.kinesis_connection = kinesis_connection
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.consumer_time = consumer_time
        self.sleep_interval = sleep_interval
        self.logger = logging.getLogger(shard_id + ":" + self.name)


    def run(self):
        self.logger.info('+ KinesisConsumer: ' + self.name)
        self.logger.info('+-> working with iterator:' + self.iterator_type)
        shard_iter_res = self.kinesis_connection.get_shard_iterator(self.stream_name,
                                                      self.shard_id,
                                                      self.iterator_type)
        self.iterator = shard_iter_res['ShardIterator']
        self.logger.info('+-> getting next records using iterator: ' + self.iterator)
        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.consumer_time)
        while finish > datetime.datetime.now():
            self.work()

    def work(self):
        """
        Simple method that prints the records from the kinesis response.
        To be overridden in subclasses of KinesisConsumer
        """
        try:
            response = self.kinesis_connection.get_records(self.iterator, limit=25)
            if len(response['Records']) > 0:
                self.logger.info('\n+-> {1} Got {0} Consumer Records'.format(
                    len(response['Records']), self.thread_name))
                for record in response['Records']:
                    print(record['Data'])
            self.iterator = response['NextShardIterator']
            time.sleep(self.sleep_interval)
        except ProvisionedThroughputExceededException as ptee:
            self.logger.error(ptee.message)
            time.sleep(5)

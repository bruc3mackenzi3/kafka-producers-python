
'''
    Bidder Kafka Producer module

    Kafka is a data streaming framework used by the Bidder to stream captains logs
    to HDFS / Spark.
'''


from datetime import timedelta

from producer_config import ProducerConfig
import confluent_kafka
import requests
import tornado.ioloop
import tornado

import avro_serializer

INITIAL_TIMEOUT = timedelta(milliseconds=10)
MAX_TIMEOUT = timedelta(seconds=1)


class KafkaProducer:

    def __init__(self, logger, config_url, client_id):
        self.logger = logger
        self.config_url = config_url
        self.client_id = client_id

        self.fatal_errors = [confluent_kafka.KafkaError._ALL_BROKERS_DOWN]

        # Config init
        self._init_config()

        # Initialize Avro serializers
        self._init_serializers(True)

        # Initialize producer
        self.producer = confluent_kafka.Producer(**self.prod_config.confluent_kafka_config)

        # Add callback wrappers around producer to Tornado IOLoop
        io_loop = tornado.ioloop.IOLoop.instance()
        io_loop.add_callback(self.poll)

        # Add schema update to Tornado Periodic Callback
        self.update_interval = 1*60*1000
        tornado_callback = tornado.ioloop.PeriodicCallback(
                    self.load_data,
                    self.update_interval,
                    io_loop=io_loop)
        tornado_callback.start()

        self.logger.log('INFO', 'Successfully Initialized Kafka Producer')

    def _init_config(self):
        self.prod_config = ProducerConfig.get_instance(self.config_url)

        # Load Kafka config
        self.prod_config.confluent_kafka_config['client.id'] = self.client_id
        self.prod_config.confluent_kafka_config['on_delivery'] = self._on_delivery_callback
        self.prod_config.confluent_kafka_config['error_cb'] = self._error_callback

    def _init_serializers(self, bootup):
        # Initialize schema config
        # There are two different ways in which this function behaves:
        # During Bidder bootup if initialization fails Tornado will catch the
        # exception and retry.
        # If a serializer init fails on a subsequent call in the periodic
        # callback the old serializer is used.  This can cause a delay when
        # pushing schema updates.

        self.serializers = {}

        for schema in self.prod_config.topics:
            try:
                new_serializer = avro_serializer.AvroMessageSerializer(
                                self.prod_config.topics[schema]['schema_subject'],
                                self.prod_config.schema_registry_url)
            except requests.ConnectionError as e:
                self.logger.log('CRIT', 'ConnectionError raised loading AvroMessageSerializer for live schema config update.  Schema subject: {}, error: {}'.format(schema, e))
                if bootup:
                    raise e
            else:
                # successfully reloaded schema
                self.serializers[schema] = new_serializer

    def load_data(self):
        '''
        Reloads the schema.  Uses the load_data function name to follow the same
        convention as all periodic data.
        '''

        self._init_serializers(False)

    def produce_record(self, log_type, record_dict, record_json_str):
        '''
        Called per request processed, attempt to serialize the record and produce
        to the Avro topic.  If serialization fails the record_json_str is
        produced to the raw topic.
        '''

        if self.prod_config.global_stop:
            if self.logger.isEnabledFor('WARN'):
                self.logger.log('WARN', 'Kafka global_stop enabled, not producing message')
            return

        # First attempt to serialize record
        serialized_record = None
        try:
            serialized_record = self.serializers[log_type].kafka_avro_encode(record_dict)
            self.logger.log('INFO', 'Successfully serialized record with subject ' + self.serializers[log_type].schema_subject)
        except (Exception) as e:
            self.logger.log('WARN', '{} while serializing kafka topic {}: {}'.format(type(e).__name__, log_type, e))

        # If serialization successful produce to Avro topic
        if serialized_record:
            self.producer.produce(self.prod_config.topics[log_type]['avro_topic'], serialized_record, callback=self._on_delivery_callback)
            if self.logger.isEnabledFor('INFO'):
                self.logger.log('INFO', 'Produced serialized avro record, Topic: {} Record: {}'.format(self.prod_config.topics[log_type]['avro_topic'], record_json_str))
        # If serialization failed use raw JSON string and produce to raw
        # topic
        else:
            self.producer.produce(self.prod_config.topics[log_type]['raw_topic'], record_json_str, callback=self._on_delivery_callback)
            if self.logger.isEnabledFor('INFO'):
                self.logger.log('INFO', 'Produced raw JSON record, Topic: {} Record: {}'.format(self.prod_config.topics[log_type]['raw_topic'], record_json_str))

    @tornado.gen.coroutine
    def poll(self):
        '''
        Periodically call Kafka poll() method to produce data.  Implemented as
        infinite coroutine for draining the delivery report queue (internal to
        Kafka), with exponential backoff.
        '''

        retry_timeout = INITIAL_TIMEOUT
        while True:
            num_processed = self.producer.poll(0)
            if num_processed > 0:
                retry_timeout = INITIAL_TIMEOUT
            else:
                # Retry with exponential backoff
                yield tornado.gen.sleep(retry_timeout.total_seconds())
                retry_timeout = min(retry_timeout*2, MAX_TIMEOUT)

    def poll_synchronous(self):
        ''' Synchronous version of poll which simply calls Kafka's poll().
        '''

        self.producer.poll(0)

    def _on_delivery_callback(self, error, msg_metadata):
        '''
        Called once for each produced message to indicate the final delivery
        result, either when the message has been succesfully produced or on
        final error (after retries).

        To enable this callback on non-errors set
        delivery.report.only.error to False in the config.
        '''

        if error:
            if '-raw' in msg_metadata.topic():
                log_stmt = 'Error producing Kafka message.  Error: {}, topic: {}, message: {}'.format(error, msg_metadata.topic(), msg_metadata.value())
            else:
                log_stmt = 'Error producing Kafka message.  Error: {}, topic: {}, partition: {}, offset: {}, ts: {}'.format(error, msg_metadata.topic(), msg_metadata.partition(), msg_metadata.offset(), msg_metadata.timestamp())
            self.logger.log('CRIT', log_stmt)
        else:
            self.logger.log('INFO', 'Kafka callback: {} message delivered to server'.format(msg_metadata.topic()))

    def _error_callback(self, error):
        '''
        Callback for generic/global error events. This callback is served by
        poll().
        '''

        if error.code() in self.fatal_errors:
            self.prod_config.global_stop = True
            self.logger.log('CRIT', 'Detected fatal Kafka error {}, disabling all message producing.'.format(error.name()))
        else:
            self.logger.log('CRIT', 'Error in Kafka producer: {}, str: {}'.format(error.name(), error))

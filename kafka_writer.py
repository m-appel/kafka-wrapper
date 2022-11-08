import logging

import msgpack
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


class KafkaWriter:
    """Generic writer to write to Kafka topics.

    This writer creates the topic with the specified configuration
    if it does not already exist.

    Messages are compressed with msgpack.
    """
    def __init__(self,
                 topic: str,
                 bootstrap_servers: str,
                 num_partitions: int = 10,
                 replication_factor: int = 2,
                 config: dict = None):
        if config is None:
            # 30 days.
            config = {'retention.ms': 2592000000}
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config
        self.timeout_in_s = 60

    def __enter__(self):
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'default.topic.config': {
                'compression.codec': 'snappy'
            }
        })
        self.__prepare_topic()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush(self.timeout_in_s)

    @staticmethod
    def __delivery_report(err, msg):
        if err is not None:
            logging.error(f'Message delivery failed: {err}')
        else:
            pass

    def __prepare_topic(self) -> None:
        """Try to create the specified topic on the Kafka servers.
        Output a warning if the topic already exists.
        """
        admin_client = AdminClient(
            {'bootstrap.servers': self.bootstrap_servers})
        topic_list = [NewTopic(self.topic,
                               num_partitions=self.num_partitions,
                               replication_factor=self.replication_factor,
                               config=self.config)]
        created_topic = admin_client.create_topics(topic_list)
        for topic, f in created_topic.items():
            try:
                f.result()  # The result itself is None
                logging.warning(f'Topic {topic} created')
            except Exception as e:
                logging.warning(f'Failed to create topic {topic}: {e}')

    def write(self, key, data, timestamp: int) -> None:
        """Write the data to the topic with the specified key and
        timestamp.

        Data will be compressed with msgpack.
        """
        try:
            self.producer.produce(
                self.topic,
                msgpack.packb(data),
                key,
                callback=self.__delivery_report,
                timestamp=timestamp
            )
            self.producer.poll(0)

        except BufferError:
            logging.warning('Buffer error, the queue must be full! '
                            'Flushing...')
            self.producer.flush()

            logging.info('Queue flushed, try re-write previous message')
            self.producer.produce(
                self.topic,
                msgpack.packb(data),
                key,
                callback=self.__delivery_report,
                timestamp=timestamp
            )
            self.producer.poll(0)

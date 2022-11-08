# `kafka-wrapper`: A generic Kafka reader and writer

The purpose of this repository is to provide two simple wrapper classes for
reading from and writing to Kafka topics, based on the
[confluent_kafka](https://pypi.org/project/confluent-kafka/) Python package. In
addition, [msgpack](https://pypi.org/project/msgpack/) is used for data
compression.

## Installation

Make sure the required Python dependencies are installed:
```
pip install -r requirements.txt
```

## Usage

Import the desired class, create an object and use within a `with` statement.
Usage as a context manager is required, since the initialization of the
producer/consumer happens in the `__enter__` function.

### Reader
```python
from kafka_reader import KafkaReader

reader = KafkaReader(topic_list, bootstrap_servers)
with reader:
    for msg in reader.read():
        # Process message.
```

### Writer
```python
from kafka_writer import KafkaWriter

writer = KafkaWriter(topic, bootstrap_servers)
with writer:
    writer.write(key, data, timestamp)
```

For advanced usage, check the source code.
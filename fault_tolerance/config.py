"""
Some configurations to change default ports
1) Starting redis on different port:
redis-server --port 6389

2) Changing Kafka's default port:
Add following in config/server.properties:
listeners=PLAINTEXT://:9092

3) Changing Zookeeper's default port:
    a) Change the following number in config/zookeeper.properties
    clientPort=2181
    b) Change the following number in config/server.properties
    zookeeper.connect=localhost:2181

Create a new kafka topic "numbers" with 4 partitions
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic numbers --partitions 4

Check the description of a Kafka topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic numbers
"""

DEFAULT_REDIS_PORT = 6389
DEFAULT_KAFKA_PORT = 9092
DEFAULT_TOPIC = 'numbers_12'
DEFAULT_GROUP_ID = 'numbers_group_subscribers'
DEFAULT_NUM_RANGE = 10_000
DEFAULT_PRODUCER_SLEEP_INTERVAL = 3  # seconds
CONSUMER_IDs = ["first", "second", "third", "fourth", "when_subscribed", "all"]

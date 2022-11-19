import argparse
import pickle
from distutils.util import strtobool
from typing import List

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from redis import Redis

from fault_tolerance import config

"""
Create topic before subscribing:
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <topic> --partitions 4

Script to start consumer.
There are supposed to be three different types of consumers (consumer groups):
1) A consumer group with 4 possible consumers
-  python -m fault_tolerance.consumer -c first -d y 
-  python -m fault_tolerance.consumer -c second
-  python -m fault_tolerance.consumer -c third
-  python -m fault_tolerance.consumer -c fourth

Warning: if starting the first consumer, don't pass -d flag, otherwise, it will clean up redis cache

2) A consumer group with just one consumer and listens from latest updates (by default)
-  python -m fault_tolerance.consumer -g g1 -c when_subscribed

3) A consumer group with just one consumer and listens from the beginning
-  python -m fault_tolerance.consumer -c all -g g2 -s y
"""


def main(args):
    # setting up redis client
    redis_client = Redis(host='localhost', port=args.redis_port, db=0)
    if args.delete:
        redis_client.flushall()
        print(f'cleaned up the cache before subscribing to the Kafka consumer')

    kafka_consumer = KafkaConsumer(args.topic, group_id=args.group_id, value_deserializer=pickle.loads)
    if args.seek:
        kafka_consumer.poll()
        kafka_consumer.seek_to_beginning()
        print(f'Sought to the beginning for consumer_id: [{args.consumer_id}], group id: [{args.group_id}]')

    msg: ConsumerRecord  # only for typehints
    for msg in kafka_consumer:
        received_value: int = msg.value

        # fetching from redis and publishing the updated values
        redis_value: bytes = redis_client.get(args.consumer_id)
        if redis_value:
            parsed_values: List[int] = pickle.loads(redis_value)
        else:
            parsed_values = []
        parsed_values.append(received_value)
        redis_client.set(args.consumer_id, pickle.dumps(parsed_values))
        print(f'Latest published values: key: [{args.consumer_id}], values: [{parsed_values}]')


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--consumer_id', type=str, help='Consumer id of this consumer')
    arg_parser.add_argument('-t', '--topic', type=str, default=config.DEFAULT_TOPIC, help='Topic to subscribe to')
    arg_parser.add_argument('-g', '--group_id', type=str, default=config.DEFAULT_GROUP_ID, help='Group Id of consumers')
    arg_parser.add_argument('-rp', '--redis_port', type=int, default=config.DEFAULT_REDIS_PORT, help='Redis port')
    arg_parser.add_argument('-d', '--delete', type=lambda x: strtobool(x.strip()), default=False, help='Clean cache')
    arg_parser.add_argument('-s', '--seek', type=lambda x: strtobool(x.strip()), default=False, help='Should seek?')
    parsed_args = arg_parser.parse_args()

    # sanity check on consumer id
    if parsed_args.consumer_id not in config.CONSUMER_IDs:
        raise ValueError(f'Invalid consumer id: [{parsed_args.consumer_id}], '
                         f'allowed consumer ids: {config.CONSUMER_IDs}')

    main(parsed_args)

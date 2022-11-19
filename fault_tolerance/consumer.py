import argparse
import pickle
from typing import List

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from redis import Redis

from fault_tolerance import config
from distutils.util import strtobool


def main(args):
    # setting up redis client
    redis_client = Redis(host='localhost', port=args.redis_port, db=0)
    if args.delete:
        redis_client.flushall()
        print(f'cleaned up the cache before subscribing to the Kafka consumer')

    kafka_consumer = KafkaConsumer(args.topic, group_id=args.group_id, value_deserializer=pickle.loads)

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
    parsed_args = arg_parser.parse_args()

    # sanity check on consumer id
    if parsed_args.consumer_id not in config.CONSUMER_IDs:
        raise ValueError(f'Invalid consumer id: [{parsed_args.consumer_id}], '
                         f'allowed consumer ids: {config.CONSUMER_IDs}')

    main(parsed_args)

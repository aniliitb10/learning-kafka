import argparse
import pickle
from time import sleep

from kafka import KafkaProducer

from fault_tolerance import config

"""
A good idea to create topic before producing numbers
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <topic> --partitions 4
"""


def main(args):
    # setting up kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=pickle.dumps)
    for i in range(args.number_range):
        kafka_producer.send(topic=args.topic, value=i + 1).get(timeout=60)
        print(f'Published {i + 1} on kafka')
        sleep(args.producer_sleep_interval)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-n', '--number_range', type=int, default=config.DEFAULT_NUM_RANGE,
                            help='The numbers to publish')
    arg_parser.add_argument('-t', '--topic', type=str, default=config.DEFAULT_TOPIC, help='Topic to publish to')
    arg_parser.add_argument('-s', '--producer_sleep_interval', type=str, default=config.DEFAULT_PRODUCER_SLEEP_INTERVAL,
                            help='Time interval between two consecutive publishes')
    parsed_args = arg_parser.parse_args()
    main(parsed_args)

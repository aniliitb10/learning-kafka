import pickle

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from pymemcache.client import base
from market_data import MarketData
from pymemcache import serde
from typing import Optional


if __name__ == '__main__':
    consumer = KafkaConsumer('my_new_topic', value_deserializer=pickle.loads)

    # Setting up memcache client
    client = base.Client(('localhost', 11211), serde=serde.pickle_serde)

    # Now, consuming the messages and publishing on memcache
    msg: ConsumerRecord  # this line is only used for type hint
    for msg in consumer:
        print(f'Message received by consumer: {msg.value}')
        msg_data: MarketData = msg.value
        prev_quantity: Optional[str] = client.get(msg_data.ticker)  # either None or Byte string
        new_quantity = (int(prev_quantity) if prev_quantity is not None else 0) + msg_data.quantity
        client.set(msg_data.ticker, new_quantity)
        print(f'Published on memcache: ticker: {msg_data}, total quantity: {new_quantity}')

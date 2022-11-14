import pickle

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from pymemcache.client import base
from market_data import MarketData
from pymemcache import serde


if __name__ == '__main__':
    consumer = KafkaConsumer('my_new_topic', key_deserializer=lambda key: key.decode('utf-8'),
                             value_deserializer=pickle.loads)

    client = base.Client(('localhost', 11211), serde=serde.pickle_serde)
    msg: ConsumerRecord  # this line is only used for type hint
    for msg in consumer:
        print(f'Message received by consumer: {msg.value}, partition: {msg.partition}')
        msg_data: MarketData = msg.value
        prev_quantity = client.get(msg_data.ticker)
        new_quantity = (int(prev_quantity) if prev_quantity is not None else 0) + msg_data.quantity
        client.set(msg_data.ticker, new_quantity)
        print(f'published: ticker: {msg_data.ticker}:{new_quantity}')

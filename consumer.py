import pickle
from typing import Optional, List, Tuple

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from pymemcache.client import base

import config
from market_data import MarketData
from datetime import datetime, time


if __name__ == '__main__':
    consumer = KafkaConsumer('my_new_topic', value_deserializer=pickle.loads)

    # Setting up memcache client
    client = base.Client(('localhost', 11211))
    client.flush_all()
    begin_time = datetime.now()
    # client.delete_many(config.TICKERS)  # cleaning it up

    # Now, consuming the messages and publishing on memcache
    msg: ConsumerRecord  # this line is only used for type hint
    for msg in consumer:
        print(f'Message received by consumer: {msg.value}')
        # publishing quantity
        msg_data: MarketData = msg.value
        prev_quantity: Optional[str] = client.get(msg_data.ticker)  # either None or Byte string
        new_quantity = (int(prev_quantity) if prev_quantity is not None else 0) + msg_data.quantity
        client.set(msg_data.ticker, new_quantity)

        # Publishing ltp data
        ltp_key = f"{msg_data.ticker}.ltp"
        ltp_data = client.get(ltp_key)
        if ltp_data:
            parsed_ltp_data: Tuple[List[float], List[int]] = pickle.loads(ltp_data)
        else:
            parsed_ltp_data = [], []
        parsed_ltp_data[0].append(msg_data.ltp)
        parsed_ltp_data[1].append(int((msg_data.timestamp - begin_time).total_seconds()))
        client.set(ltp_key, pickle.dumps(parsed_ltp_data))
        print(f'Published on memcache: ticker: {msg_data}, total quantity: {new_quantity}, ltp data: {parsed_ltp_data}')

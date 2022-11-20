import pickle
from datetime import datetime
from typing import Optional, List, Tuple

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from redis import Redis

import config
from market_data import MarketData


def publish_notional(msg_data: MarketData, redis_client: Redis) -> float:
    """
    To publish total traded notional
    """
    notional_key = f'{msg_data.ticker}{config.NOTIONAL_KEY}'
    prev_notional: Optional[bytes] = redis_client.get(notional_key)  # either None or Byte string
    new_notional: float = (float(prev_notional) if prev_notional else 0) + msg_data.quantity * msg_data.ltp
    redis_client.set(notional_key, new_notional)
    return new_notional


def publish_qty(msg_data: MarketData, redis_client: Redis) -> int:
    """
    To publish total traded quantity
    """
    prev_quantity: Optional[bytes] = redis_client.get(msg_data.ticker)  # either None or Byte string
    new_quantity: int = (int(prev_quantity) if prev_quantity else 0) + msg_data.quantity
    redis_client.set(msg_data.ticker, new_quantity)
    return new_quantity


def publish_ltp(msg_data: MarketData, redis_client: Redis, begin_time: datetime) -> Tuple[List[float], List[int]]:
    """
    To publish latest ltp in addition to the existing ltp data
    """
    ltp_key = f"{msg_data.ticker}{config.LTP_KEY}"
    ltp_data = redis_client.get(ltp_key)
    if ltp_data:
        parsed_ltp_data: Tuple[List[float], List[int]] = pickle.loads(ltp_data)
    else:
        parsed_ltp_data = [], []

    age: int = int((msg_data.timestamp - begin_time).total_seconds())
    if age < 0:
        raise ValueError(f'Market data age is -ve, was consumer started late?')
    parsed_ltp_data[0].append(msg_data.ltp)
    parsed_ltp_data[1].append(age)
    redis_client.set(ltp_key, pickle.dumps(parsed_ltp_data))
    return parsed_ltp_data


def main():
    consumer = KafkaConsumer(config.DEFAULT_TOPIC, bootstrap_servers=[f'localhost:{config.DEFAULT_KAFKA_PORT}'],
                             value_deserializer=pickle.loads)

    # Setting up memcache redis_client
    redis_client = Redis(host='localhost', port=config.DEFAULT_REDIS_PORT, db=0)
    redis_client.flushall()
    begin_time = datetime.now()

    # Now, consuming the messages and publishing on memcache
    msg: ConsumerRecord  # this line is only used for type hint
    for msg in consumer:
        msg_data: MarketData = msg.value
        print(f'Message received by consumer: {msg_data}')

        # Publishing total quantity, latest ltp and total notional
        total_quantity = publish_qty(msg_data, redis_client)
        ltp_data = publish_ltp(msg_data, redis_client, begin_time)
        new_notional = publish_notional(msg_data, redis_client)

        print(f'Published on redis: ticker: {msg_data}, total quantity: {total_quantity}, ltp data: {ltp_data}, '
              f'total notional: {new_notional}')


if __name__ == '__main__':
    main()

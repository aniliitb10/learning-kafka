import pickle
from datetime import datetime
from time import sleep

from kafka import KafkaProducer

from market_data import MarketData

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=lambda key: key.encode('utf-8'),
                             value_serializer=pickle.dumps)
    for i in range(1000):
        md = MarketData(ticker="SBI", ltp=100 + 0.1 * i, quantity=i + 1, timestamp=datetime.now())
        future = producer.send('my_new_topic', value=md, key=md.ticker)
        result = future.get(timeout=60)
        print(f'This is after successfully sending the message: {md}')
        sleep(2)
    print('And producer is done')

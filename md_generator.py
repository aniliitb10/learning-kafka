import pickle
from datetime import datetime
from random import uniform, randrange
from time import sleep
from typing import List, Dict

from kafka.producer import KafkaProducer

import config
from market_data import MarketData


class MDGenerator:
    """ A class to randomly generate MarketData for tickers"""

    def __init__(self, tickers: List[str], prices: Dict[str, float]):
        self.tickers: List[str] = tickers
        self.prices: Dict[str, float] = prices
        self.price_dev_threshold = 0.0005  # within 0.05%
        self.quantity_min_max_range = (10, 25)

        # following attributes just help the quantity and price rise over time to make updates look live
        self.quantity_up = 2
        self.price_up = 0.001  # 0.1 %
        self.counter = 1

    def generate_md(self) -> MarketData:
        """ To randomly generate MD for a randomly chosen ticker"""
        ticker_index = randrange(0, len(self.tickers))  # randomly choose an index
        ticker = self.tickers[ticker_index]
        configured_price = self.prices[ticker]
        price_delta = configured_price * self.price_dev_threshold
        price = uniform(configured_price - price_delta, configured_price + price_delta)  # select a random float
        quantity = randrange(*self.quantity_min_max_range)  # select a random int

        # values almost finalized, let's smoothly increase the price and quantity
        price += price * self.price_up * self.counter
        quantity += self.quantity_up * self.counter
        self.counter += 1

        return MarketData(ticker=ticker, ltp=price, quantity=quantity, timestamp=datetime.now())


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=pickle.dumps)

    md_generator = MDGenerator(tickers=config.TICKERS, prices=config.TICKERS_PRICES)
    for i in range(0, 100000):
        md = md_generator.generate_md()
        future = producer.send('my_new_topic', value=md)
        result = future.get(timeout=60)
        print(f'#{i:6d}: Published MD: {md}')
        sleep(1)  # pause for 1 seconds before publishing the next one

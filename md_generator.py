import argparse
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

    def __init__(self, tickers: List[str], prices: Dict[str, float], quantity_up_rate: int):
        self.tickers: List[str] = list(ticker.upper() for ticker in tickers)
        self.prices: Dict[str, float] = prices
        self.price_dev_threshold = 0.005  # within 0.5%
        self.quantity_min_max_range = (10, 25)

        # sanity check: ticker must have a configured price
        for ticker in self.tickers:
            if ticker not in self.prices:
                raise ValueError(f'There is no price configured for [{ticker}]\n'
                                 f'Following are the tickers with configured prices: {[t for t in self.prices.keys()]}')

        # following attributes just help the quantity and price rise over time to make updates look live
        self.quantity_up_rate = quantity_up_rate
        self.price_up = 0.0005  # 0.05 %
        self.counter = 1

        print(f'Initialized publisher with tickers: {self.tickers}, quantity_up_rate: {self.quantity_up_rate}')

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
        quantity += self.quantity_up_rate * self.counter
        self.counter += 1

        return MarketData(ticker=ticker, ltp=price, quantity=quantity, timestamp=datetime.now())


def main(args):
    # setting up the producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=pickle.dumps)

    md_generator = MDGenerator(tickers=args.tickers, prices=config.TICKERS_PRICES,
                               quantity_up_rate=args.quantity_up_rate)

    for i in range(0, 100000):
        md = md_generator.generate_md()
        future = producer.send('my_new_topic', value=md)
        result = future.get(timeout=60)  # guarantees delivery
        print(f'#{i:6d}: Published MD: {md}')
        sleep(1)  # pause for 1 seconds before publishing the next one


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-t', '--tickers', nargs='+', default=config.TICKERS)
    arg_parser.add_argument('-qr', '--quantity_up_rate', type=int, default=1)
    parsed_args = arg_parser.parse_args()

    main(parsed_args)

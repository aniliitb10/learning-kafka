from typing import List, Dict

TICKERS: List[str] = ["SBIN", "RELIANCE", "ADANIENT", "ICICIBANK", "TATASTEEL", "TCS", "HDFCBANK", "ZOMATO", "PAYTM",
                      "HDFCLIFE"]
TICKERS_PRICES: Dict[str, float] = {"SBIN": 600, "RELIANCE": 2630, "ADANIENT": 4006, "ICICIBANK": 907,
                                    "TATASTEEL": 108, "TCS": 3332, "HDFCBANK": 1622, "ZOMATO": 70, "PAYTM": 624,
                                    "HDFCLIFE": 526}

DEFAULT_REDIS_PORT = 6389
DEFAULT_KAFKA_PORT = 9092
DEFAULT_TOPIC = 'ltp'
LTP_KEY = '.ltp'
NOTIONAL_KEY = '.notional'
MIN_QUANTITY = 1000
MAX_QUANTITY = 1020
PRICE_RANGE = 0.005  # within 0.5%
PRICE_UP_RATE = 0.0005  # 0.05 %
QUANTITY_UP_RATE = 1

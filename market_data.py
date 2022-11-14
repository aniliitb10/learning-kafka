from datetime import datetime


class MarketData:
    """
    As per the current usage, this should have been a dataclass but let's keep it simple
    """
    def __init__(self, ticker: str, ltp: float, quantity: int, timestamp: datetime):
        self.ticker: str = ticker
        self.ltp: float = ltp
        self.quantity: int = quantity
        self.timestamp = timestamp

    def __repr__(self):
        return f'MarketData(ticker: {self.ticker}, ltp: {self.ltp:.4f}, quantity: {self.quantity}, ' \
               f'timestamp: {self.timestamp})'

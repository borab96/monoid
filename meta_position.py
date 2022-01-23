from dataclasses import dataclass, field
import pandas as pd
import logging
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce, singledispatch
import pandas_datareader.data as web
logging.basicConfig(filename='trade.log', encoding='utf-8', level=logging.INFO)
from typing import List

pdts = pd.Timestamp
pd.set_option("display.max_columns", 10)

def check_date(func):
    def wrapper(self, *args):
        if not pdts(args[0]).dayofweek<5: raise Exception(f'{args[0]} is a weekend')
        res = func(self, *args)
        return res
    return wrapper

@dataclass()
class Position:
    ticker_symbol: str
    init_date: str
    shares: float
    trades: None = None
    roll_date : None = None
    exit_date: None = None
    exit_value: None = None
    id: int = field(default=-1, repr=True)
    long_only: bool = True

    def __post_init__(self):
        if not pdts(self.init_date).dayofweek < 5:
            raise Exception(f'{self.init_date} is a weekend')
        # self.trades = {'date': [], 'price': [], 'shares': [], 'position': []}
        self.today = pd.Timestamp.today().floor('D')

    @check_date
    def exit(self, exit_date):
        self.exit_date = exit_date

    @property
    def is_open(self):
        if self.shares == 0:
            return False
        else:
            return True

    def __repr__(self):
        if self.is_open:
            return f"Position(Ticker: {self.ticker_symbol}, Shares: {self.shares}, Date: {self.init_date})"
        else:
            return f"Position(Ticker: {self.ticker_symbol}, ExitDate: {self.init_date})"

    def __str__(self):
        return self.__repr__()

    def __bool__(self):
        return self.is_open

    def __add__(self, pos):
        if self.is_open and pos.is_open:
            if self.ticker_symbol == pos.ticker_symbol:
                # self.exit(pos.init_date)
                return Position(ticker_symbol=self.ticker_symbol, init_date=self.init_date, roll_date=pos.init_date, shares=self.shares+pos.shares)
            else:
                return Portfolio(
                    self,
                    Position(ticker_symbol=pos.ticker_symbol, init_date=pos.init_date, shares=pos.shares))

    def __sub__(self, pos):
        if pos.shares == self.shares:
            self.exit(pos.init_date)
        pos.shares *= -1
        return self.__add__(pos)

    def __hash__(self):
        self.id = hash((self.ticker_symbol, self.init_date))
        return self.id



class Portfolio:
    def __init__(self, *positions_: Position):
        self.positions = positions_
        self.invested = 0
        _portfolio = []
        symbol_list = []
        for pos in positions_:
            if pos.ticker_symbol in symbol_list:
                _portfolio.append((self.positions[pos.ticker_symbol]+pos, pos.shares))
            else:
                _portfolio.append((pos, pos.shares))
            self.invested += _portfolio[-1][1]
            symbol_list.append(pos.ticker_symbol)
        self.portfolio = {p[0].ticker_symbol: (p[0].shares, p[0].shares / self.invested, p) for p in _portfolio}

    def get_vectorized(self):
        out = np.array([(p[0].shares, p[1]) for p in self])
        return out

    def __len__(self):
        return len(self.positions)

    def __getitem__(self, idx):
        try:
            out = self.portfolio[idx]
        except KeyError:
            out = (list(self.portfolio.values())[idx][0], list(self.portfolio.values())[idx][1], list(self.portfolio.values())[idx][2])
        return out

    def __str__(self):
        return f"({self.positions.values()}, {self.weights.values()})"



# p = Portfolio(Position("SPY", "2021-08-10", 380), Position("QQQ", "2021-08-10", 380))
# print(p['SPY'])
# print(p.get_vectorized())
# print(p)
# print(Position("SPY", "2021-08-10", 380)+Position("SPY", "2021-08-12", 380)+Position("SPY", "2021-08-13", 380))
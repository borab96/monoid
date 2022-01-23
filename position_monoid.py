from dataclasses import dataclass, field
import pandas as pd
import logging
from collections import namedtuple, OrderedDict
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

def log_positions(pd_series):
    def logger(func):
        def wrapper(*args):
            pos = args[1]
            if pos.roll_date:
                pd_series[pos.roll_date] = pos.shares
            else:
                pd_series[pos.init_date] = pos.shares
            return func(*args)
        return wrapper
    return logger

def time_order_check(func):
    def wrapper(arg1, arg2, *rest):
        # if arg1 is None or arg2 is None:
        #     print("Position was closed")
        #     return None
        if arg1.roll_date==0:
            date1 = arg1.init_date
        else:
            date1 = arg1.roll_date
        if arg2.roll_date == 0:
            date2 = arg2.init_date
        else:
            date2 = arg2.roll_date
        print(date1, date2)
        if pdts(date1) > pdts(date2):
            raise Exception(f"The base date is {arg1.init_date}, this cannot be after"
                            f" the provided roll_date {arg2.roll_date}")
        else:
            return func(arg1, arg2, *rest)
    return wrapper

class Monoid:
    def __init__(self, null, type_, binary_op):
        self.null = null
        self.type = type_
        self.binary_op = binary_op

    def __call__(self, *args):
        _ = self.null
        for arg in args:
            arg = self.type(arg)
            _ = self.binary_op(_, arg)
        return _

@dataclass
class Position:
    ticker_symbol: str
    init_date: str or None
    shares: float
    roll_date: str or int = 0
    exit_date: None = None
    id: int = field(default=-1, repr=True)
    is_open: bool = True

    def __post_init__(self):
        if not pdts(self.init_date).dayofweek < 5:
            raise Exception(f'{self.init_date} is a weekend')
        self.today = pd.Timestamp.today().floor('D')
        if self.roll_date and not self.shares: # This is a closing position
            self.exit_date = self.roll_date
            self.is_open = False

    @check_date
    def exit(self, exit_date):
        self.exit_date = exit_date

    def __repr__(self):
        # if self.is_open:
        return f"Position(Ticker: {self.ticker_symbol}, Shares: {self.shares}, " \
                   f"InitDate: {self.init_date}, RollDate: {self.roll_date})"
        # else:
        #     return f"Position(Ticker: {self.ticker_symbol}, ExitDate: {self.init_date})"

    def __str__(self):
        return self.__repr__()

    def __bool__(self):
        return self.is_open

    # def __add__(self, pos):
    #     if self.is_open and pos.is_open:
    #         if self.ticker_symbol == pos.ticker_symbol:
    #             # self.exit(pos.init_date)
    #             return Position(ticker_symbol=self.ticker_symbol, init_date=self.init_date, roll_date=pos.init_date, shares=self.shares+pos.shares)
    #         else:
    #             return Portfolio(
    #                 self,
    #                 Position(ticker_symbol=pos.ticker_symbol, init_date=pos.init_date, shares=pos.shares))
    #
    # def __sub__(self, pos):
    #     if pos.shares == self.shares:
    #         self.exit(pos.init_date)
    #     pos.shares *= -1
    #     return self.__add__(pos)

    def __hash__(self):
        self.id = hash((self.ticker_symbol, self.init_date))
        return self.id




@singledispatch
def null(a):
    pass

@singledispatch
def update(a, b):
    pass

@null.register(Position)
def _(a):
    a.shares = 0
    return a
series = pd.Series(index=pd.date_range(start='2020-02-05', end='2020-02-29'), dtype=float)
@log_positions(pd_series=series)
def add(a, b):
        return Position(ticker_symbol=a.ticker_symbol, init_date=a.init_date, roll_date=b.roll_date, shares=a.shares+b.shares)

update = namedtuple('update', 'shares roll_date')
lift = lambda x:  Position('SPY', '2020-02-05', *x )


position_monoid = Monoid(Position("SPY", '2020-02-05', 0), lift, add)
# for update in [(2, "2020-02-08"), (-2, "2020-02-10")]:
#     print(position_monoid(update).shares)
position_monoid((1, "2020-02-05"), (-3, "2020-02-06"), (2, "2020-02-10"), (-6, "2020-02-14"), (3, "2020-02-24"))

import matplotlib.pyplot as plt
import numpy as np
plt.figure()
plt.stem(series.index, series)
plt.xticks(rotation=-45)
plt.ylabel("Trade signals")
plt.tight_layout()
plt.savefig("sample_signals.png")
plt.show()

import pandas_datareader.data as web
hist = web.DataReader("SPY", 'yahoo','2020-02-05', '2020-02-29')
hist['LogRet'] = np.log(hist['Adj Close']).diff()
hist['Position'] = series
hist['CumsumPosition'] = np.cumsum(hist['Position'])
hist['CumsumPosition'].fillna(inplace=True, method='ffill')
print(hist.head())
hist['position_return'] = hist['CumsumPosition'].shift(1) * hist['LogRet']
hist['position_value'] = hist['position_return'].cumsum().apply(np.exp)
hist['underlying_value'] = hist['LogRet'].cumsum().apply(np.exp)
hist.drop(columns=['High', 'Low', 'Open', 'Close', 'Volume'], inplace=True)
hist[['underlying_value', 'position_value']].plot()
plt.savefig("sample_strat.png", dpi=400)
plt.show()

# @update.register(Position)
# def _(a, b):
#     if a.ticker_symbol == b.ticker_symbol:
#         return Position(ticker_symbol=a.ticker_symbol, init_date=a.init_date, roll_date=b.init_date, shares=a.shares+b.shares)
#     else:
#         Portfolio = OrderedDict()
#         Portfolio[a.ticker_symbol] = a
#         Portfolio[b.ticker_symbol] = b
#         return Portfolio
print(pd.date_range(start='1/1/2018', end='1/08/2018'))
# print(update(Position(ticker_symbol="SPY", init_date="2020-05-05", shares=2), Position(ticker_symbol="QQQ", init_date="2020-05-05", shares=4) ))
from dataclasses import dataclass, field
import pandas as pd
import logging
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce
import pandas_datareader.data as web
logging.basicConfig(filename='trade.log', encoding='utf-8', level=logging.INFO)

pdts = pd.Timestamp
pd.set_option("display.max_columns", 10)

def check_date(func):
    def wrapper(self, *args):
        if not pdts(args[0]).dayofweek<5: raise Exception(f'{args[0]} is a weekend')
        res = func(self, *args)
        return res
    return wrapper

def check_lens(func):
    def wrapper(self, symbol_list, date, weights=[]):
        weights = np.array(weights)
        if len(weights) == 0:
            weights = len(symbol_list) * [1 / len(symbol_list)]
        else:
            weights = weights / len(weights)
        assert len(weights) == len(symbol_list)

        if not isinstance(date, str):
            assert len(date) == len(symbol_list)
        else:
            date = [date] * len(symbol_list)
        res = func(self, symbol_list, date, weights)
        return res
    return wrapper


@dataclass
class Position:
    ticker_symbol: str
    init_date: str
    init_price: field(default=float, repr=False)
    quantity: field(default=float, repr=False)
    trades: None = None
    exit_date: None = None
    exit_price: None = field(default=None, repr=False)
    id: int = field(default=-1, repr=True)

    def __post_init__(self):
        if not pdts(self.init_date).dayofweek < 5: raise Exception(f'{self.init_date} is a weekend')
        self.init_quantity = self.quantity
        self.history = None
        self.history_saved = False
        self.is_open = True
        self.trades = {'date':[], 'price':[], 'shares':[], 'position':[]}
        self.update(self.init_date, self.init_price, self.quantity)
        self.today = pd.Timestamp.today().floor('D')
        logging.info(f"BTO {self.quantity} shares of {self.ticker_symbol} at {self.init_price} on {self.init_date}")
        self.id = self.__hash__()
        # self.save_history()
    @check_date
    def buy(self, buy_date, price, shares):
        if self.is_open:
            # if not pdts(buy_date).dayofweek<5: raise Exception(f'{buy_date} is a weekend')
            shares = abs(shares)
            self.update(buy_date, price, shares)
            self.quantity += shares
            logging.info(f"added {shares} shares of {self.ticker_symbol} at {price} on {buy_date}")
        return self

    @check_date
    def sell(self, sell_date, price, shares):
        shares = abs(shares)
        if shares >= self.quantity and self.is_open:
            self.exit(sell_date, price)
        elif shares < self.quantity and self.is_open:
            self.quantity -= shares
            logging.info(f"sold {shares} shares of {self.ticker_symbol} at {price} on {sell_date}")
            self.update(sell_date, price, -shares)
        else:
            raise RuntimeError("Something not right")
        return self

    @check_date
    def exit(self, exit_date, exit_price):
        self.exit_price = exit_price
        self.exit_date = exit_date
        logging.info(f"STC {self.quantity} shares of {self.ticker_symbol} at {exit_price} on {exit_date}")
        self.update(exit_date, exit_price, -self.quantity)
        self.is_open = False
        self.quantity = 0
        return self

    @property
    def cash_invested(self):
        if self.is_open:
            return np.dot(self.trades['price'], self.trades['shares'])
        else:
            return np.dot(self.trades['price'][:-1], self.trades['shares'][:-1])

    @property
    def days_open(self):
        if self.is_open:
            return (self.today - self.trades['date'][0]).days
        else:
            return (self.trades['date'][-1] - self.trades['date'][0]).days

    def update(self, date, price, shares):
        self.trades['date'].append(pdts(date))
        self.trades['price'].append(price)
        self.trades['shares'].append(shares)
        self.trades['position'].append(np.sign(shares))

    def save_history(self, start=False):
        if not start:
            hist = web.DataReader(self.ticker_symbol, 'yahoo', self.trades['date'][0]+pd.to_timedelta(-1,unit='d'), self.today)
        else:
            hist = web.DataReader(self.ticker_symbol, 'yahoo',start + pd.to_timedelta(-1, unit='d'),
                                  self.today)
        hist['LogRet'] = np.log(hist['Adj Close']).diff()
        hist['Position'] = np.nan
        hist.loc[self.trades['date'], 'Position'] = np.cumsum(self.trades['shares'])
        hist['Position'].fillna(inplace=True, method='ffill')
        hist['position_return'] = hist['Position'].shift(1)*hist['LogRet']
        hist['position_value'] = hist['position_return'].cumsum().apply(np.exp)
        hist['underlying_value'] = hist['LogRet'].cumsum().apply(np.exp)
        hist.drop(columns=['High', 'Low', 'Open', 'Close', 'Volume'], inplace=True)
        # hist.dropna(inplace=True)
        self.history_saved = True
        self.history = hist
        self.fix_trade_price()
        return self

    def fix_trade_price(self):
        self.trades['price'] = self.history.loc[self.trades['date']]['Adj Close'].tolist()
        self.init_price = self.trades['price'][0]

    @property
    def returns(self):
        assert self.history_saved
        return self.history['position_value'].iloc[-1]-1


    def summary(self, plots=True):
        if not self.history_saved:
            self.save_history()
        print(f'Ticker symbol: {self.ticker_symbol}')
        print(f'Return over {self.days_open} days: {round(self.returns*100,3)}%')
        print(f'Total cash invested: {self.cash_invested}')
        if plots:
            plt.figure()
            self.history[['position_value', 'underlying_value']].plot()
            plt.figure()
            plt.title(f'{self.ticker_symbol} trades')
            pos_changes = self.history['Position'].diff()
            pos_changes[0] = self.trades['shares'][0]
            plt.stem(self.history.index, pos_changes)
            plt.show()

    def __hash__(self):
        return hash((self.ticker_symbol, self.init_date))


class Portfolio:
    def __init__(self, *positions, cash=10000):
        self.cash = cash
        self.portfolio = {}
        self.id_list = []
        self.symbol_list = []
        self.from_list_ = False
        if len(positions)>0:
            for pos in positions:
                if pos.id in self.id_list:
                    raise Warning(f"{pos} is a duplicate position")
                    return None
                if pos.ticker_symbol in self.symbol_list:
                    raise Warning(f"{pos} is a duplicate symbol, new key is (symbol, id)")
                    self.portfolio[pos.ticker_symbol, pos.id] = pos
                assert pos.is_open
                self.portfolio[pos.ticker_symbol] = pos
                self.id_list.append(pos.id)
                self.symbol_list.append(pos.ticker_symbol)

    def add_position(self, pos):
        if pos.id in self.id_list:
            raise Warning(f"{pos} is a duplicate position")
            return None
        if pos.ticker_symbol in self.symbol_list:
            raise Warning(f"{pos} is a duplicate symbol, new key is (symbol, id)")
            self.portfolio[pos.ticker_symbol, pos.id] = pos
            self.symbol_list.append(pos.ticker_symbol)
        assert pos.is_open
        self.portfolio[pos.ticker_symbol] = pos

    @check_lens
    def from_list(self, symbol_list, date, weights=[]):
        for symbol, _date, w in zip(symbol_list, date, weights):
            pos = Position(symbol, _date, 0, 0).save_history()
            pos.buy(_date,pos.init_price, w*self.cash/pos.init_price).save_history()
            self.add_position(pos)
        self.from_list_ = True
        return self

    def to_pandas(self):
        histories = []
        self.portfolio_df = pd.DataFrame()
        for symbol, pos in self.portfolio.items():
            if self.from_list_:
                self.portfolio_df[symbol] = pos.history['position_value']#*pos.init_value*pos.trades['shares'][1]
                pos.summary()
                print(pos.trades['shares'])
        print(self.portfolio_df)




# pos = Position("SPY",  '2020-08-20', 430, 1).buy("2021-09-18", 440, 2)#.sell('2021-09-11', 440, 2)
#
# pos.summary()
Portfolio().from_list(['SPY', 'IWM'], '2020-02-05').to_pandas()
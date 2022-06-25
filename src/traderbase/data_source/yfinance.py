import time
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd

from traderbase.data_source.base import DataSource
from traderbase.cache import Cache
import traderbase.constants as c


class YFinance(DataSource):

    MAX_RETRIES = 20

    def __init__(self, prop: dict = {}):

        if 'historical' not in prop:
            prop['historical'] = True

        super().__init__(prop)

    def fetch(self):

        # dateparser.parse() assumes english language, uses MDY date order
        orig_start_str = f'{self.p["start_year"]}-{self.p["start_month"]}-{self.p["start_day"]}'
        orig_end_str = f'{self.p["end_year"]}-{self.p["end_month"]}-{self.p["end_day"]}'

        if 'end_hour' in self.p and 'end_minute' in self.p:
            if self.p['historical'] is True:
                raise Exception('Investigate in which case this is working fine. It seems that hours and minutes are not supported in yfinance')
                orig_end_str = orig_end_str + f' {self.p["end_hour"]}:{self.p["end_minute"]}'
            else:
                # We just add +1 day as passing in hours and minutes does not seem to work. YFinance will just return the last bar available.
                end_dt = datetime.strptime(orig_end_str, '%Y-%m-%d')
                end_dt = end_dt + timedelta(days=1)
                orig_end_str = end_dt.strftime('%Y-%m-%d')

        if self.p['min_bar_interval'] == '1m':
            raise Exception('Fetching of klines should be adapted as 1 * 60* 24 is more than the 1000 records limit')

        for i in range(self.MAX_RETRIES):
            try:
                df = yf.download(self.p["symbol"], start=orig_start_str, end=orig_end_str, interval=self.p['min_bar_interval'])
                break
            except KeyError:
                time.sleep(1)
                pass

        if df.empty:
            raise Exception(f'No data returned for {self.p["symbol"]}. Check the logs')

        if df.index.tz is not None:
            # YFinance returns local timezone times, we standardise to UTC
            # The index represents the closing time of the bar so no extra actions needed to standardise.
            df.index = df.index.tz_convert('UTC')
        else:
            df.index = df.index.tz_localize('UTC')

        # Remove the duplicated rows as we fetch each day's 00:00 twice.
        df = df[~df.index.duplicated(keep='first')]

        drop_cols = ['Adj Close']
        df.drop(columns=drop_cols, inplace=True)

        # Standard col names
        df.rename(columns={'Open': c.O, 'High': c.H, 'Low': c.L, 'Close': c.C, 'Volume': c.VOLUME}, inplace=True)

        # To floats
        for col in [c.O, c.H, c.L, c.C, c.VOLUME]:
            df[col] = df[col].astype(float)

        # Standard index name for later read from cache.
        df.index.rename(c.T, inplace=True)

        # 100% made up
        df[c.TC] = 0

        Cache.set(self.get_uuid(), df)

        return df

    @staticmethod
    def dynamic_factory():
        return 'dynamic_yfinance'

    def _time_interval(self):
        return self.p['min_bar_interval']

import logging
import os
import datetime
import time
import random
import dateparser

from binance import Client
import pandas as pd

from traderbase.data_source.base import DataSource
from traderbase.cache import Cache
import traderbase.constants as c


class BinanceAPI(object):

    client = None

    def __init__(self):
        key = os.environ['BINANCE_API_KEY']
        secret = os.environ['BINANCE_API_SECRET']
        self.client = Client(key, secret)

    def get_client(self):
        return self.client


class Binance(DataSource):

    MAX_RETRIES = 5

    TC = 'trades_count'
    QUOTE_VOL = 'quote_volume'

    def __init__(self, prop: dict = {}):
        if prop['binanceapi']:
            self.api = prop['binanceapi']
        else:
            self.api = BinanceAPI

        if 'historical' not in prop:
            prop['historical'] = True

        super().__init__(prop)

    def fetch(self):

        # dateparser.parse() assumes english language, uses MDY date order
        orig_start_str = f'{self.p["start_month"]}-{self.p["start_day"]}-{self.p["start_year"]}'
        orig_end_str = f'{self.p["end_month"]}-{self.p["end_day"]}-{self.p["end_year"]}'

        if 'end_hour' in self.p and 'end_minute' in self.p:
            orig_end_str = orig_end_str + f' {self.p["end_hour"]}:{self.p["end_minute"]}'

        if self.p['min_bar_interval'] == '1m':
            raise Exception('Fetching of klines should be adapted as 1 * 60* 24 is more than the 1000 records limit')

        # Using dateparser as the binance lib internally does.
        iteration_dt = dateparser.parse(orig_start_str)
        end_dt = dateparser.parse(orig_end_str)

        hours_delta = self._hours_delta_period()

        # Iterate by days as we need to make sure that we can get all the data (i.e. every 3m = 480 records)
        raw = []
        it_raw = None
        while iteration_dt < end_dt:

            if self.p['historical'] is True:

                start_str = iteration_dt.strftime('%m-%d-%Y')
                # +1 day
                end_str = (iteration_dt + datetime.timedelta(hours=hours_delta)).strftime('%m-%d-%Y')

                logging.debug(f'Fetch {self.p["symbol"]} data from {start_str} to {end_str}')

                # The lines are returned in epoch timestamp and UTC
                exception = None
                for i in range(self.MAX_RETRIES):
                    try:
                        it_raw = self.api.get_client().get_historical_klines(symbol=self.p['symbol'],
                                                                   interval=self._time_interval(),
                                                                   start_str=start_str, end_str=end_str,
                                                                   limit=1000)
                        # Break if success.
                        exception = None
                        break
                    except Exception as e:
                        exception = e
                        # Likely that we run out of binance fuel.
                        time.sleep(30)

                if exception:
                    raise exception

            else:

                iteration_delta_hours = int(hours_delta) + int(self.p['end_hour'])
                iteration_delta_minutes = int(self.p['end_minute'])
                iteration_end_dt = iteration_dt + datetime.timedelta(hours=iteration_delta_hours, minutes=iteration_delta_minutes)

                # - The lines are returned in epoch timestamp and UTC
                # startTime and endTime expect epoch in miliseconds.
                exception = None
                for i in range(self.MAX_RETRIES):
                    try:
                        it_raw = self.api.get_client().get_klines(symbol=self.p['symbol'],
                                                        interval=self._time_interval(),
                                                        startTime=int(iteration_dt.timestamp() * 1000),
                                                        endTime=int(iteration_end_dt.timestamp() * 1000),
                                                        limit=1000)
                        # Break if success.
                        exception = None
                        break
                    except Exception as e:
                        exception = e
                        # Likely that we run out of binance fuel.
                        time.sleep(30)

                if exception:
                    raise exception

            raw.extend(it_raw)

            iteration_dt = iteration_dt + datetime.timedelta(hours=hours_delta)

            # Sleep a random num of milliseconds.
            time.sleep(random.random() / 4)

        cols = ['opentime', c.O, c.H, c.L, c.C, c.VOLUME,
                'closetime', self.QUOTE_VOL, c.TC,
                'taker_base_volume', 'taker_quote_volume', '_ignore1']
        df = pd.DataFrame.from_records(raw, columns=cols)

        # The bar starting at 8:00 in the UI is returned before closing. However, we don't want to use it until it is closed.
        # Timedate comes as miliseconds and for whatever reason -1 second, what is why we need the +1 trick...
        # datetime64[ns, UTC]
        df.index = pd.to_datetime(((df['closetime'] / 1000) + 1).astype(int), unit='s', utc=True)

        # Remove the duplicated rows as we fetch each day's 00:00 twice.
        df = df[~df.index.duplicated(keep='first')]

        drop_cols = ['closetime', 'opentime', '_ignore1', self.QUOTE_VOL,
                     'taker_base_volume', 'taker_quote_volume']
        df.drop(columns=drop_cols, inplace=True)

        # To floats
        for col in [c.O, c.H, c.L, c.C, c.VOLUME]:
            df[col] = df[col].astype(float)

        # Standard index name for later read from cache.
        df.index.rename(c.T, inplace=True)

        Cache.set(self.get_uuid(), df)

        return df

    @staticmethod
    def dynamic_factory():
        return 'dynamic_binance'

    def _extra_col_renames(self):
        return {
            self.TC: self._id + '-' + c.TC,
            self.QUOTE_VOL: self._id + '-' + c.TC,
        }

    def _time_interval(self):
        if self.p['min_bar_interval'] == '5m':
            return Client.KLINE_INTERVAL_5MINUTE
        elif self.p['min_bar_interval'] == '15m':
            return Client.KLINE_INTERVAL_15MINUTE
        elif self.p['min_bar_interval'] == '30m':
            return Client.KLINE_INTERVAL_30MINUTE
        elif self.p['min_bar_interval'] == '1h':
            return Client.KLINE_INTERVAL_1HOUR
        elif self.p['min_bar_interval'] == '4h':
            return Client.KLINE_INTERVAL_4HOUR
        elif self.p['min_bar_interval'] == '1d':
            return Client.KLINE_INTERVAL_1DAY
        else:
            raise Exception(f'Unsupported {self.p["min_bar_interval"]} interval')

    def _hours_delta_period(self):
        """We need to make sure we fetch less than 1000 records per request."""

        if self.p['min_bar_interval'] == '5m':
            # 12 every hour x 80h = 960
            return 80
        elif self.p['min_bar_interval'] == '15m':
            # 4 every hour x 248h = 992
            return 248
        elif self.p['min_bar_interval'] == '30m':
            return 480
        elif self.p['min_bar_interval'] == '1h':
            return 990
        elif self.p['min_bar_interval'] == '4h':
            return 3900
        elif self.p['min_bar_interval'] == '1d':
            return 23900
        else:
            raise Exception(f'Unsupported {self.p["min_bar_interval"]} interval')

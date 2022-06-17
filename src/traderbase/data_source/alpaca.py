import os
from enum import Enum

import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame

import traderbase.constants as c
from traderbase.data_source.base import DataSource
from traderbase.cache import Cache


class AlpacaApi(object):

    def get(self):
        return REST()


class AlpacaExtraTimeFrame(Enum):
    FiveMinutes = '5Min'
    FifteenMinutes = '15Min'
    ThirtyMinutes = '30Min'
    FourHours = '4Hour'


class Alpaca(DataSource):

    TC = 'trade_count'
    VWAP = 'vwap'

    def fetch(self):

        api = AlpacaApi()

        df = api.get().get_bars(self.p['symbol'], self._time_interval(),
                                start=f'{self.p["start_year"]}-{self.p["start_month"]}-{self.p["start_day"]}',
                                end=f'{self.p["end_year"]}-{self.p["end_month"]}-{self.p["end_day"]}').df

        # TODO asyncio
        # result = await api.get().get_bars_async(symbol=self.p['symbol'], timeframe=self._time_interval().value,
        #                         start=f'{self.p["start_year"]}-{self.p["start_month"]}-{self.p["start_day"]}',
        #                         end=f'{self.p["end_year"]}-{self.p["end_month"]}-{self.p["end_day"]}')

        # Alpaca is so shit that daily bars do not come in UTC while lower intervals do come in UTC
        if self.p['min_bar_interval'] != '1d':
            # Data comes in utc, we still force the utc so that we standardise datasets to datetime64[ns, UTC]
            # Timedelta as the bars timestamp is the beginning time of the bar and we need to use
            # the bar closing time.
            df.index = pd.to_datetime(df.index, utc=True) + pd.Timedelta(self.p['min_bar_interval'])

        if self.p['min_bar_interval'] == '1d':
            df.index = df.index.floor(freq='D')

        # Standardise trade counts col name. Can't reuse self.TC as it gets prefixed with the symbol.
        df.rename(columns={'trade_count': c.TC}, inplace=True)

        drop_cols = [self.VWAP]
        df.drop(columns=drop_cols, inplace=True)

        # Standard index name for later read from cache.
        df.index.rename(c.T, inplace=True)

        Cache.set(self.get_uuid(), df)

        return df

    @staticmethod
    def dynamic_factory():
        raise Exception('TODO dynamic_alpaca')
        return 'dynamic_alpaca'

    def _extra_col_renames(self):
        # TODO Smells like deprecated to me, keeping temporarily just in case I am missing something.
        return {
            self.TC: self._id + '-' + c.TC,
            self.VWAP: self._id + '-vwap',
        }

    def _time_interval(self):
        if self.p['min_bar_interval'] == '5m':
            return AlpacaExtraTimeFrame.FiveMinutes
        elif self.p['min_bar_interval'] == '15m':
            return AlpacaExtraTimeFrame.FifteenMinutes
        elif self.p['min_bar_interval'] == '30m':
            return AlpacaExtraTimeFrame.ThirtyMinutes
        elif self.p['min_bar_interval'] == '1h':
            return TimeFrame.Hour
        elif self.p['min_bar_interval'] == '4h':
            return AlpacaExtraTimeFrame.Hour
        elif self.p['min_bar_interval'] == '1d':
            return TimeFrame.Day
        else:
            raise Exception(f'Unsupported {self.p["min_bar_interval"]} interval')

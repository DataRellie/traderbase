from datetime import datetime, timedelta
import calendar
import math
import asyncio
import random

import dateparser

import pandas as pd
import numpy as np
from ib_insync import *

import traderbase.constants as c
from traderbase.data_source.base import DataSource
from traderbase.cache import Cache


class InteractiveBrokers(DataSource):
    """This really need some love. It is following bad practices"""

    TIMEOUT = 1800

    def fetch(self):

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        host = os.getenv('INTERACTIVEBROKERS_HOST', '127.0.0.1')
        port = int(os.getenv('INTERACTIVEBROKERS_PORT', 7496))

        ib = IB()
        ib.connect(host, port, clientId=random.randint(100, 999))

        start_str = f'{self.p["start_month"]}-{self.p["start_day"]}-{self.p["start_year"]}'
        end_str = f'{self.p["end_month"]}-{self.p["end_day"]}-{self.p["end_year"]}'
        if 'end_hour' in self.p and 'end_minute' in self.p:
            end_str = end_str + f' {self.p["end_hour"]}:{self.p["end_minute"]}'

        start = dateparser.parse(start_str)
        end = dateparser.parse(end_str)
        # IB reads the end as a timezone-aware time

        if self.p['symbol_data']['contract_type'] == 'Future':
            df = self.get_future_df(ib, start, end)
        else:
            df = self.get_df(ib, start, end)

        if df is None:
            raise Exception(f'Symbol {self.p["symbol"]} failed to get retrieved from Interactive Brokers')

        # We have identified some duplicated indexes in XJO 2018, adding this for safety.
        df = df[~df.index.duplicated(keep='last')]

        Cache.set(self.get_uuid(), df)

        # TODO This is crap, should be a singleton
        ib.disconnect()

        return df

    @staticmethod
    def ib_df_cleanup(df: pd.DataFrame, start: datetime, end: datetime):

        df = df.set_index('date')

        df.index = pd.to_datetime(df.index, utc=True)

        drop_cols = ['average', 'barCount']
        df.drop(columns=drop_cols, inplace=True)

        # 100% there are no weird rows here.
        # df = df[(df.index >= start) & (df.index <= end)]

        df[c.TC] = np.nan

        # Standard index name for later read from cache.
        df.index.rename(c.T, inplace=True)

        return df

    def get_future_contract(self, contract_month: int):
        return Future(self.p['symbol'], lastTradeDateOrContractMonth=str(contract_month), **self.p['symbol_data']['kwargs'])

    def get_contract(self):
        type = self.p['symbol_data']['contract_type']
        if type == 'Forex':
            return Forex(self.p['symbol'])
        elif type == 'Stock':
            return Stock(self.p['symbol'], **self.p['symbol_data']['kwargs'])
        elif type == 'CFD':
            return CFD(self.p['symbol'], **self.p['symbol_data']['kwargs'])
        elif type == 'Future':
            raise Exception('Use get_future_contract')
        elif type == 'Index':
            return Index(self.p['symbol'], **self.p['symbol_data']['kwargs'])
        elif type == 'Commodity':
            return Commodity(self.p['symbol'], **self.p['symbol_data']['kwargs'])

    def get_df(self, ib: IB, start: datetime, end: datetime):

        contract = self.get_contract()
        duration_str = self._duration_str(start, end)

        # formatDate = 2 for UTC
        bars = ib.reqHistoricalData(
            contract, endDateTime=end, durationStr=duration_str, formatDate=2,
            barSizeSetting=self._time_interval(),
            whatToShow=self.p['symbol_data']['what_to_show'],
            useRTH=self.p['symbol_data']['use_rth'],
            timeout=self.TIMEOUT)
        df = util.df(bars)

        df = self.ib_df_cleanup(df, start, end)

        return df

    def get_future_df(self, ib: IB, start: datetime, end: datetime):
        """Similar to get_df but automatically generating a contract time"""

        df = None
        offset = start
        while offset < end:

            # We need to craft a
            contract_month = offset.month + 1
            contract_year = offset.year
            if contract_month > 12:
                contract_month = contract_month - 12
                contract_year = contract_year + 1

            # Integer format.
            contract_date = (contract_year * 100) + contract_month

            contract = self.get_future_contract(contract_date)

            _, last_day_month = calendar.monthrange(offset.year, offset.month)
            offset_end = offset.replace(day=last_day_month)
            duration_str = self._duration_str(offset, offset_end)

            # formatDate = 2 for UTC
            bars = ib.reqHistoricalData(
                contract, endDateTime=offset_end, durationStr=duration_str, formatDate=2,
                barSizeSetting=self._time_interval(),
                whatToShow=self.p['symbol_data']['what_to_show'],
                useRTH=self.p['symbol_data']['use_rth'],
                timeout=self.TIMEOUT)

            if len(bars) == 0:
                offset = offset_end + timedelta(days=1)
                continue

            it_df = util.df(bars)
            it_df = self.ib_df_cleanup(it_df, offset, offset_end)

            if df is None:
                # First iteration
                df = it_df
            else:
                # Concat
                df = pd.concat([df, it_df], axis=1)

            offset = offset_end + timedelta(days=1)

        return df

    @staticmethod
    def dynamic_factory():
        raise Exception('TODO dynamic_interactivebrokers')
        return 'dynamic_interactivebrokers'

    def _extra_col_renames(self):
        # TODO Smells like deprecated to me, keeping temporarily just in case I am missing something.
        return {
        }

    def _time_interval(self):
        if self.p['min_bar_interval'] == '5m':
            return '5 mins'
        elif self.p['min_bar_interval'] == '15m':
            return '15 mins'
        elif self.p['min_bar_interval'] == '30m':
            return '30 mins'
        elif self.p['min_bar_interval'] == '1h':
            return '1 hour'
        elif self.p['min_bar_interval'] == '4h':
            return '4 hours'
        elif self.p['min_bar_interval'] == '1d':
            return '1 day'
        else:
            raise Exception(f'Unsupported {self.p["min_bar_interval"]} interval')

    def _duration_str(self, start: datetime, end: datetime):

        diff_days = (end - start).days
        #  Historical data requests for durations longer than 365 days must be made in years.
        if diff_days <= 365:
            duration_str = f'{diff_days} D'
        else:
            diff_years_ceil = math.ceil(diff_days / 365)
            duration_str = f'{diff_years_ceil} Y'

        return duration_str

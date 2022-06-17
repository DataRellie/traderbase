import json
import abc
import hashlib

import pandas as pd

import traderbase.constants as c
from traderbase.cache import Cache


class DataSource(metaclass=abc.ABCMeta):

    p = {}

    def __init__(self, p: dict = {}):
        self.p = p

        self._id = self.set_id()

        self.O = self._id + '-' + c.O
        self.H = self._id + '-' + c.H
        self.L = self._id + '-' + c.L
        self.C = self._id + '-' + c.C
        self.V = self._id + '-' + c.VOLUME
        self.TC = self._id + '-' + c.TC

    def get_uuid(self):

        uuid_params = {}
        for key, p in self.p.items():

            # These are the only relevant parameters for datasets.
            if 'start_' not in key and 'end_' not in key and 'min_bar_interval' not in key:
                continue
            try:
                json.dumps(p)
                uuid_params[key] = p
            except:
                continue

        md5hash = hashlib.md5()
        md5hash.update(json.dumps(uuid_params).encode('utf-8'))
        return self.__class__.__name__ + '-' + self.p['symbol'] + '-' + md5hash.hexdigest()

    def set_id(self):
        """Default implementation that just wants to get something unique generated."""
        return self.p['symbol']

    def get_id(self):
        return self._id

    def get_data(self) -> pd.DataFrame:

        if Cache.exists(self.get_uuid()):
            df = Cache.get(self.get_uuid())

            # Make sure the index is in datetime UTC format after reading from the cache.
            df.index = pd.to_datetime(df.index, utc=True)
        else:
            # Fetch from the source
            df = self.fetch()

        # After prepare we have the same df regardless of it coming from the source
        # or from the cache.
        prepared_df = self.prepare(df)
        prepared_df.sort_index(inplace=True, ascending=True)

        return prepared_df

    @staticmethod
    @abc.abstractmethod
    def dynamic_factory():
        pass

    @abc.abstractmethod
    def fetch(self):
        """Fetch data from the source

        The data frame returned from this function is what will be cached

        - We want data to be in UTC, force to_datetime to utc=True if necessary
        - We use closing times of the bars as index.
        - The indexes should use datetime64[ns, UTC] format otherwise we won't be able to join datasets
        """
        pass

    def prepare(self, df):
        """Prepares the provided data frame coming from fetch() or from the cache

        Nothing by default"""
        return df

    def format_merge(self, df):
        # TODO Smells like unnecessary now that we specify a rsuffix in pd.join, keeping temporarily just in case I am missing something.
        renames = {
            c.O: self.O, c.H: self.H, c.L: self.L, c.C: self.C,
            c.VOLUME: self.V, c.TC: self.TC
        }
        renames.update(self._extra_col_renames())
        return df.rename(columns=renames)

    def _extra_col_renames(self):
        """Hook to return a map of extra columns that need to be renamed"""
        # TODO Smells like deprecated to me, keeping temporarily just in case I am missing something.
        return {}

    @abc.abstractmethod
    def _time_interval(self):
        pass


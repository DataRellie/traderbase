import time
import logging

from concurrent.futures import ThreadPoolExecutor, as_completed

import traderbase.constants as c
from traderbase.data_source.base import DataSource


class DataIngestor(object):

    def __init__(self, p: dict = {}):
        # Parameters of the strategy
        self.p = p

        # List of data source definitions
        self._data_source_defs = []

        # In-memory data from the sources
        self._data = {}

        # DataFrame with all data sources joined together
        self._df = None

        if 'n_parallel_workers' not in self.p:
            self.p['n_parallel_workers'] = c.N_PARALLEL_WORKERS

    def add_ds(self, ds_def: dict):
        self._data_source_defs.append(ds_def)

    def instantiate_dss(self):
        ds_instances = {}
        for ds in self._data_source_defs:

            # Merge global properties with properties specific to the data source.
            merged_props = {**self.p, **ds["p"]}
            ds_instance = ds["class"](merged_props)
            ds_instances[ds_instance.get_id()] = ds_instance
        return ds_instances

    def get_data(self, joined: bool = False):

        ds_instances = self.instantiate_dss()

        # TODO Migrate to asyncio: binance, alpaca and interactive brokers support it.
        # async def get_all(ds_instances):
        #     tasks = [asyncio.ensure_future(run_get_data(ds)) for ds in ds_instances.values()]
        #     results = await asyncio.gather(*tasks)
        #     return results
        #
        # async def run_get_data(ds: DataSource):
        #     # We also need to pass the id as we need to map out futures to ds later.
        #     data = await ds.get_data()
        #     return ds.get_id(), data
        #
        # loop = asyncio.get_event_loop()
        # results = loop.run_until_complete(get_all(ds_instances))
        # loop.close()

        # for dsid, df in results:
        #     self._data[dsid] = df

        def run_get_data(ds: DataSource):
            # We also need to pass the id as we need to map out futures to ds later.
            return ds.get_id(), ds.get_data()

        pool = ThreadPoolExecutor(self.p['n_parallel_workers'])
        futures = []
        for ds in ds_instances.values():
            futures.append(pool.submit(run_get_data, ds))

        for future in as_completed(futures):
            dsid, df = future.result()
            self._data[dsid] = df

        if joined:
            return self.join_data(self._data)
        else:
            return self._data

    def join_data(self, data: dict):

        # We must start with the target data frame because this allows
        # us to join keeping the target's time periods as reference.
        df = data[self.p['label_symbol']]

        for symbol, ds_data in data.items():
            if symbol != self.p['label_symbol']:
                suffix = f'-{symbol}'
                # Join as outer, we will honour the label indexes later after forward filling data from symbols
                # in non-matching timezones.
                df = df.join(ds_data, how='outer', rsuffix=suffix)

        label_df = df[~df['open'].isna()]
        percent_nulls_joined = label_df.isnull().sum() / len(label_df)
        mostly_nulls = [col for col, percent_nulls in percent_nulls_joined.items() if percent_nulls > 0.8]
        if len(mostly_nulls) > 0:
            logging.warning(f'Most values are null after joining with the label trading hours: {mostly_nulls}')

        # We need this below. These are the indexes where the label symbol is not null.
        label_indexes = df[~df['open'].isna()].index

        # The df is in asc order so we should first fill forward to fill empty points
        df = df.fillna(method="ffill")

        # Now filter out all the rows where the label is null.
        # copy & deep just to clear up pandas views.
        df = df.loc[label_indexes].copy(deep=True)

        return df

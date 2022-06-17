import os

import pandas as pd

import traderbase.constants as c


class Cache(object):

    @staticmethod
    def exists(uuid: str):

        if os.getenv('DAYTRADER_NO_CACHE', False):
            # Cache is disabled.
            return False

        file_path = Cache._id_location(uuid)

        if os.path.exists(file_path):
            return True

        return False

    @staticmethod
    def get(uuid: str):

        file_path = Cache._id_location(uuid)
        return pd.read_csv(file_path, index_col=c.T)

    @staticmethod
    def set(uuid: str, df: pd.DataFrame):

        if os.getenv('DAYTRADER_NO_CACHE', False):
            # Cache is disabled.
            return False

        df.to_csv(Cache._id_location(uuid), index=True, index_label=c.T)

    @staticmethod
    def _id_location(uuid: str):

        cache_dir = os.getenv('TRADERBASE_LOCAL_CACHE_DIR', False)
        if cache_dir is False:
            cache_dir = os.path.join(
                os.path.dirname(__file__),
                '..',
                'cache'
            )
        return os.path.join(cache_dir, uuid)

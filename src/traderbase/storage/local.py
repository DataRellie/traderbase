import json
import os
import shutil

import pandas as pd

from traderbase.storage.base import BaseStorage


class LocalStorage(BaseStorage):

    def __init__(self, file_path_base: str):
        self.file_path_base = file_path_base

    def get_df(self, rel_file_path: str, **kwargs):
        return pd.read_csv(self._abs_path(rel_file_path), **kwargs)

    def get_serialisable(self, rel_file_path: str, **kwargs):
        with open(self._abs_path(rel_file_path)) as f:
            obj = json.load(f)
        return obj

    def set_df(self, df: pd.DataFrame, rel_file_path: str, **kwargs):
        """
        Stores a data frame as csv locally

        :param df:
        :param rel_file_path: Path relative to self.prefix (do not include the leading /)
        :param kwargs: Arguments to forward to to_csv
        :return:
        """
        df.to_csv(self._abs_path(rel_file_path), **kwargs)

    def set_serialisable(self, obj, rel_file_path: str, **kwargs):
        """
        Stores the provided contents into a file with name and type as specified in rel_file_path

        :param obj:
        :param rel_file_path: Path relative to self.prefix (do not include the leading /)
        :param kwargs: Arguments to forward to to_csv
        :return:
        """
        with open(self._abs_path(rel_file_path), 'w') as f:
            json.dump(obj, f)

    def cp_to_storage(self, local_path: str, destination: str):
        # Easy for local.
        shutil.copy(local_path, self._abs_path(destination))

    def cp_to_local(self, path: str, local_path: str):
        # Easy for local.
        shutil.copy(self._abs_path(path), local_path)

    def _abs_path(self, rel_file_path: str) -> str:
        return os.path.join(self.file_path_base, rel_file_path)

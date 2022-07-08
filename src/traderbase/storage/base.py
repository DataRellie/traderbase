import abc

import pandas as pd


class BaseStorage(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_df(self, rel_file_path: str):
        """
        Retrieves a data frame from the storage
        :param rel_file_path:  Path relative to the base specified in the constructor
        :return: pd.DataFrame
        """
        pass

    @abc.abstractmethod
    def get_serialisable(self, rel_file_path: str):
        """
        Retrieves a serialisable object from the storage
        :param rel_file_path:  Path relative to the base specified in the constructor
        :return: pd.DataFrame
        """
        pass

    @abc.abstractmethod
    def set_df(self, df: pd.DataFrame, rel_file_path: str, **kwargs):
        """
        Stores a data frame as csv

        :param df:
        :param rel_file_path:  Path relative to the base specified in the constructor
        :param kwargs: Arguments to forward to to_csv
        :return:
        """
        pass

    @abc.abstractmethod
    def set_serialisable(self, obj, rel_file_path: str, **kwargs):
        """
        Stores any json-serialisable object
        :param obj:
        :param rel_file_path:
        :param kwargs:
        :return:
        """
        pass

    @abc.abstractmethod
    def cp_to_storage(self, local_path: str, destination: str):
        pass

    @abc.abstractmethod
    def cp_to_local(self, path: str, local_path: str):
        pass

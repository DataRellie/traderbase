import random
import tempfile
import json

import boto3
import pandas as pd

from traderbase.storage.base import BaseStorage


class S3Storage(BaseStorage):

    def __init__(self, bucket_name: str, prefix: str):
        self.bucket_name = bucket_name
        self.prefix = prefix

    def get_df(self, rel_file_path: str, **kwargs):
        """
        Retrieves a data frame from the storage
        :param rel_file_path:  Path relative to the base specified in the constructor
        :return: pd.DataFrame
        """

        file_path = self.prefix + '/' + rel_file_path
        temp_path = self._new_temp_file()
        s3_client = boto3.client('s3')
        s3_client.download_file(self.bucket_name, file_path, temp_path)

        return pd.read_csv(temp_path, **kwargs)

    def get_serialisable(self, rel_file_path: str, **kwargs):
        """
        Retrieves a serialisable object from the storage
        :param rel_file_path:  Path relative to the base specified in the constructor
        :return: obj
        """

        file_path = self.prefix + '/' + rel_file_path
        temp_path = self._new_temp_file()
        s3_client = boto3.client('s3')
        s3_client.download_file(self.bucket_name, file_path, temp_path)

        with open(temp_path) as f:
            obj = json.load(f, **kwargs)
        return obj

    def set_df(self, df: pd.DataFrame, rel_file_path: str, **kwargs):
        """
        Uploads a data frame as csv to s3

        :param df:
        :param rel_file_path: Path relative to self.prefix (do not include the leading /)
        :param kwargs: Arguments to forward to to_csv
        :return:
        """

        temp_path = self._new_temp_file()
        df.to_csv(temp_path, **kwargs)

        file_path = self.prefix + '/' + rel_file_path

        s3_client = boto3.client('s3')
        s3_client.upload_file(temp_path, self.bucket_name, file_path)

    def set_serialisable(self, obj, rel_file_path: str, **kwargs):
        """
        Stores the provided contents into a file with name and type as specified in rel_file_path to s3

        :param obj:
        :param rel_file_path: Path relative to self.prefix (do not include the leading /)
        :param kwargs: Arguments to forward to to_csv
        :return:
        """

        temp_path = self._new_temp_file()

        with open(temp_path, 'w') as f:
            json.dump(obj, f)

        file_path = self.prefix + '/' + rel_file_path

        s3_client = boto3.client('s3')
        s3_client.upload_file(temp_path, self.bucket_name, file_path)

    def _new_temp_file(self) -> str:
        return tempfile.mkdtemp(prefix='traderbase-') + '/' + str(random.randint(10000, 99999))

import os

from traderbase.storage.base import BaseStorage
from traderbase.storage.local import LocalStorage
from traderbase.storage.s3 import S3Storage


def load_storage_manager() -> BaseStorage:

    storage_backend = os.getenv('TRADERBASE_STORAGE_BACKEND', 'local')
    if storage_backend == 'local':
        local_dir = os.getenv('TRADERBASE_STORAGE_LOCAL_DIR', False)
        if local_dir is False:
            raise Exception(f'TRADERBASE_STORAGE_LOCAL_DIR env var is required for local storage backend')
        storage = LocalStorage(file_path_base=local_dir)

    elif storage_backend == 's3':

        bucket = os.getenv('TRADERBASE_STORAGE_S3_BUCKET', False)
        if bucket is False:
            raise Exception(f'TRADERBASE_STORAGE_S3_BUCKET env var is required for s3 storage backend')

        prefix = os.getenv('TRADERBASE_STORAGE_S3_PREFIX', False)
        if prefix is False:
            raise Exception(f'TRADERBASE_STORAGE_S3_PREFIX env var is required for s3 storage backend')

        storage = S3Storage(bucket_name=bucket, prefix=prefix)
    else:
        raise Exception(f'Storage backend {storage_backend} not supported')

    return storage

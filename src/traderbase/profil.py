import time
import logging


class RuntimeProfiler(object):

    first_time = None
    latest_time = None

    def __init__(self, track_memory=False):
        self.first_time = time.time()
        self.latest_time = self.first_time
        if track_memory is True:
            logging.error('Remember to implement track_memory in RuntimeProfiler')

    def checkpoint(self, prefix: str):

        now = time.time()
        time_diff = round(now - self.latest_time, 2)
        logging.info(f'{prefix}: {time_diff} seconds since the last checkpoint')

        self.latest_time = now

        return now

    def close(self):

        now = self.checkpoint('done')
        time_diff = round(now - self.first_time, 2)
        logging.info(f'task finished. Seconds since the start of the task {time_diff}')

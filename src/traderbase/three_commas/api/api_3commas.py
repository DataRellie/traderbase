import time
import os

from py3cw.request import Py3CW

from traderbase.three_commas.api.base import BaseApi3commas


class Api3commas(BaseApi3commas):

    # Retries before triggering an exception. Separate from the Py3CW as this works on top of it for all exceptions.
    RETRIES = 5

    def __init__(self, paper=True):

        self.paper = paper
        self.base_account = None

        key = os.environ['THREE_COMMAS_API_KEY']
        secret = os.environ['THREE_COMMAS_API_SECRET']
        self.p3cw = Py3CW(
            key=key,
            secret=secret,
            request_options={
                'request_timeout': 10,
                'nr_of_retries': 10,
                # 429 is limit usage reached.
                'retry_status_codes': [502, 429]
            }
        )

    def is_paper(self):
        return self.paper

    def request(self, *args, **kwargs):

        if self.paper is True:
            forced_mode = 'paper'
        else:
            forced_mode = 'real'
        kwargs['additional_headers'] = {'Forced-mode': forced_mode}

        # They come as {} from the api when no errors found, using False for readability
        error = False
        data = {}

        for i in range(self.RETRIES):
            error, data = self.p3cw.request(*args, **kwargs)
            if bool(error) is False:
                break
            # Incrementally wait a bit if an error was returned
            time.sleep(2 * i)

        if error:
            raise Exception(error)

        return data

    def get_base_account(self):
        accounts_data = self.request(entity='accounts', action='')

        if self.base_account is not None:
            return self.base_account

        # Calculate it otherwise
        if self.paper is True:
            self.base_account = accounts_data[0]
        else:
            for account_data in accounts_data:
                if account_data['market_code'] == 'binance':
                    self.base_account = account_data

        return self.base_account

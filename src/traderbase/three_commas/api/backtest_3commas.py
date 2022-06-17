import logging

from traderbase.three_commas.api.base import BaseApi3commas


class BacktestApi3commas(BaseApi3commas):

    def __init__(self, paper=True):

        self.paper = paper
        self.base_account = {
            'id': 99999999,
            'market_code': 'backtesting'
        }

        self.latest_id = 1

        # Returned as a list but stored as dict to keep a mapping with the ids.
        # We hold the bots here because they are created through the API. The state reads them on every cycle.
        self.bots = {}
        self.bots_enabled = {}
        self.deals_early_sell = {}
        self.deals_cancelled = {}
        self.deals_converted_to_smart_trade = {}

    def is_paper(self):
        return self.paper

    def request(self, *args, **kwargs):

        if kwargs['entity'] == 'bots' and kwargs['action'] == '':
            return [item for k, item in self.bots.items()]

        if kwargs['entity'] == 'bots' and kwargs['action'] == 'create_bot':
            bot = kwargs['payload']
            bot['id'] = int(self.generate_unique_id())
            self.bots[bot['id']] = bot
            return bot

        if kwargs['entity'] == 'bots' and kwargs['action'] == 'update':
            # Likely that this will need refinements.
            bot = kwargs['payload']
            bot['id'] = int(kwargs['action_id'])
            self.bots[bot['id']] = bot
            return bot

        if kwargs['entity'] == 'bots' and kwargs['action'] == 'enable':
            self.bots_enabled[kwargs['action_id']] = True
            return

        if kwargs['entity'] == 'bots' and kwargs['action'] == 'disable':
            self.bots_enabled[kwargs['action_id']] = False
            return

        if kwargs['entity'] == 'deals' and kwargs['action'] == 'update_tp':
            # TODO What we do now is hardcode PN.TP_MIN_PERCENT as TP for the deals that got
            # converted to smart trades...
            return

        if kwargs['entity'] == 'deals' and kwargs['action'] == 'cancel':
            self.deals_cancelled[kwargs['action_id']] = True
            return

        if kwargs['entity'] == 'smart_trades_v2' and kwargs['action'] == 'set_note':
            return

        if kwargs['entity'] == 'deals' and kwargs['action'] == 'convert_to_smart_trade':
            self.deals_converted_to_smart_trade[kwargs['action_id']] = True

            # This is supposed to be a smart trade.
            return {
                'id': self.generate_unique_id(),
                'note_raw': f'Generated from deal {self.generate_unique_id()}'
            }

        if kwargs['entity'] == 'deals' and kwargs['action'] == 'panic_sell':
            self.deals_early_sell[kwargs['action_id']] = True
            return

        logging.error(args)
        logging.error(kwargs)
        raise Exception('request')

    def generate_unique_id(self):
        """KISS"""

        new_id = self.latest_id + 1
        self.latest_id = new_id
        return new_id

    def get_base_account(self):
        return self.base_account


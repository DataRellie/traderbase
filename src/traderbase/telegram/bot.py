import os

import telegram


class Bot(object):

    @staticmethod
    def send_message(msg):
        """Initialise bot in __init__ if we end up using it more than "not often"
        """

        token = os.environ['TELEGRAM_TOKEN']
        chatid = os.environ['TELEGRAM_CHATID']

        bot = telegram.Bot(token=token)
        bot.send_message(text=msg, chat_id=chatid)

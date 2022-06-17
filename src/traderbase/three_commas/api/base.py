import abc


class BaseApi3commas(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, paper=True):
        pass

    @abc.abstractmethod
    def is_paper(self):
        pass

    @abc.abstractmethod
    def request(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_base_account(self):
        pass

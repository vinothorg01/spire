import inspect

from datasci.utils import LoggingManager

lm = LoggingManager("spire")


class Logger:
    @property
    def logger(self):
        name = ".".join([__name__, self.__class__.__name__])
        logger = lm.get_logger(name)
        return logger


def get_logger(name=None):
    if name is None:
        # get caller's module name
        from_ = inspect.stack()[1]
        module = inspect.getmodule(from_[0])
        name = module.__name__
    return lm.get_logger(name)

import logging
import os
from datetime import datetime


class FalconLogger:
    """
    Singleton class for Logger object
    """
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:

            print("Logger new")

            dirname = "/tmp/"
            current_dt = datetime.now().strftime("%Y%m%d_%H%M%S")
            logfilename = "log_" + current_dt + ".log"
            finalpath = dirname + logfilename

            cls._logger = super().__new__(cls, *args, **kwargs)
            logging.getLogger("py4j").setLevel(logging.ERROR)
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logging.INFO)

            # file_handler = logging.FileHandler(finalpath, mode = 'a')


            formatter = logging.Formatter(
                '%(asctime)s - [%(levelname)s | %(filename)s:%(lineno)s] > %(message)s')



            if not os.path.isdir(dirname):
                os.mkdir(dirname)


            # file_handler.setFormatter(formatter)

            # cls._logger.addHandler(file_handler)

        return cls._logger

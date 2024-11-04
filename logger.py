import os
import logging
import threading
from datetime import datetime

class DailyLogger(logging.Logger):
    def __init__(self, name, log_dir, log_name, level=logging.INFO):
        super().__init__(name, level)
        self.log_dir = log_dir
        self.log_name = log_name
        self.current_date = None
        self.file_handler = None
        self.handler_lock = threading.Lock()
        self.prefix = ''

    def update_handler(self):
        with self.handler_lock:
            new_date = datetime.now().strftime('%Y-%m-%d')
            underscore = '_' if self.prefix else ''            
            if self.current_date:
                new_filename = f'{self.log_dir}/{self.current_date}_{self.prefix}{underscore}{self.log_name}.log'
            else:
                new_filename = f'{self.log_dir}/{new_date}_{self.prefix}{underscore}{self.log_name}.log'
            if new_date != self.current_date or (self.file_handler and self.file_handler.baseFilename != new_filename):
                self.current_date = new_date

                log_filename = new_filename                

                if not os.path.exists(self.log_dir):
                    os.makedirs(self.log_dir)

                if self.file_handler:
                    self.removeHandler(self.file_handler)
                    self.file_handler.close()

                self.file_handler = logging.FileHandler(log_filename)
                formatter = logging.Formatter('%(asctime)s:%(threadName)s:%(levelname)s:%(message)s')
                self.file_handler.setFormatter(formatter)
                self.addHandler(self.file_handler)

    def handle(self, record):
        self.update_handler()
        super().handle(record)

    def set_prefix(self, prefix):
        self.prefix = prefix
        self.update_handler()

    @classmethod
    def get_logger(cls, name, log_dir, log_name, level=logging.INFO, prefix=''):
        logger = logging.getLogger(name)
        if not isinstance(logger, cls):
            logger.__class__ = cls
            logger.log_dir = log_dir
            logger.log_name = log_name
            logger.current_date = None
            logger.file_handler = None
            logger.handler_lock = threading.Lock()
            logger.prefix = prefix
            logger.update_handler()
        logger.setLevel(level)
        return logger
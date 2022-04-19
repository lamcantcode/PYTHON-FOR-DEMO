import logging
# import os
# os.environ.get('LOGLEVEL', 'INFO').upper()


def get_logger(name) -> logging.Logger:
    file_formatter = logging.Formatter('[%(asctime)s] [%(thread)d] [%(levelname)s] : %(module)s.%(funcName)s : %(message)s')
    console_formatter = logging.Formatter('[%(asctime)s] [%(thread)d] [%(levelname)s] : %(module)s.%(funcName)s : %(message)s')

    file_handler = logging.FileHandler("../app_log.log", encoding='utf8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(console_formatter)

    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(1)

    return logger

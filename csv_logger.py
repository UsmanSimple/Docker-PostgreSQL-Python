import logging

format_logger = f"%(asctime)s- [%(levelname)s] %(name)s [%(filename)s.%(funcName)s:%(lineno)d]: %(message)s"

def get_file_handler():
    file_handler = logging.FileHandler('explain.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(format_logger))
    return file_handler

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler())
    return logger

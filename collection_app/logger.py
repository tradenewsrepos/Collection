import logging
import os


def create_logger(log_name):
    log_path = '../logs'
    log_filepath = os.path.join(log_path, log_name)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    handler = logging.FileHandler(log_filepath)
    handler.setFormatter(formatter)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

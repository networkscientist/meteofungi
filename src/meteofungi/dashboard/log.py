import logging

from meteofungi.dashboard import settings


def init_logging(name):
    root_logger = logging.getLogger(name)
    if root_logger.handlers:  # logger is already setup, don't setup again
        return
    root_logger.propagate = True
    log_level = logging.DEBUG if settings.get_args().debug else logging.WARNING
    root_logger.setLevel(log_level)
    formatter = logging.Formatter('%(name)s %(asctime)s %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

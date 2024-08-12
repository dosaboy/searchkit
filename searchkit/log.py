""" Searchkit logging. """
import logging

log = logging.getLogger('searchkit')
LOGFORMAT = ("%(asctime)s %(process)d %(levelname)s %(name)s [-] "
             "%(message)s")


def configure_handler():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOGFORMAT))
    log.addHandler(handler)


def set_log_level(level):
    log.setLevel(level)
    if not log.hasHandlers():
        configure_handler()


if log.level and not log.hasHandlers():
    configure_handler()

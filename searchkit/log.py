#!/usr/bin/python3
import logging

log = logging.getLogger('searchkit')
format = ("%(asctime)s.%(msecs)03d %(process)d %(levelname)s %(name)s [-] "
          "%(message)s")


def configure_handler():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(format))
    log.addHandler(handler)


def set_log_level(level):
    log.setLevel(level)
    if not log.hasHandlers():
        configure_handler()


if log.level and not log.hasHandlers():
    configure_handler()

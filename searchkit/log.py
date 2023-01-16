#!/usr/bin/python3
import logging

log = logging.getLogger()


def setup_logging(level):
    format = ("%(asctime)s.%(msecs)03d %(process)d %(levelname)s %(name)s [-] "
              "%(message)s")
    log.name = 'searchkit'
    logging.basicConfig(format=format, level=level)

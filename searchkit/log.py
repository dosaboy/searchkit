#!/usr/bin/python3
import logging

log = logging.getLogger('searchkit')
format = ("%(asctime)s.%(msecs)03d %(process)d %(levelname)s %(name)s [-] "
          "%(message)s")
if log.level and not log.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(format))
    log.addHandler(handler)

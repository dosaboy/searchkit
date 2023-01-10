#!/usr/bin/python3
import logging

log = logging.getLogger()
format = ("%(asctime)s.%(msecs)03d %(process)d %(levelname)s %(name)s [-] "
          "%(message)s")
log.name = 'searchtools'
logging.basicConfig(format=format, level=logging.DEBUG)

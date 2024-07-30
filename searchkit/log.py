#!/usr/bin/python3
import logging

def set_log_level(level):
    log.setLevel(level)
    if not log.hasHandlers():
        configure_handler(log)

def configure_handler(logger_obj):
    logformat = ("%(asctime)s %(process)d %(levelname)s %(name)s [-] "
             "%(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logformat))
    logger_obj.addHandler(handler)

def initialize_new_logger(name):
    new_logger = logging.getLogger(name)
    if not new_logger.hasHandlers():
        new_logger.setLevel(logging.INFO)
        configure_handler(logger_obj=new_logger)
    return new_logger

log = initialize_new_logger("searchkit")

if log.level and not log.hasHandlers():
    configure_handler(log)

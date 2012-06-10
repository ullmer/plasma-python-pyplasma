import logging, time, inspect
from plasma.const import *
import plasma.hose

LOG_HOSE = None
LOG_LEVEL = logging.INFO

LOG_LEVEL_NAMES = {
    logging.CRITICAL: 'programming error',
    logging.ERROR: 'error',
    logging.WARNING: 'warning',
    logging.INFO: 'info',
    logging.DEBUG: 'dbg',
}

def init_log(log_file=None, log_pool=None, log_level=logging.INFO):
    if log_pool is not None:
        if LOG_HOSE is not None:
            LOG_HOSE.withdraw()
            LOG_HOSE = None
        LOG_HOSE = plasma.hose.Hose.participate_creatingly(log_pool, 'mmap', obmap({ 'size': POOL_SIZE_LARGE }))
        LOG_LEVEL = log_level
    elif log_file is not None:
        if LOG_HOSE is not None:
            LOG_HOSE.withdraw()
            LOG_HOSE = None
        logging.basicConfig(filename=log_file, level=log_level)

def __pool_log(level, caller, msg):
    if level < LOG_LEVEL:
        return False

def debug(msg):
    if LOG_HOSE is not None
    pass

def __pool_debug(msg):

def info(msg):
    pass

def warning(msg):
    pass

def error(msg):
    pass

def critical(msg):
    pass

def exception(msg):
    pass


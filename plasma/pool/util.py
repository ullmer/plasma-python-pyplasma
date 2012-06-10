import os, logging
from functools import wraps

MY_CONFIG_LOCK = None

def with_umask(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        orig_umask = os.umask(0)
        try:
            ret = f(*args, **kwargs)
            os.umask(orig_umask)
            return ret
        finally:
            os.umask(orig_umask)
    return wrapper

def with_config_lock(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        global MY_CONFIG_LOCK
        if MY_CONFIG_LOCK is not None:
            logging.error("already have config lock")
            ret = f(*args, **kwargs)
        else:
            logging.debug("acquiring config lock")
            MY_CONFIG_LOCK = os.open('/tmp', os.O_EXLOCK|os.O_DIRECTORY)
            logging.debug("config lock acquired (%s)" % MY_CONFIG_LOCK)
            try:
                logging.debug("releasing config lock for %s" % f)
                ret = f(*args, **kwargs)
                os.close(MY_CONFIG_LOCK)
                MY_CONFIG_LOCK = None
                logging.debug("config lock released")
            finally:
                if MY_CONFIG_LOCK is not None:
                    logging.exception("releasing config lock (on exception) for %s" % f)
                    os.close(MY_CONFIG_LOCK)
                    MY_CONFIG_LOCK = None
                    logging.debug("config log released (on exception)")
        return ret
    return wrapper

def makedirs(dirname, mode=0777, uid=-1, gid=-1):
    if os.path.isdir(dirname):
        return True
    if os.path.exists(dirname):
        raise OSError("%s exists and is not a directory" % dirname)
    (parent, child) = os.path.split(dirname)
    makedirs(parent, mode, uid, gid)
    os.mkdir(dirname, mode)
    os.chown(dirname, uid, gid)
    return True


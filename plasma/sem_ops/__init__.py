import sys, math, logging, random, traceback
from plasma.exceptions import *
from plasma.sem_ops.const import *
import plasma.sem_ops.c

POOL_SEM_SET_COUNT = 2
POOL_SEM_DEPOSIT_LOCK_IDX = 0
POOL_SEM_NOTIFICATION_LOCK_IDX = 1

class SemaphoreSet(object):
    """
    Example usage:

    from plasma.sem_ops import Semaphore

    if getattr(pool, 'sem', None) is None:
        key = pool.get_sem_key()
        pool.sem = Semaphore(key)
        if pool.sem.created:
            pool.write_config_file()
    """

    def __init__(self, key=None):
        """
        If key is None, a new semaphore set will be created
        with a randomly chosen key.  If key is not None, then
        a semaphore set will be opened with that key.  If the
        semaphore specified by key doesn't already exist, it
        will be created.  If a semaphore set was created, the
        created attribute will be set to True on the object.
        """
        self.__deposit_lock = False
        self.__notification_lock = False
        self.created = False
        if key is None:
            self.__create_semaphores()
        else:
            self.__open_semaphores(key)

    def key(self):
        return self.__key

    def id(self):
        return self.__id

    def has_deposit_lock(self):
        return self.__deposit_lock

    def has_notification_lock(self):
        return self.__notification_lock

    def __random_sem_key(self):
        bites = list(random.randint(0, 255) for i in range(3))
        return 0x0b800000 | (bites[0] << 16) | (bites[1] << 8) | bites[2]

    def __create_semaphores(self):
        semkey = self.__random_sem_key()
        while True:
            try:
                #mode = 0666
                mode = 0o666
                id = plasma.sem_ops.c.semget(semkey, nsems=POOL_SEM_SET_COUNT, semflg=mode|IPC_CREAT|IPC_EXCL)
                self.__key = semkey
                self.__id = id
                self.created = True
                break
            except SemExistsException:
                semkey = self.__random_sem_key()
            except SemNoSpaceException:
                logging.exception("This means you need to increase the number of semaphores.\nhttps://wiki.oblong.com/post/main/08TFFxFCpaqEGXTt\nTry doing this:\nsudo sh -c \"echo 250 65536 32 32768 > /proc/sys/kernel/sem\"\n")
                raise
            except SemInterruptException:
                pass
        self.__setval(POOL_SEM_DEPOSIT_LOCK_IDX, 1)
        self.__setval(POOL_SEM_NOTIFICATION_LOCK_IDX, 1)

    def __open_semaphores(self, key):
        #mode = 0666
        mode = 0o666
        try:
            id = plasma.sem_ops.c.semget(key, nsems=0, semflg=mode)
            self.__key = key
            self.__id = id
        except SemDoesNotExistException:
            self.__create_semaphores()
            logging.info("recreated semaphore")

    def __random_delay(self, lo, hi):
        time.sleep(random.uniform(lo, hi))

    def __recreate_maliciously_deleted_semaphore(self):
        #mode = 0666
        mode = 0o666
        old_sem_id = self.__id
        while True:
            try:
                new_sem_id = plasma.sem_ops.c.semget(self.__key, POOL_SEM_SET_COUNT, mode|IPC_CREAT|IPC_EXCL)
                self.__id = new_sem_id
            except SemExistsException:
                self.__random_delay((1+math.sqrt(5))/2, math.pi)
                try:
                    new_sem_id = plasma.sem_ops.c.semget(self.__key, POOL_SEM_SET_COUNT, mode|IPC_CREAT|IPC_EXCL)
                    self.__id = new_sem_id
                except SemInterruptException:
                    pass
            except SemInterruptException:
                pass
        self.__setval(POOL_SEM_DEPOSIT_LOCK_IDX, 1)
        self.__setval(POOL_SEM_NOTIFICATION_LOCK_IDX, 1)
        logging.info("Recreated a maliciously deleted semaphore:\nsem_key = '%s'\nold sem_id = 0x%08x\nnew sem_id = 0x%08x\nThis is NOT supposed to happen and is NOT a good thing, and various unpleasantness like race conditions might occur.  If you are randomly deleting semaphores on your machine for the fun of it, you are advised to not do that!")

    def __semop(self, sops):
        while True:
            try:
                return plasma.sem_ops.c.semop(self.__id, sops)
            except SemInterruptException:
                pass

    def __setval(self, idx, val):
        while True:
            try:
                return plasma.sem_ops.c.semctl(self.__id, idx, SETVAL, { 'val': val })
            except SemInterruptException:
                pass

    def __getval(self, idx):
        while True:
            try:
                return plasma.sem_ops.c.semctl(self.__id, idx, GETVAL)
            except SemInterruptException:
                pass

    def destroy(self):
        """
        Removes the semaphore from the system.  Only use this
        when disposing of a pool.  Otherwise, other hoses using
        this pool will complain about their semaphores being
        maliciously deleted.
        """
        while True:
            try:
                return plasma.sem_ops.c.semctl(self.__id, 0, IPC_RMID, { 'val': 0 })
            except SemInterruptException:
                pass
            except:
                logging.exception("semaphore destroy failed")
                raise

    def __idx_name(self, idx):
        if idx == POOL_SEM_DEPOSIT_LOCK_IDX:
            return 'deposit'
        if idx == POOL_SEM_NOTIFICATION_LOCK_IDX:
            return 'notification'
        return 'unknown'

    def __lock(self, idx):
        lock_op = {
            'sem_op': -1,
            'sem_num': idx,
            'sem_flg': SEM_UNDO
        }
        try:
            count = self.__getval(idx)
        except SemInvalidException:
            ## semaphore deleted after hose was opened
            self.__recreate_maliciously_deleted_semaphore()
        try:
            ret = self.__semop([lock_op,])
        #except (SemInvalidException, SemDoesNotExistException), e:
        except (SemInvalidException, SemDoesNotExistException) as e:
            logging.exception("%s lock semop failed with %s, which probably means somebody deleted a pool you were still using" % (self.__idx_name(idx), type(e).__name__))
            raise
        #except SemNoSpaceException, e:
        except SemNoSpaceException as e:
            logging.exception("%s lock semop failed with %s, which probably means you have too many concurrent locks in this process" % (self.__idx_name(idx), type(e).__name__))
            raise
        try:
            count = self.__getval(idx)
        except:
            logging.exception("%s lock semctl failed with %s" % (self.__idx_name(idx), type(sys.exc_value).__name__))
            raise
        if count != 0:
            raise LockingException("%s locked, but count shows %d (should be 0)" % (self.__idx_name(idx), count))

    def __unlock(self, idx):
        unlock_op = {
            'sem_op': 1,
            'sem_num': idx,
            'sem_flg': SEM_UNDO
        }
        try:
            count = self.__getval(idx)
        except:
            logging.exception("%s unlock semctl failed with %s" % (self.__idx_name(idx), type(sys.exc_value).__name))
        if count != 0:
            raise LockingException("%s lock count %d (should be 0)" % (self.__idx_name(idx), count))
        try:
            self.__semop([unlock_op,])
        #except (SemInvalidException, SemDoesNotExistException), e:
        except (SemInvalidException, SemDoesNotExistException) as e:
            logging.exception("%s unlock semop failed with %s, which probably means somebody deleted a pool you were still using" % (self.__idx_name(idx), type(e).__name__))
            raise
        #except SemNoSpaceException, e:
        except SemNoSpaceException as e:
            logging.exception("%s lock semop failed with %s, which probably means you have too many concurrent locks in this process" % (self.__idx_name(idx), type(e).__name__))
            raise
        try:
            count = self.__getval(idx)
        except:
            logging.exception("%s unlock semctl failed with %s" % (self.__idx_name(idx), type(sys.exc_value).__name__))
            raise
        if count > 1:
            raise LockingException("%s lock has a count > 1 (%d)" % (self.__idx_name(idx), count))

    def has_deposit_lock(self):
        return self.__deposit_lock

    def deposit_lock(self):
        """
        Locks (decrements) the deposit semaphore.  If the lock
        is already held, nothing will happen here.  If the
        notification lock is already held, this will raise an
        exception, as that locking order could lead to deadlock.
        Returns True on success, or raises an exception on error.
        """
        if self.__deposit_lock:
            ## we already hold the deposit lock
            logging.error('already holding deposit lock')
            return True
        if self.__notification_lock:
            ## we should always acquire locks in the same order
            ## to avoid deadlock.  I'm going to assume that we would
            ## always do the deposit before the notify
            raise LockingException("You may not acquire the deposit lock while holding the notification lock.  This could result in a deadlock situation.")
        logging.debug('acquiring deposit lock')
        self.__lock(POOL_SEM_DEPOSIT_LOCK_IDX)
        self.__deposit_lock = True
        return True

    def deposit_unlock(self):
        """
        Unlocks (increments) the deposit semaphore.  If the
        lock is not already held, you'll get a warning in the
        log, and this will return False.  Otherwise, it will
        return True.
        """
        if not self.__deposit_lock:
            logging.error("attempting to release the deposit lock, but you don't already hold it")
            traceback.print_stack()
            return False
        logging.debug('releasing deposit lock')
        self.__unlock(POOL_SEM_DEPOSIT_LOCK_IDX)
        self.__deposit_lock = False
        return True

    def has_notification_lock(self):
        return self.__notification_lock

    def notification_lock(self):
        """
        Locks (decrements) the notification semaphore.  If the
        lock is already held, nothing will happen here.  Returns
        True on success, or raises an exception on error.
        """
        if self.__notification_lock:
            ## we already hold the notification lock
            logging.error('already holding notification lock')
            return True
        logging.debug('acquiring notification lock')
        self.__lock(POOL_SEM_NOTIFICATION_LOCK_IDX)
        self.__notification_lock = True
        return True

    def notification_unlock(self):
        """
        Unlocks (increments) the notification semaphore.  If
        the lock is not already held, you'll get a warning in
        the log, and this will return False.  Otherwise, it
        will return True.
        """
        if not self.__notification_lock:
            logging.error("attempting to release the notification lock, but you don't already hold it")
            traceback.print_stack()
            return False
        logging.debug('releasing notification lock')
        self.__unlock(POOL_SEM_NOTIFICATION_LOCK_IDX)
        self.__notification_lock = False
        return True


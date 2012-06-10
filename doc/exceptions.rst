**********
Exceptions
**********

libLoam and libPlasma both define a set of exceptions.  You can import them all with::

  from plasma.exceptions import *

===============
Base Exceptions
===============

.. autoclass:: loam.exceptions.ObException

.. autoclass:: loam.exceptions.ObErrnoException
   :show-inheritance:

.. autoclass:: loam.exceptions.LoamException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PlasmaException
   :show-inheritance:

.. autofunction:: plasma.exceptions.get_retort_exception

==================
libLoam Exceptions
==================

.. autoclass:: loam.exceptions.ObNoMemException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObBadIndexException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObArgumentWasNullException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObNotFoundException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObInvalidArgumentException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObUnknownErrException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObInadequateClassException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObAlreadyPresentException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObEmptyException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObInvalidOperationException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObDisconnectedException
   :show-inheritance:

.. autoclass:: loam.exceptions.ObVersionMismatchException
   :show-inheritance:

---------------
Slaw Exceptions
---------------


.. autoclass:: loam.exceptions.SlawCorruptProteinException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawCorruptSlawException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawFabricatorBadnessException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawNotNumericException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawRangeErrException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawUnidentifiedSlawException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawWrongLengthException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawNotFoundException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawAliasNotSupportedException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawBadTagException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawEndOfFileException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawParsingBadnessException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawWrongFormatException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawWrongVersionException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawYamlErrException
   :show-inheritance:

.. autoclass:: loam.exceptions.SlawNoYamlException
   :show-inheritance:

====================
libPlasma Exceptions
====================

.. autoclass:: plasma.exceptions.PoolNoSuchProteinException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolAwaitTimedoutException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolAwaitWokenException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNoPoolsDirException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolFileBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNullHoseException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolMmapBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolInappropriateFilesystemException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolInUseException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolTypeBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolConfigBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolWrongVersionException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolCorruptException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolPoolnameBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolImpossibleRenameException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolFifoBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolInvalidSizeException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNoSuchPoolException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolExistsException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolIllegalNestingException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolProtocolErrorException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolWakeupNotEnabledException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolProteinBiggerThanPoolException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolFrozenException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolFullException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNotAProteinException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNotAProteinOrMapException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolConfWriteBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolConfReadBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolSendBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolRecvBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolSockBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolServerBusyException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolServerUnreachException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolAlreadyGangMemberException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNotAGangMemberException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolEmptyGangException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolNullGangException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolUnsupportedOperationException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolInvalidatedByForkException
   :show-inheritance:

--------------
TLS Exceptions
--------------


.. autoclass:: plasma.exceptions.PoolNoTlsException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolTlsRequiredException
   :show-inheritance:

.. autoclass:: plasma.exceptions.PoolTlsErrorException
   :show-inheritance:

--------------------
Semaphore Exceptions
--------------------

.. autoclass:: plasma.exceptions.PoolSemaphoresBadthException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemPermissionException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemExistsException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemInvalidException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemDoesNotExistException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemNoSpaceException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemInterruptException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemRangeException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemUndoException
   :show-inheritance:

.. autoclass:: plasma.exceptions.SemTooBigException
   :show-inheritance:



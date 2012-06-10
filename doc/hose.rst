*****
Hoses
*****

Pool hoses are the primary objects for interacting with a pool.  They hold
all of the state information associated with a pool connection, have methods
for querying information about the pool, and methods for reading to and writing
from the pool.

.. autoclass:: plasma.hose.Hose
   :show-inheritance:
   :members: __new__

Creation and Disposal
=====================

.. currentmodule:: plasma.hose
.. py:class:: Hose

   .. automethod:: create
   .. automethod:: dispose
   .. automethod:: rename
   .. automethod:: exists
   .. automethod:: validate_name
   .. automethod:: sleep
   .. automethod:: check_in_use

Connecting and Disconnecting
============================

.. py:class:: Hose

   .. automethod:: participate
   .. automethod:: participate_creatingly
   .. automethod:: withdraw

Pool and Hose Information
=========================

.. py:class:: Hose

   .. automethod:: list_pools
   .. automethod:: list_ex
   .. automethod:: name
   .. automethod:: get_hose_name
   .. automethod:: set_hose_name
   .. automethod:: get_info
   .. automethod:: newest_index
   .. automethod:: oldest_index

Depositing (Writing) to Pools
=============================

.. py:class:: Hose

   .. automethod:: deposit
   .. automethod:: deposit_ex

Reading from Pools
==================

.. py:class:: Hose

   .. automethod:: curr
   .. automethod:: prev
   .. automethod:: next
   .. automethod:: fetch
   .. automethod:: nth_protein
   .. automethod:: index_lookup
   .. automethod:: probe_back
   .. automethod:: probe_frwd
   .. automethod:: await_next
   .. automethod:: await_probe_frwd
   .. automethod:: enable_wakeup
   .. automethod:: wake_up

Plasma++ Methods
================

.. py:class:: Hose

   .. automethod:: IsConfigured
   .. automethod:: LastRetort
   .. automethod:: Withdraw
   .. automethod:: Deposit
   .. automethod:: Next
   .. automethod:: Current
   .. automethod:: Previous
   .. automethod:: Nth
   .. automethod:: ProbeForward
   .. automethod:: ProbeBackward
   .. automethod:: EnableWakeup
   .. automethod:: WakeUp
   .. automethod:: CurrentIndex
   .. automethod:: OldestIndex
   .. automethod:: NewestIndex
   .. automethod:: SeekTo
   .. automethod:: SeekToTime
   .. automethod:: SeekBy
   .. automethod:: SeekByTime
   .. automethod:: ToLast
   .. automethod:: Runout
   .. automethod:: Rewind
   .. automethod:: PoolName
   .. automethod:: Name
   .. automethod:: SetName
   .. automethod:: ResetName

Examples
========

Creating a pool::

   from loam import *
   from plasma.protein import Protein
   from plasma.const import *
   from plasma.exceptions import *
   from plasma.hose import Hose
   pool = 'local-pool'
   pool_options = obmap({ 'size': unt64(1024*100),
                          'index-capacity': unt64(100) })
   Hose.create(pool, 'mmap', pool_options)
   hose = hose.participate(pool)

or::

   from loam import *
   from plasma.protein import Protein
   from plasma.const import *
   from plasma.exceptions import *
   from plasma.hose import Hose
   pool = 'local-pool'
   pool_options = obmap({ 'size': unt64(1024*100),
                          'index-capacity': unt64(100) })
   hose = hose.participate_creatingly(pool, 'mmap', pool_options)

A basic listener example::

   from loam import *
   from plasma.protein import Protein
   from plasma.const import *
   from plasma.exceptions import *
   from plasma.hose import Hose
   pool = 'local-pool'
   pool_options = obmap({ 'size': unt64(1024*100),
                          'index-capacity': unt64(100) })
   Hose.create(pool, 'mmap', pool_options)
   hose = Hose.participate(pool)
   hose.deposit(Protein(descrips=['some', 'identifiers'],
                        ingests={ 'key': 'value', 'foo': [1, 2, 3] }))
   while True:
       try:
           p = hose.await_next(timeout=60)
       except PoolAwaitTimedoutException:
           break
       print '%s' % p.to_json()



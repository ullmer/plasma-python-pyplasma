*************
Complex Types
*************

obcons
======

obcons behaves like a python 2-tuple.

.. autoclass:: loam.obstruct.obcons
   :show-inheritance:
   :members: to_slaw, __add__, __mul__, __rmul__
   :undoc-members:

oblist
======

oblist behaves like a python list.

.. autoclass:: loam.obstruct.oblist
   :show-inheritance:
   :members: __setitem__, __setslice__, __getslice__, __add__, __iadd__, __imul__, __mul__, __rmul__, append, extend, insert, search_ex, gapsearch, contigsearch, to_slaw
   :undoc-members:

obmap
=====

obmap behaves like a python dict.

.. autoclass:: loam.obstruct.obmap
   :show-inheritance:
   :members: __setitem__, update, items, iteritems, keys, values, to_slaw
   :undoc-members:


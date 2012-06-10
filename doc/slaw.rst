*****
Slawx
*****

Slawx (plural for "slaw") are the lowest level of libPlasma. The term slaw encapsulates the idea of a self-describing, highly structured, strongly typed (but arbitrarily composed) data unit. A slaw can be an unsigned 64-bit integer, a complex number, a vector, a string, or a list. A more complete list of types implemented as slawx can be found in :ref:`libLoam <loam>`.

For our purposes, you don't really need to know much about slaw, other than that it is a binary packing format for passing around data.  The plasma.slaw module provides a few functions for reading and writing slaw data.

.. automodule:: plasma.slaw
   :show-inheritance:
   :members: read_slaw_file, read_slaw_fh, write_slaw_file, write_slaw_fh, parse_slaw, parse_slaw_data, parse_slaw1, parse_slaw2

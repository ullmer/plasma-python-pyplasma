.. _loam:

****
Loam
****

All loam objects have a to_slaw method that returns a binary representation of the object that can be written to pools, hoses, protein files, etc, to be used by other plasma-based programs.

.. method:: to_slaw(version=2)

   Pack the object into a string of binary data to be passed around

.. toctree::
   :maxdepth: 2

   obsimple.rst
   obnum.rst
   obvect.rst
   obstr.rst
   obstruct.rst
   obtime.rst

.. pyplasma documentation master file, created by
   sphinx-quickstart on Mon Jun 11 17:35:53 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyplasma's documentation!
====================================

Contents:

.. toctree::
   :maxdepth: 2

   loam.rst
   plasma.rst
   exceptions.rst

Getting Started
===============

This is a work in progress, and written by somebody that doesn't do much C/C++, so there may be bits that are poorly or incorrectly implemented.  That said:

What Works
----------

*Slaw (Version 2)*
  Reading and writing of version 2 binary slaw are 99% implemented.  The only limitation remaining is with "non-standard proteins," which are mentioned in the format specification, but not described.

*TCP Pools*
  The full TCP protocol is implemented (including TLS support), and works with the C-based pool_tcp_server.  There does appear to be a small bug with hose disconnect protocol, as the pool_tcp_server emits error messages when pyplasma calls withdraw.  This seems a minor issue.

What Mostly Works
-----------------

*Reading from MMap Pools*
  In the conditions I've tested, this works fantastically, and performance is almost as good as with the pool_tcp_server on localhost.  However, test coverage is pretty poor at this point, so there may edge cases that I haven't run into yet.  In any case, reading from a pool won't cause corruption, so this is pretty safe to use.

What Might Not Work
-------------------

*Slaw (Version 1)*
  This is simply due to a lack of test data.  The only version 1 slaw I have to test with is the TCP handshake message.  If you have version 1 slaw files that I can write some tests against, I'd be happy to promote this to "Works".

*Depositing (Writing) to MMap Pools*
  This actually works quite well, but it's one of those things that could cause major problems if it doesn't work 100% perfectly.  For now, writing to mmap pools should be considered dangerous, so do this at your own risk.

What Doesn't Work
-----------------

*Resizing MMap Pools*
  This is very much broken.  In fact, using pyplasma to resize an mmap pool will simply raise an exception rather than try end end up messing up your pool.  Furthermore, using one of the C tools (like the TCP server, for instance) to resize a pool that pyplasma has open will likely cause pyplasma to destroy your pool.  So don't do that.

*TCP Server*
  I've started work on a pyplasma implementation of the pool TCP server.  It is very definitely a work in progress, so don't bother trying to get it to work quite yet.  The same goes for most of the plasma command line tools.  There are stubs for pyplasma versions, but they are in no way ready for use.

  There are a couple of command line utilities that are ready to use.  py-ob-version is a pyplasma utility similar to ob-version, and pyplasma-benchmark is a tool for testing the speed of pyplasma reads and writes.  Run pyplasma-benchmark --help for options.

Building and Installing
=======================

As with most python packages, this package comes with a setup.py script.  To build and install the package, run:

  python setup.py install

To run the unit tests, run:

  python setup.py test

And to build this documentation (which will end up in doc/_build/html), run:

  python setup.py doc

Prerequisites
-------------

This library was developed using Python 2.7, but it may work on older versions.  The only limitation I'm aware of is that TLS/SSL support requires at least Python 2.6.

The only nonstandard library required for pyplasma is the pybonjour library, but even this is only needed if you want to use zeroconf to advertise or discover pool servers.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


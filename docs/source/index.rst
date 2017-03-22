.. ont_fast5_api documentation master file, created by
   sphinx-quickstart on Fri Nov 21 09:32:46 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Oxford Nanopore Technologies fast5 API software
======================================================

This project provides classes and utility functions for working with read fast5
files. It provides an abstraction layer between the underlying h5py library and
the various concepts central to read fast5 files, such as "reads", "analyses",
"analysis summaries", and "analysis datasets". Ideally all interaction with a
read fast5 file should be possible via this API, without having to directly
invoke the h5py library.

Contents:

.. toctree::
   :maxdepth: 4
   :glob:

   ont_fast5_api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


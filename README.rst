.. image:: img/ONT_logo.png
  :width: 800
  :alt:  .


ont_fast5_api
===============================================================================

``ont_fast5_api`` is a simple interface to HDF5 files of the Oxford Nanopore
fast5 file format.

- Source code: https://github.com/nanoporetech/ont_fast5_api
- Fast5 File Schema: https://github.com/nanoporetech/ont_h5_validator

It provides:

- Concrete implementation of the fast5 file schema using the generic h5py library
- Plain-english-named methods to interact with and reflect the fast5 file schema
- Tools to convert between `multi_read` and `single_read` formats

Getting Started
===============================================================================
The ``ont_fast5_api`` is available on PyPI and can be installed via pip::

    pip install ont-fast5-api

Alternatively, it is available on github where it can be built from source::

    git clone https://github.com/nanoporetech/ont_fast5_api
    cd ont_fast5_api
    python setup.py install

Dependencies
-------------------------------------------------------------------------------
``ont_fast5_api`` is a pure python project and should run on most python
versions and operating systems.

It requires:

- `h5py <http://www.h5py.org>`_: 2.2.1 or higher
- `NumPy <https://www.numpy.org>`_: 1.8.1 or higher
- `six <https://github.com/benjaminp/six>`_: 1.9 or higher
- `progressbar33 <https://github.com/germangh/python-progressbar>`_: 2.3.1 or higher


Interface - Console Scripts
===============================================================================
The ``ont_fast5_api`` provides terminal/command-line ``console_scripts`` for
converting between files in the Oxford Nanopore ``single_read`` and
``multi_read`` fast5 formats. These are provided to ensure compatibility between
tools which expect either the ``single_read`` or ``multi_read`` fast5 file
formats.

The scripts are added during installation and can be called from the
terminal/command-line or from within python.

single_to_multi_fast5
-------------------------------------------------------------------------------
This script converts folders containing ``single_read_fast5`` files into
``multi_read_fast5_files``::

    single_to_multi_fast5
        -i, --input_path <(path) folder containing single_read_fast5 files>
        -s, --save_path <(path) to folder where multi_read fast5 files will be output>
        [optional] -f, --filename_base <(string) name for new multi_read file; default="batch" (see note-1)>
        [optional] -n, --batch_size <(int) number of single_reads to include in each multi_read file; default=4000>
        [optional] --recursive <(bool) if included, rescursively search sub-directories for single_read files; default=False>

*note-1:* newly created ``multi_read`` files require a name. This is the
``filename_base`` with the batch count and ``.fast5`` appended to it; e.g.
``-f batch`` yields ``batch_0.fast5, batch_1.fast5, ...``

**example usage**::

    single_to_multi_fast5 --input_path /data/reads --save_path /data/multi_reads
        --filename_base batch_output --batch_size 100 --recursive

Where ``/data/reads`` and/or its subfolders contain ``single_read`` fast5
files. The output will be ``multi_read`` fast5 files each containing 100 reads,
in the folder: ``/data/multi_reads`` with the names: ``batch_output_0.fast5``,
``batch_output_1.fast5`` etc.

multi_to_single_fast5
-------------------------------------------------------------------------------
This script converts folders containing ``multi_read_fast5`` files into
``single_read_fast5`` files::

    multi_to_single_fast5
        -i, --input_path <(path) folder containing multi_read_fast5 files>
        -s, --save_path <(path) to folder where single_read fast5 files will be output>
        [optional] -n, --batch_size <(int) number of single_reads to include in each output folder; default=4000 (see note-2)>
        [optional] --recursive <(bool) if included, rescursively search sub-directories for multi_read files; default=False>

*note-2:* ``single_read`` fast5 files are batched into subdirectories for
output for performance reasons

**example usage**::

    multi_to_single_fast5 --input_path /data/multi_reads --save_path /data/single_reads
        --batch_size 100 --recursive

Where ``/data/multi_reads`` and/or its subfolders contain ``multi_read``  fast5
files. The output will be ``single_read`` fast5 files, in subfolders of the
output_folder: ``/data/single_reads``, with each folder containing 100 fast5
files.

Glossary of Terms:
==============================================================================

**HDF5 file format** - a portable file format for storing and managing
data. It is designed for flexible and efficient I/O and for high volume and
complex data
**Fast5** - an implementation of the HDF5 file format, with specific data
schemas for Oxford Nanopore sequencing data
**Single read fast5** - A  fast5 file containing all the data pertaining to a
single Oxford Nanopore read. This may include raw signal data, run metadata,
fastq-basecalls and any other additional analyses
**Multi read fast5** - A fast5 file containing data pertaining to a multiple
Oxford Nanopore reads.

Help
==============================================================================

Licence and Copyright:
----------------------
[c] 2018 Oxford Nanopore Technologies Ltd.

``ont_fast5_api`` is distributed under the terms of the Oxford Nanopore Technologies Developer licence.
Developer releases are provided to allow caveated access to source code and APIs for
third-party tool development and exploration. Support is provided via this Github project and/or in
the Community, here [https://community.nanoporetech.com/] and releases are accompanied by a release
note at this website.



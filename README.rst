.. image:: img/ONT_logo.png
  :width: 800
  :alt:  .


ont_fast5_api
===============================================================================

``ont_fast5_api`` is a simple interface to HDF5 files of the Oxford Nanopore
.fast5 file format.

- Source code: https://github.com/nanoporetech/ont_fast5_api
- Fast5 File Schema: https://github.com/nanoporetech/ont_h5_validator

It provides:

- Concrete implementation of the fast5 file schema using the generic h5py library
- Plain-english-named methods to interact with and reflect the fast5 file schema
- Tools to convert between `multi_read` and `single_read` formats
- Tools to compress/decompress raw data in files

Getting Started
===============================================================================
The ``ont_fast5_api`` is available on PyPI and can be installed via pip::

    pip install ont-fast5-api

Alternatively, it is available on github where it can be built from source::

    git clone https://github.com/nanoporetech/ont_fast5_api
    pip install ./ont_fast5_api

Dependencies
-------------------------------------------------------------------------------
``ont_fast5_api`` is a pure python project and should run on most python
versions and operating systems.

It requires:

- `h5py <http://www.h5py.org>`_: 2.6 or higher
- `NumPy <https://www.numpy.org>`_: 1.11 or higher
- `six <https://github.com/benjaminp/six>`_: 1.10 or higher
- `progressbar33 <https://github.com/germangh/python-progressbar>`_: 2.3.1 or higher

Interface - get_fast5_file
===============================================================================

The ont_fast5_api provides a simple interface to access the data structures in .fast5
files of either single- or multi- read format using the same method calls.

For example to print the raw data from all reads in a file::

    from ont_fast5_api.fast5_interface import get_fast5_file

    def print_all_raw_data():
        fast5_filepath = "test/data/single_reads/read0.fast5" # This can be a single- or multi-read file
        with get_fast5_file(fast5_filepath, mode="r") as f5:
            for read in f5.get_reads():
                raw_data = read.get_raw_data()
                print(read.read_id, raw_data)

Interface - Console Scripts
===============================================================================
The ``ont_fast5_api`` provides terminal/command-line ``console_scripts`` for
converting between files in the Oxford Nanopore ``single_read`` and
``multi_read`` .fast5 file formats. These are provided to ensure compatibility between
tools which expect either the ``single_read`` or ``multi_read`` .fast5 file
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
        [optional] -t, --threads <(int) number of CPU threads to use; default=1>
        [optional] -f, --filename_base <(string) name for new multi_read file; default="batch" (see note-1)>
        [optional] -n, --batch_size <(int) number of single_reads to include in each multi_read file; default=4000>
        [optional] --recursive <if included, recursively search sub-directories for single_read files>

*note-1:* newly created ``multi_read`` files require a name. This is the
``filename_base`` with the batch count and ``.fast5`` appended to it; e.g.
``-f batch`` yields ``batch_0.fast5, batch_1.fast5, ...``

**example usage**::

    single_to_multi_fast5 --input_path /data/reads --save_path /data/multi_reads
        --filename_base batch_output --batch_size 100 --recursive

Where ``/data/reads`` and/or its subfolders contain ``single_read`` .fast5
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
        [optional] -t, --threads <(int) number of CPU threads to use; default=1>
        [optional] --recursive <if included, recursively search sub-directories for multi_read files>

**example usage**::

    multi_to_single_fast5 --input_path /data/multi_reads --save_path /data/single_reads
        --recursive

Where ``/data/multi_reads`` and/or its subfolders contain ``multi_read``  .fast5
files. The output will be ``single_read`` .fast5 files in the folder
``/data/single_reads`` with one subfolder per ``multi_read`` input file

fast5_subset
-------------------------------------------------------------------------------
This script extracts reads from ``multi_read_fast5_file(s)`` based on a list of read_ids::

    fast5_subset
        -i, --input <(path) to folder containing multi_read_fast5 files or an individual multi_read_fast5 file> 
        -s, --save_path <(path) to folder where multi_read fast5 files will be output>
        -l,--read_id_list <(file) either sequencing_summary.txt file or a file containing a list of read_ids>
        [optional] -f, --filename_base <(string) name for new multi_read file; default="batch" (see note-1)>
        [optional] -n, --batch_size <(int) number of single_reads to include in each multi_read file; default=4000>
        [optional] --recursive <if included, recursively search sub-directories for single_read files>

**example usage**::

    fast5_subset --input /data/multi_reads --save_path /data/subset
        --read_id_list read_id_list.txt --batch_size 100 --recursive

Where ``/data/multi_reads`` and/or its subfolders contain ``multi_read`` .fast5
files and ``read_id_list.txt`` is a text file either containing 1 read_id per line
or a tsv file with a column named ``read_id``.
The output will be ``multi_read`` .fast5 files each containing 100 reads,
in the folder: ``/data/multi_reads`` with the names: ``batch_output_0.fast5``,
``batch_output_1.fast5`` etc.

compress_fast5
-------------------------------------------------------------------------------
This script copies and converts raw data between `vbz` and `gzip` compression formats::

    compress_fast5
        -i, --input_path <(path) folder containing multi_read_fast5 files>
        -s, --save_path <(path) to folder where single_read fast5 files will be output>
        -c, --compression <(str) [vbz, gzip] target compression format>
        [optional] -t, --threads <(int) number of CPU threads to use; default=1>
        [optional] --recursive <if included, recursively search sub-directories for fast5 files>

**example usage**::

    compress_fast5 --input_path /data/uncompressed_reads --save_path /data/compressed_reads
        --compression vbz --recursive --threads 40

Where ``/data/uncompressed_reads`` and/or its subfolders contain .fast5 files. The output will be a copy of the input
folder structure containing compressed reads preserving both the folder structure and file type.

NB `compress_fast5` will copy .fast5 files in order to compress them due to HDF5 implementation constraints.
Further detail of HDF5 data management strategies can be found:
https://support.hdfgroup.org/HDF5/doc/Advanced/FileSpaceManagement/FileSpaceManagement.pdf


VBZ Compression
==============================================================================
VBZ compression is a compression algorithm developed by Oxford Nanopore to reduce file size and improve read/write
performance when handling raw data in Fast5 files. Previously, the default compression was GZIP and comparing to GZIP
we see a compression improvement of >30% and a CPU performance improvement of >10X for compression and >5X for
decompression. Further details of the implementation and benchmarks can be found here:
https://github.com/nanoporetech/vbz_compression

Benchmarking the performance of compression within the ont_fast5_api against a normal file copy showed
compressing from `gzip` to `vbz` was approximately 2x slower than copying files.  In other words, if it would take two
hours to copy a set of files from an input folder to an output folder then it should take four hours to compress those
files with VBZ. Running the script without compressing (i.e. the same type of compression in and out; gzip->gzip)
was approximately 2x faster than a file copy since it can utilise mutiple threads.


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

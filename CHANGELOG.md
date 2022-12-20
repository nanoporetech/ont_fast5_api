# Changelog
All notable changes and fixes to ont_fast5_api will be documented here

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
This project (aspires to) adhere to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [4.1.1]

### Fixed
- Compatibility with numpy==1.24 unicode type

### Changed
- Updated Windows VBZ Plugin dll

## [4.1.0]

### Added
- Support for fast5_api on macOS-M1

## [4.0.2]

### Fixed
- Fixed Fast5Read import error

## [4.0.1]

### Changed
- Fixed unresolved reference in `compress_fast5.py`
- Fixed issue with `compress_fast5.py` not retaining enumeration metadata for the end_reason attribute
- Increased minimum h5py version to 2.10

## [4.0.0]

### Added
- Script `demux_fast5` for demultiplexing fast5 reads based on column in summary file, e.g. for barcoded experiments 

### Removed
- Removed deb builds which are no longer supported
- Python3.5 support 

## [3.3.0] 2021-02-17

### Added
- Added `yield_fast5_reads` to conversion_tools.

## [3.2.0] 2021-01-28

### Changed
- Dropped support for older h5py/numpy versions, min now h5py>=2.8, numpy>=1.16
- fast5_subset now displays errors (but continues processing) when it encounters input fast5 files it can't read.

### Added
- Add support for explicitly specifying file drivers when loading
  multi-read fast5 files.

## [3.1.6] 2020-08-20
### Added
- `compress_fast5` now has a `--sanitize` option to remove optional groups.

### Fixed
- Correctly handle the case where h5pl can be imported but doesn't have the prepend() function available.

## [3.1.5] 2020-06-15
### Added
- Added explicit requirements and checks to prevent running on Python 2.

## [3.1.4] 2020-06-12
### Fixed
- Compression now works in `single_to_multi`.

## [3.1.3] 2020-05-28
### Fixed
- Compression argument in `fast5_subset` and `single_to_multi` failed if not set

## [3.1.2] 2020-05-04
### Fixed
- Compression argument in `fast5_subset` and `single_to_multi` was parsed incorrectly

## [3.1.1] 2020-04-03
### Fixed
- Argument list for `fast5_subset` and `single_to_multi` had a syntax error

## [3.1.0] 2020-04-02
### Added
- Hardlinking of metadata to prevent duplication and reduce filesize
- Ability to enable compression when using `fast5_subset` and `single_to_multi`
### Fixed
- `fast5_subset` thread pool could sometimes close before all tasks were completed
- `fast5_subset` will create output directory if it doesn't exist

## [3.0.2] 2020-03-17
### Fixed
- Comparison of file_versions could throw an error

## [3.0.1] 2020-01-29
### Fixed
- Basecall1DTools could not load data from a Fast5Read

## [3.0.0] 2020-01-20
### Removed
- python2 compatibility
### Fixed
- minor documentation errors: https://github.com/nanoporetech/ont_fast5_api/issues/28

## [2.1.0] 2019-12-16 
### Added
- Script to check the compression type of fast5 files in a folder
- `compress_fast5` can now be used `--in_place`
### Fixed
- Reading arrays with padded strings now succeeds (on h5py>2.7)
- Compatibility bugs with h5py==2.6 now raises appropriate errors
- Fast5File now has attribute read_id to match documentation
### Changed
- Now use standard settings for gzip compression (gzip=1, shuffle=None)
- Inverted dependency between `Fast5File` and `Fast5Read` so `Fast5Read` is now the primary object

## [2.0.1] 2019-11-28
### Added
- Option to `--ignore_symlinks` in fast5 conversion scripts
- Explicit check to file_type for detemining single/multi-read files 
### Fixed
- `fast5_subset` with single read fast5s was failing
- unit test data now cleaned up properly

## [2.0.0] 2019-11-19
### Added 
- Compatibility for VBZ compressed reads
- `compress_fast5` script for compressing/decompressing fast5 files
- `get_reads()` helper method to more easily loop through reads in a fast5 file
### Changed
- `Fast5File().get_raw_data()` updated interface to match `Fast5Read` and remove support for legacy files with multiple read numbers in a single `Fast5File`
- Minimum depedency version requirements bumped. Set to Ubuntu16 `apt` python3-package defaults 
### Removed 
- Legacy `Fast5Writer` object. `MultiReadFast5` or `EmptyFast5File` are preferred 

## [1.4.9] 2019-11-01
### Added
- Check for progressbar2 package and fail early if it's installed.

## [1.4.8] 2019-10-22
### Added
- Support for h5py==2.10 string data type encoding changes
### Fixed
- Corrected some "for for" typos in argparse help text.

## [1.4.7] 2019-07-29
### Fixed
- Bug in read string and read_id concatenation resulted in broken output file

## [1.4.6] 2019-07-03
### Added
- Updated fast5_subset script to extract also from single-read fast5 files
### Changed
- Renamed fast5_subset source script from multi_fast5_subset.py to fast5_subset.py

## [1.4.5] 2019-07-01
### Fixed
- Bug in number of processes being 0 when batch size is greater than number of reads (py2)

## [1.4.4] 2019-06-18
### Fixed
- Bug in path name output from pathlib changes

## [1.4.3] 2019-06-12
### Fixed
- Bug with apt-install and pathlib2

## [1.4.2] 2019-06-10
### Fixed
- get_raw_data() now works with scale=True when start,end are None

## [1.4.1] 2019-06-06
### Added
- Useful error message if no input files found
### Fixed
- filename_mapping output gave incorrect filenames

## [1.4.0] 2019-05-29
### Added
- Script for extracting reads by id from `multi_read` files

## [1.3.0] 2019-03-01
### Fixed
- Bug in output to `filename_mapping.txt`

## [1.2.0] 2019-01-11
### Added
- Multi-threading support for multi<->single conversion for improved performance

### Fixed
- Removed incorrect license accidentally added to README

## [1.1.1] 2019-01-10
### Changed
- Minor documentation updates
- Follow symlinks when finding files recursively

## [1.1.0] 2019-01-07
### Added
- Generic single- and multi- read interface via `get_fast5_file`

### Fixed
- Incorrect time estimates for single-multi conversion
- Fixed path creation if not exist

## [1.0.1] 2018-09-26
### Added
- Support for multi-read fast5 files
- Conversion tools for single-multi read files

### Fixed
- Support for deprecated interface to Basecall2D following 0.4.0, support will end in v1.x.x


## [0.4.0] 2017-07-16 (internal only)
### Fixed
- Basecall1d and Basecall2d raise consistent KeyError when fastq data missing

### Changed
- Interface to Basecall1d and Basecall2d unified for add_sequence() and get_sequence()


## [0.3.3] 2017-06-23
### Added
- Fast5 file now supports logging via 'Fast5File.add_log()'

### Fixed
- Invalid component names no longer checked against LEGACY_COMPENENTS
- Raise KeyError when fastq data missing from Basecall1d
- median_before and start_mux populate correctly with sensible defaults


## [0.3.2] 2017-03-22
### Added
Major release - changes not logged before this point

# Changelog
All notable changes and fixes to ont_fast5_api will be documented here

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
This project (aspires to) adhere to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## Unreleased

## [1.2.0] 2019-01-11
### Added
- Multi-threading support for multi<->single conversion for improved peformance

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

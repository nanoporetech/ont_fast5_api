from argparse import ArgumentParser

from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import yield_fast5_files
from ont_fast5_api.fast5_interface import get_fast5_file


def check_read_compression(read):
    """
    Check the compresion type on the raw data of a read
    :param read: Fast5Read object
    :return: AbstractCompression object
    """
    detected_compression = read.raw_compression_filters
    for compression in COMPRESSION_MAP.values():
        if compression.filter_settings == detected_compression:
            return compression
    return detected_compression


def check_compression(input_path, recursive, follow_symlinks, check_all_reads):
    """
    Check the compression type of the raw data in files in a folder
    :param input_path:
    :param recursive:
    :param follow_symlinks:
    :param check_all_reads: bool - check all reads in a file or just the first
    :return: (Compression, read_id, file_path)
    """
    for input_file in yield_fast5_files(input_path, recursive, follow_symlinks):
        with get_fast5_file(input_file, 'r') as f5:
            for read in f5.get_reads():
                compression = check_read_compression(read)
                yield (compression, read.read_id, input_file)
                if not check_all_reads:
                    break


def main():
    parser = ArgumentParser("Tool for checking the compression type of raw data in fast5 files")
    parser.add_argument('-i', '--input_path', required=True,
                        help="Path to Fast5 file or directory of Fast5 files")
    parser.add_argument('--check_all_reads', action='store_true', required=False, default=False,
                        help="Check all reads in a file individually (default: check only the first read)")
    parser.add_argument('-r', '--recursive', action='store_true', required=False, default=False,
                        help="Search recursively through folders for MultiRead fast5 files")
    parser.add_argument('--ignore_symlinks', action='store_true',
                        help="Ignore symlinks when searching recursively for fast5 files")
    parser.add_argument('--file_list', required=False,
                        help="File containing names of files to search in")
    args = parser.parse_args()
    compression_results = check_compression(args.input_path, args.recursive, not args.ignore_symlinks,
                                            args.check_all_reads)
    for result in compression_results:
        print(result)


if __name__ == '__main__':
    main()

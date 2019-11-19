import logging
import os
import sys
from argparse import ArgumentParser
from multiprocessing.pool import Pool

from ont_fast5_api import __version__
from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar
from ont_fast5_api.fast5_file import Fast5File, EmptyFast5
from ont_fast5_api.fast5_interface import is_multi_read
from ont_fast5_api.multi_fast5 import MultiFast5File


def makedirs(name, exist_ok):
    # This is an ugly hack which we can remove when we drop python2 support
    try:
        os.makedirs(name, exist_ok=exist_ok)
    except TypeError:
        if sys.version_info.major == 2:
            if not os.path.exists(name):
                os.makedirs(name)


def compress_batch(input_folder, output_folder, target_compression, recursive=True, threads=1):
    # We require an absolute input path to we can replicate the data structure relative to it later on
    input_folder = os.path.abspath(input_folder)

    file_list = get_fast5_file_list(input_folder, recursive)
    if len(file_list) == 0:
        raise ValueError("No input fast5 files found in '{}'. Recursive={}".format(input_folder, recursive))

    # Set up the process pool and the progressbar
    pool = Pool(min(threads, len(file_list)))
    pbar = get_progress_bar(len(file_list))

    def update(result):
        pbar.update(pbar.currval + 1)

    for input_file in file_list:
        input_path = os.path.join(input_folder, input_file)
        output_path = os.path.join(output_folder, os.path.relpath(input_path, input_folder))

        pool.apply_async(func=compress_file,
                         args=(input_path, output_path, target_compression),
                         callback=update)

    # Tear down the process pool and pbar. We can't use contextmanagers since we need to close() then join()
    pool.close()
    pool.join()
    pbar.finish()


def compress_file(input_file, output_file, target_compression):
    try:
        makedirs(os.path.dirname(output_file), exist_ok=True)
        if is_multi_read(input_file):
            with MultiFast5File(input_file, 'r') as input_f5, MultiFast5File(output_file, 'a') as output_f5:
                for read in input_f5.get_reads():
                    compress_read_from_multi(output_f5, read, target_compression)
        else:
            with Fast5File(input_file, 'r') as input_f5, \
                    EmptyFast5(output_file, 'a') as output_f5:
                compress_read_from_single(output_f5, input_f5, target_compression)
    except Exception as e:
        # Error raised in Pool.aync will be lost so we explicitly print them.
        logging.exception(e)
        raise


def compress_read_from_multi(output_f5, read_to_copy, target_compression):
    read_id = read_to_copy.get_read_id()
    read_name = "read_" + read_id
    if str(target_compression) in read_to_copy.compression_filters:
        # If we have the right compression then no need for doing anything fancy
        output_f5.handle.copy(read_to_copy.handle, read_name)
    else:
        output_f5.handle.create_group(read_name)
        output_group = output_f5.handle[read_name]
        for subgroup in read_to_copy.handle:
            if subgroup != read_to_copy.raw_dataset_group_name:
                output_group.copy(read_to_copy.handle[subgroup], subgroup)
            else:
                raw_attrs = read_to_copy.handle[read_to_copy.raw_dataset_group_name].attrs
                raw_data = read_to_copy.handle[read_to_copy.raw_dataset_name]
                output_f5.get_read(read_id).add_raw_data(raw_data, raw_attrs, compression=target_compression)


def compress_read_from_single(output_f5, read_to_copy, target_compression):
    read_id = read_to_copy.get_read_id()
    raw_dataset_name = read_to_copy.raw_dataset_name
    raw_group_name = read_to_copy.raw_dataset_group_name
    read_name = "read_" + read_id
    # Recreating the status object is painful, but doesn't actually interact with the file so we can just reference it.
    output_f5.status = read_to_copy.status

    if str(target_compression) in read_to_copy.compression_filters:
        # If we have the right compression then no need for doing anything fancy
        output_f5.handle.copy(read_to_copy.handle, read_name)
    else:
        for subgroup in read_to_copy.handle:
            if subgroup not in raw_dataset_name:
                output_f5.handle.copy(read_to_copy.handle[subgroup], subgroup)
            else:
                raw_attrs = read_to_copy.handle[raw_group_name].attrs
                raw_data = read_to_copy.handle[raw_dataset_name]
                output_f5.add_raw_data(raw_data, raw_attrs, compression=target_compression)


def main():
    parser = ArgumentParser("Tool for changing the compression of Fast5 files")
    parser.add_argument('-i', '--input_path', required=True,
                        help='Folder containing single read fast5 files')
    parser.add_argument('-s', '--save_path', required=True,
                        help="Folder to output multi read files to")
    parser.add_argument('-c', '--compression', required=True, choices=list(COMPRESSION_MAP.keys()),
                        help="Target output compression type")
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Maximum number of threads to use")
    parser.add_argument('--recursive', action='store_true',
                        help="Search recursively through folders for single_read fast5 files")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    compress_batch(input_folder=args.input_path,
                   output_folder=args.save_path,
                   target_compression=COMPRESSION_MAP[args.compression],
                   threads=args.threads,
                   recursive=args.recursive)


if __name__ == '__main__':
    main()

import logging
import os
import shutil
from argparse import ArgumentParser, ArgumentError
from multiprocessing.pool import Pool

from ont_fast5_api import __version__
from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar
from ont_fast5_api.fast5_file import Fast5File, EmptyFast5
from ont_fast5_api.fast5_interface import is_multi_read
from ont_fast5_api.multi_fast5 import MultiFast5File, copy_attributes
from ont_fast5_api.static_data import OPTIONAL_READ_GROUPS


def compress_batch(input_folder, output_folder, target_compression, recursive=True, threads=1, follow_symlinks=True,
                   in_place=False, sanitize=False):
    # We require an absolute input path to we can replicate the data structure relative to it later on
    input_folder = os.path.abspath(input_folder)

    file_list = get_fast5_file_list(input_folder, recursive, follow_symlinks=follow_symlinks)
    if len(file_list) == 0:
        raise ValueError("No input fast5 files found in '{}'. Recursive={}".format(input_folder, recursive))

    # Set up the process pool and the progressbar
    pool = Pool(min(threads, len(file_list)))
    pbar = get_progress_bar(len(file_list))

    def update(result):
        if in_place and result is not None:
            input_path, output_path = result
            shutil.move(output_path, input_path)
        pbar.update(pbar.currval + 1)

    for input_file in file_list:
        input_path = os.path.join(input_folder, input_file)
        if in_place:
            output_path = os.path.join(input_path + ".tmp.compressed")
        else:
            output_path = os.path.join(output_folder, os.path.relpath(input_path, input_folder))

        pool.apply_async(func=compress_file,
                         args=(input_path, output_path, target_compression, sanitize),
                         callback=update)

    # Tear down the process pool and pbar. We can't use contextmanagers since we need to close() then join()
    pool.close()
    pool.join()
    pbar.finish()


def compress_file(input_file, output_file, target_compression, sanitize=False):
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        if is_multi_read(input_file):
            with MultiFast5File(input_file, 'r') as input_f5, MultiFast5File(output_file, 'a') as output_f5:
                for read in input_f5.get_reads():
                    output_f5.add_existing_read(read, target_compression, sanitize=sanitize)
        else:
            with Fast5File(input_file, 'r') as input_f5, \
                    EmptyFast5(output_file, 'a') as output_f5:
                compress_single_read(output_f5, input_f5, target_compression, sanitize=sanitize)
    except Exception as e:
        # Error raised in Pool.async will be lost so we explicitly print them.
        logging.exception(e)
        raise
    return (input_file, output_file)


def compress_single_read(output_f5, read_to_copy, target_compression, sanitize=False):
    read_id = read_to_copy.get_read_id()
    raw_dataset_name = read_to_copy.raw_dataset_name
    raw_group_name = read_to_copy.raw_dataset_group_name
    read_name = "read_" + read_id
    # Recreating the status object is painful, but doesn't actually interact with the file so we can just reference it.
    output_f5.status = read_to_copy.status

    if str(target_compression) in read_to_copy.raw_compression_filters:
        # If we have the right compression then no need for doing anything fancy
        output_f5.handle.copy(read_to_copy.handle, read_name)
    else:
        copy_attributes(read_to_copy.handle.attrs, output_f5.handle)
        for subgroup in read_to_copy.handle:
            if subgroup not in raw_dataset_name:
                if sanitize and subgroup in OPTIONAL_READ_GROUPS:
                    # skip optional groups when sanitizing
                    continue
                output_f5.handle.copy(read_to_copy.handle[subgroup], subgroup)
            else:
                raw_attrs = read_to_copy.handle[raw_group_name].attrs
                raw_data = read_to_copy.handle[raw_dataset_name]
                output_f5.add_raw_data(raw_data, raw_attrs, compression=target_compression)


def main():
    parser = ArgumentParser("Tool for changing the compression of Fast5 files")
    parser.add_argument('-i', '--input_path', required=True,
                        help='Folder containing single read fast5 files')

    output_group = parser.add_mutually_exclusive_group(required=True)
    save_arg = output_group.add_argument('-s', '--save_path', default=None,
                                         help="Folder to output multi read files to")
    output_group.add_argument('--in_place', action='store_true',
                              help='Replace the old files with new files in place')

    parser.add_argument('-c', '--compression', required=True, choices=list(COMPRESSION_MAP.keys()),
                        help="Target output compression type")
    parser.add_argument('--sanitize', action='store_true',
                        help="Clean output files of optional groups and datasets (e.g. 'Analyses')")
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Maximum number of threads to use")
    parser.add_argument('--recursive', action='store_true',
                        help="Search recursively through folders for single_read fast5 files")
    parser.add_argument('--ignore_symlinks', action='store_true',
                        help="Ignore symlinks when searching recursively for fast5 files")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    if args.input_path == args.save_path:
        raise ArgumentError(save_arg, "--input_path and --save_path must be different locations, or use --in_place")
    if args.sanitize and args.save_path is None:
        raise ArgumentError(save_args, "--save_path must be given if using --sanitize")

    compress_batch(input_folder=args.input_path,
                   output_folder=args.save_path,
                   target_compression=COMPRESSION_MAP[args.compression],
                   threads=args.threads,
                   recursive=args.recursive,
                   follow_symlinks=not args.ignore_symlinks,
                   in_place=args.in_place,
                   sanitize=args.sanitize)


if __name__ == '__main__':
    main()

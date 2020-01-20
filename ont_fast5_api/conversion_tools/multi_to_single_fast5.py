from argparse import ArgumentParser
from multiprocessing import Pool
import logging
import os

from ont_fast5_api import __version__
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar
from ont_fast5_api.fast5_file import EmptyFast5, Fast5FileTypeError
from ont_fast5_api.fast5_interface import check_file_type, MULTI_READ
from ont_fast5_api.multi_fast5 import MultiFast5File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
exc_info = False


def batch_convert_multi_files_to_single(input_path, output_folder, threads, recursive, follow_symlinks):
    pool = Pool(threads)
    file_list = get_fast5_file_list(input_path, recursive, follow_symlinks=follow_symlinks)
    pbar = get_progress_bar(len(file_list))

    def update(result):
        input_file = result[0]
        with open(os.path.join(output_folder, "filename_mapping.txt"), 'a') as output_table:
            for filename in result[1]:
                output_table.write("{}\t{}\n".format(input_file, filename))
        pbar.update(pbar.currval + 1)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    results_array = []
    for batch_num, filename in enumerate(file_list):
        results_array.append(pool.apply_async(convert_multi_to_single,
                                              args=(filename, output_folder,
                                                    str(batch_num)),
                                              callback=update))

    pool.close()
    pool.join()
    pbar.finish()


def convert_multi_to_single(input_file, output_folder, subfolder):
    output_files = ()
    try:
        output_files = try_multi_to_single_conversion(input_file, output_folder, subfolder)
    except Exception as e:
        logger.error("{}\n\tFailed to copy files from: {}"
                     "".format(e, input_file), exc_info=exc_info)
    return input_file, output_files


def try_multi_to_single_conversion(input_file, output_folder, subfolder):
    output_files = []
    with MultiFast5File(input_file, 'r') as multi_f5:
        file_type = check_file_type(multi_f5)
        if file_type != MULTI_READ:
            raise Fast5FileTypeError("Could not convert Multi->Single for file type '{}' with path '{}'"
                                     "".format(file_type, input_file))
        for read in multi_f5.get_reads():
            try:
                output_file = os.path.join(output_folder, subfolder, "{}.fast5".format(read.read_id))
                create_single_f5(output_file, read)
                output_files.append(os.path.basename(output_file))
            except Exception as e:
                logger.error("{}\n\tFailed to copy read '{}' from {}"
                             "".format(str(e), read.read_id, input_file), exc_info=exc_info)
    return output_files


def create_single_f5(output_file, read):
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))
    with EmptyFast5(output_file, 'w') as single_f5:
        for group in read.handle:
            if group == "Raw":
                read_number = read.handle["Raw"].attrs["read_number"]
                single_f5.handle.copy(read.handle[group], "Raw/Reads/Read_{}".format(read_number))
            elif group in ("channel_id", "context_tags", "tracking_id"):
                if "UniqueGlobalKey" not in single_f5.handle:
                    single_f5.handle.create_group("UniqueGlobalKey")
                single_f5.handle.copy(read.handle[group], "UniqueGlobalKey/{}".format(group))
            else:
                single_f5.handle.copy(read.handle[group], group)


def main():
    parser = ArgumentParser("")
    parser.add_argument('-i', '--input_path', required=True,
                        help="MultiRead fast5 file or path to directory of MultiRead files")
    parser.add_argument('-s', '--save_path', required=True,
                        help="Folder to output SingleRead fast5 files to")
    parser.add_argument('--recursive', action='store_true',
                        help="Search recursively through folders for MultiRead fast5 files")
    parser.add_argument('--ignore_symlinks', action='store_true',
                        help="Ignore symlinks when searching recursively for fast5 files")
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Number of threads to use")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    batch_convert_multi_files_to_single(args.input_path, args.save_path, args.threads,
                                        args.recursive, follow_symlinks=not args.ignore_symlinks)


if __name__ == '__main__':
    main()

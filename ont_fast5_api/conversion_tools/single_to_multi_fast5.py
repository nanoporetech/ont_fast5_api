from argparse import ArgumentParser
from multiprocessing import Pool
from collections import deque
import logging
import os

from ont_fast5_api import __version__
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, batcher, get_progress_bar
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.multi_fast5 import MultiFast5File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
exc_info = False


def batch_convert_single_to_multi(input_path, output_folder, filename_base, batch_size, threads, recursive):

    pool = Pool(threads)
    file_list = get_fast5_file_list(input_path, recursive)
    pbar = get_progress_bar(int((len(file_list)+batch_size-1)/batch_size))

    def update(results):
        output_file = os.path.basename(results.popleft())
        with open(os.path.join(output_folder, "filename_mapping.txt"), 'a') as output_table:
            for filename in results:
                output_table.write("{}\t{}\n".format(filename, output_file))
        pbar.update(pbar.currval + 1)

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    results_array = []
    for batch_num, batch in enumerate(batcher(file_list, batch_size)):
        output_file = os.path.join(output_folder, "{}_{}.fast5".format(filename_base, batch_num))
        results_array.append(pool.apply_async(create_multi_read_file,
                                              args=(batch, output_file),
                                              callback=update))

    pool.close()
    pool.join()
    pbar.finish()


def create_multi_read_file(input_files, output_file):
    results = deque([os.path.basename(output_file)])
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))
    if os.path.exists(output_file):
        logger.info("FileExists - appending new reads to existing file: {}".format(output_file))
    try:
        with MultiFast5File(output_file, 'a') as multi_f5:
            for filename in input_files:
                try:
                    with Fast5File(filename, 'r') as single_f5:
                        add_read_to_multi_fast5(multi_f5, single_f5)
                        results.append(os.path.basename(filename))
                except Exception as e:
                    logger.error("{}\n\tFailed to add single read file: '{}' to '{}'"
                                 "".format(e, filename, output_file), exc_info=exc_info)
    except Exception as e:
        logger.error("{}\n\tFailed to write to MultiRead file: {}"
                     "".format(e, output_file), exc_info=exc_info)
    finally:
        return results


def add_read_to_multi_fast5(multi_f5, single_f5):
    read_number = single_f5._get_only_read_number()
    read_id = single_f5.get_read_id()
    run_id = single_f5.get_run_id()
    read = multi_f5.create_read(read_id, run_id)

    # Copy Raw data into new file
    read.handle.copy(single_f5.handle["Raw/Reads/Read_{}".format(read_number)], "Raw")

    # Copy UniqueGlobalKey data into new file
    for group in single_f5.handle["UniqueGlobalKey"]:
        read.handle.copy(single_f5.handle["UniqueGlobalKey/{}".format(group)], group)

    for group in single_f5.handle:
        if group in ("Raw", "UniqueGlobalKey"):
            # Skip these as they require special handling
            continue
        read.handle.copy(single_f5.handle[group], group)


def main():
    parser = ArgumentParser("")
    parser.add_argument('-i', '--input_path', required=True,
                        help='Folder containing single read fast5 files')
    parser.add_argument('-s', '--save_path', required=True,
                        help="Folder to output multi read files to")
    parser.add_argument('-f', '--filename_base', default='batch', required=False,
                        help="Root of output filename, default='batch' -> 'batch_0.fast5'")
    parser.add_argument('-n', '--batch_size', type=int, default=4000, required=False,
                        help="Number of reads per multi-read file")
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Number of threads to use")
    parser.add_argument('--recursive', action='store_true',
                        help="Search recursively through folders for for single_read fast5 files")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    batch_convert_single_to_multi(args.input_path, args.save_path, args.filename_base, args.batch_size, args.threads, args.recursive)


if __name__ == '__main__':
    main()

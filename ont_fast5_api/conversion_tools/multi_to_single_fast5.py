from __future__ import division

from argparse import ArgumentParser
from multiprocessing import Pool
from collections import deque
import logging
import h5py
import os

from ont_fast5_api import CURRENT_FAST5_VERSION, __version__
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, get_progress_bar
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.multi_fast5 import MultiFast5File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
exc_info = False



class EmptyFast5(Fast5File):
    def _initialise_file(self):
        # We don't want to create/validate the full f5 data structure as most fields won't exist yet
        self.handle = h5py.File(self.filename, self.mode)
        self.handle.attrs['file_version'] = CURRENT_FAST5_VERSION
        self._is_open = True


def batch_convert_multi_files_to_single(input_path, output_folder, threads, recursive):

    pool = Pool(threads)
    file_list = get_fast5_file_list(input_path, recursive)
    pbar = get_progress_bar(len(file_list))

    def update(results):
        output_file = os.path.basename(results.popleft())
        with open(os.path.join(output_folder, "filename_mapping.txt"), 'a') as output_table:
            for filename in results:
                output_table.write("{}\t{}\n".format(output_file, filename))
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
    results = deque([os.path.basename(input_file)])
    try:
        with MultiFast5File(input_file, 'r') as multi_f5:
            for read_id in multi_f5.get_read_ids():
                try:
                    read = multi_f5.get_read(read_id)
                    output_file = os.path.join(output_folder, subfolder, "{}.fast5".format(read_id))
                    create_single_f5(output_file, read)
                    results.append(os.path.basename(output_file))
                except Exception as e:
                    logger.error("{}\n\tFailed to copy read '{}' from {}"
                                 "".format(str(e), read_id, input_file), exc_info=exc_info)
    except Exception as e:
        logger.error("{}\n\tFailed to copy files from: {}"
                     "".format(e, input_file), exc_info=exc_info)
    finally:
        return results


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
                        help="Search recursively through folders for for MultiRead fast5 files")
    parser.add_argument('-t', '--threads', type=int, default=1, required=False,
                        help="Number of threads to use")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    batch_convert_multi_files_to_single(args.input_path, args.save_path, args.threads, args.recursive)


if __name__ == '__main__':
    main()

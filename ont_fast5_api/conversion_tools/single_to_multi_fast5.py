import logging
import os
from argparse import ArgumentParser
from multiprocessing import Pool

from ont_fast5_api import __version__
from ont_fast5_api.compression_settings import COMPRESSION_MAP
from ont_fast5_api.conversion_tools.conversion_utils import get_fast5_file_list, batcher, get_progress_bar
from ont_fast5_api.fast5_file import Fast5File, Fast5FileTypeError
from ont_fast5_api.multi_fast5 import MultiFast5File

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
exc_info = False


def batch_convert_single_to_multi(input_path, output_folder, filename_base, batch_size,
                                  threads, recursive, follow_symlinks, target_compression):
    pool = Pool(threads)
    file_list = get_fast5_file_list(input_path, recursive, follow_symlinks)
    pbar = get_progress_bar(int((len(file_list) + batch_size - 1) / batch_size))

    def update(result):
        output_file = result[1]
        with open(os.path.join(output_folder, "filename_mapping.txt"), 'a') as output_table:
            for filename in result[0]:
                output_table.write("{}\t{}\n".format(filename, output_file))
        pbar.update(pbar.currval + 1)

    results_array = []
    os.makedirs(output_folder, exist_ok=True)
    for batch_num, batch in enumerate(batcher(file_list, batch_size)):
        output_file = os.path.join(output_folder, "{}_{}.fast5".format(filename_base, batch_num))
        results_array.append(pool.apply_async(create_multi_read_file,
                                              args=(batch, output_file, target_compression),
                                              callback=update))

    pool.close()
    pool.join()
    pbar.finish()


def create_multi_read_file(input_files, output_file, target_compression):
    results = []
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if os.path.exists(output_file):
        logger.info("FileExists - appending new reads to existing file: {}".format(output_file))
    try:
        with MultiFast5File(output_file, 'a') as multi_f5:
            for filename in input_files:
                try:
                    with Fast5File(filename, 'r') as f5_input:
                        read = f5_input.get_read(f5_input.read_id)
                        multi_f5.add_existing_read(read, target_compression=target_compression)
                    results.append(os.path.basename(filename))
                except Fast5FileTypeError as e:
                    logger.error("{}: Cannot input MultiRead files to single_to_multi: '{}'"
                                 "".format(e, filename), exc_info=exc_info)
                    raise
                except Exception as e:
                    logger.error("{}\n\tFailed to add single read file: '{}' to '{}'"
                                 "".format(e, filename, output_file), exc_info=exc_info)

    except Fast5FileTypeError:
        raise
    except Exception as e:
        logger.error("{}\n\tFailed to write to MultiRead file: {}"
                     "".format(e, output_file), exc_info=exc_info)
    return results, output_file


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
                        help="Search recursively through folders for single_read fast5 files")
    parser.add_argument('--ignore_symlinks', action='store_true',
                        help="Ignore symlinks when searching recursively for fast5 files")
    parser.add_argument('-c', '--compression', required=False, default=None,
                        choices=list(COMPRESSION_MAP.keys()) + [None], help="Target output compression type")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    if args.compression is not None:
        args.compression = COMPRESSION_MAP[args.compression]

    batch_convert_single_to_multi(args.input_path,
                                  args.save_path,
                                  args.filename_base,
                                  args.batch_size,
                                  args.threads,
                                  args.recursive,
                                  follow_symlinks=not args.ignore_symlinks,
                                  target_compression=args.compression)


if __name__ == '__main__':
    main()

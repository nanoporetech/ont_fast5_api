import os

from glob import glob
from progressbar import RotatingMarker, ProgressBar, SimpleProgress, Bar, Percentage, ETA
from ont_fast5_api.fast5_interface import get_fast5_file

def batcher(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def yield_fast5_files(input_path, recursive, follow_symlinks=True):
    """
    Yield fast5 file paths within a given directory.
    Optionally search recursively and follow symbolic links

    :param input_dir: Path
    :param recursive: bool
    :param follow_symlinks: bool
    :return:
    """
    if os.path.isfile(input_path):
        yield input_path
        return

    if recursive:
        for root, _, filenames in os.walk(input_path, followlinks=follow_symlinks):
            for filename in filenames:
                if not filename.endswith('.fast5'): continue
                yield os.path.join(root, filename)
    else:
        for filename in glob(os.path.join(input_path, '*.fast5')):
            yield filename
    return

def yield_fast5_reads(input_path, recursive, follow_symlinks=True, read_ids=None):
    """
    Iterate over reads in fast5 files and yield read_ids and fast5 read objects.
    If read_id_set is defined, skip reads which are not in this set/list. An empty set/list returns all.

    :param input_dir: Path
    :param recursive: bool
    :param follow_symlinks: bool
    :param read_ids: set or list
    :raise TypeError: read_id_set must be of type set or list'
    :return: yielded tuple (read_id, fast5_read_object)
    """
    if not isinstance(read_ids, (list, set)) and read_ids is not None:
        raise TypeError('read_ids must be of type set or list or none')

    if read_ids and isinstance(read_ids, list):
        read_ids = set(read_ids)

    for fast5_path in yield_fast5_files(input_path=input_path, recursive=recursive, follow_symlinks=follow_symlinks):
        fast5_file = get_fast5_file(fast5_path)
        if read_ids:
            selected_reads = read_ids.intersection(fast5_file.get_read_ids())
        else:
            selected_reads = fast5_file.get_read_ids()

        for read_id in selected_reads:
            yield read_id, fast5_file.get_read(read_id)

def get_fast5_file_list(input_path, recursive, follow_symlinks=True):
    # NB this method is provided for compatibility with use cases where
    # generator behaviour is not appropriate.
    # E.g. where we need to know the file_list length or be able to sample from it
    return list(yield_fast5_files(input_path, recursive, follow_symlinks))


def get_progress_bar(num_reads):
    bar_format = [RotatingMarker(), " ", SimpleProgress(), Bar(), Percentage(), " ", ETA()]
    progress_bar = ProgressBar(maxval=num_reads, widgets=bar_format)
    bad_progressbar_version = False
    try:
        progress_bar.currval
    except AttributeError as e:
        bad_progressbar_version = True
        pass
    if bad_progressbar_version:
        raise RuntimeError('Wrong progressbar package detected, likely '
                           '"progressbar2". Please uninstall that package and '
                           'install "progressbar33" instead.')

    return progress_bar.start()

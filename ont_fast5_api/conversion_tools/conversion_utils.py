import os
from glob import glob
from progressbar import RotatingMarker, ProgressBar, SimpleProgress, Bar, Percentage, ETA


def batcher(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def yield_fast5_files(input_path, recursive):
    if os.path.isfile(input_path):
        yield input_path
        return

    if recursive:
        for root, _, filenames in os.walk(input_path, followlinks=True):
            for filename in filenames:
                if not filename.endswith('.fast5'): continue
                yield os.path.join(root, filename)
    else:
        for filename in glob(os.path.join(input_path, '*.fast5')):
            yield filename
    return


def get_fast5_file_list(input_path, recursive):
    # NB this method is provided for compatibility with use cases where
    # generator behaviour is not appropriate.
    # E.g. where we need to know the file_list length or be able to sample from it
    return list(yield_fast5_files(input_path, recursive))


def get_progress_bar(num_reads):
    bar_format = [RotatingMarker(), " ", SimpleProgress(), Bar(), Percentage(), " ", ETA()]
    return ProgressBar(maxval=num_reads, widgets=bar_format).start()

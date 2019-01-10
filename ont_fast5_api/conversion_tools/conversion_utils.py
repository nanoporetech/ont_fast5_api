import os
from glob import glob
from progressbar import RotatingMarker, ProgressBar, SimpleProgress, Bar, Percentage, ETA


def batcher(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def get_fast5_file_list(input_path, recursive):
    if os.path.isfile(input_path):
        return [input_path]
    if recursive:
        file_list = [y for x in os.walk(input_path, followlinks=True)
                     for y in glob(os.path.join(x[0], '*.fast5'))]
    else:
        file_list = [i for i in glob(os.path.join(input_path, '*.fast5'))]
    return file_list


def get_progress_bar(num_reads):
    bar_format = [RotatingMarker(), " ", SimpleProgress(), Bar(), Percentage(), " ", ETA()]
    return ProgressBar(maxval=num_reads, widgets=bar_format).start()

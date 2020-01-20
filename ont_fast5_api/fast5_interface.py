
from ont_fast5_api.fast5_file import Fast5File, Fast5FileTypeError
from ont_fast5_api.fast5_info import _clean
from ont_fast5_api.multi_fast5 import MultiFast5File

MULTI_READ = "multi-read"
SINGLE_READ = "single-read"
BULK_FAST5 = "bulk"


def get_fast5_file(filepath, mode='r'):
    if is_multi_read(filepath):
        return MultiFast5File(filepath, mode)
    else:
        return Fast5File(filepath, mode)


def check_file_type(f5_file):
    try:
        return _clean(f5_file.handle.attrs['file_type'])
    except KeyError:
        # On older files we don't have the 'file_type' attribute so check groups explicitly
        if len(f5_file.handle) == 0:
            # If there are no top-level groups we default to MultiRead
            return MULTI_READ
        if len([read for read in f5_file.handle if read.startswith('read_')]) != 0:
            # If there are any read_xxx groups we're definitely MultiRead
            return MULTI_READ
        if "UniqueGlobalKey" in f5_file.handle:
            # This group indicates a single read
            return SINGLE_READ
    raise TypeError("Fast5 file type could not be identified as single- or multi-read. "
                    "\nFilepath: {}".format(f5_file.filename))


def is_multi_read(filepath):
    """
    Determine if a file is a MultiFast5File, True if it is, False if it is a single Fast5File error for other types
    """
    with MultiFast5File(filepath, mode='r') as f5_file:
        file_type = check_file_type(f5_file)
        if file_type == MULTI_READ:
            return True
        elif file_type == SINGLE_READ:
            return False
        elif file_type == BULK_FAST5:
            raise NotImplementedError("ont_fast5_api does not support bulk fast files: {}".format(filepath))
        raise Fast5FileTypeError("Unknown file type: '{}' for file: {}".format(file_type, filepath))

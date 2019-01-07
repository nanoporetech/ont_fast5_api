from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.multi_fast5 import MultiFast5File


def get_fast5_file(filepath, mode='r'):
    if is_multi_read(filepath):
        return MultiFast5File(filepath, mode)
    else:
        return Fast5File(filepath, mode)


def is_multi_read(filepath):
    with MultiFast5File(filepath, mode='r') as fast5:
        if len(fast5.handle) == 0:
            # If there are no top-level groups we default to MultiRead
            return True
        if len(MultiFast5File(filepath, mode='r').get_read_ids()) != 0:
            # If there are any read_0123 groups we're definitely MultiRead
            return True
        if "UniqueGlobalKey" in fast5.handle:
            # This group indicates a single read
            return False
    raise TypeError("Fast5 file type could not be identified as single- or multi-read. "
                    "It should contain either 'UniqueGlobalKey' or 'read_' groups."
                    "\nFilepath: {}". format(filepath))

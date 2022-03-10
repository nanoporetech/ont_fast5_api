import h5py
import warnings
from ont_fast5_api import fast5_file as f5f


class FileToDict(object):

    def __init__(self):
        self.contents = {}

    def scan(self, name, obj):
        if isinstance(obj, h5py.Group):
            self.contents[name] = 'group'
        else:
            self.contents[name] = 'dataset'
            self.contents['{}.data'.format(name)] = \
                str(f5f._sanitize_data_for_reading(obj))
            self.contents['{}.cols'.format(name)] = obj.dtype.names
        attrdict = {}
        for item in obj.attrs:
            attrdict[item] = \
                str(f5f._sanitize_data_for_reading(obj.attrs[item]))
        self.contents['{}.attrs'.format(name)] = attrdict
        return None


def compare_hdf_files(file1, file2):
    """ Compare two hdf files.
    :param file1: First file to compare.
    :param file2: Second file to compare.

    :returns True if they are the same.
    """
    warnings.simplefilter("default")
    warnings.warn("'compare_hdf_files': HDF5 comparison is deprecated. \n"
                  "If this feature is still required please contact the project maintainers",
                  DeprecationWarning)
    data1 = FileToDict()
    data2 = FileToDict()
    scanner1 = data1.scan
    scanner2 = data2.scan
    with h5py.File(file1, 'r') as fh1:
        fh1.visititems(scanner1)
    with h5py.File(file2, 'r') as fh2:
        fh2.visititems(scanner2)
    return data1.contents == data2.contents

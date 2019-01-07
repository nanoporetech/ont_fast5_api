import h5py

from six import raise_from
from ont_fast5_api import CURRENT_FAST5_VERSION
from ont_fast5_api.fast5_file import AbstractFast5File
from ont_fast5_api.fast5_read import Fast5Read



class MultiFast5File(AbstractFast5File):
    def __init__(self, filename, mode='r'):
        self.filename = filename
        self.mode = mode
        self.handle = h5py.File(self.filename, self.mode)
        self._is_open = True
        if mode != 'r' and 'file_version' not in self.handle.attrs:
            try:
                self.handle.attrs['file_version'] = str(CURRENT_FAST5_VERSION)
            except IOError as e:
                raise_from(IOError("Could not write 'file_version' in mode '{}' to fast5 file: {}"
                                   "".format(self.filename, self.mode)), e)

    def get_read_ids(self):
        # Return all groups with the 'read_' stripped from the front
        return [group_name[5:] for group_name in self.handle if group_name.startswith('read_')]

    def get_read(self, read_id):
        group_name = "read_" + read_id
        if group_name not in self.handle:
            raise KeyError("Read '{}' not in: {}".format(group_name, self.filename))
        return Fast5Read(self, read_id)

    def create_read(self, read_id, run_id):
        group_name = "read_" + read_id
        if group_name not in self.handle:
            try:
                self.handle.create_group(group_name)
            except ValueError as e:
                raise_from(ValueError("Could not create group '{}' in file: {}"
                                      .format(group_name, self.filename)), e)
            self.handle[group_name].attrs["run_id"] = run_id
        else:
            raise ValueError("Read '{}' already exists in: {}".format(group_name, self.filename))
        return Fast5Read(self, read_id)

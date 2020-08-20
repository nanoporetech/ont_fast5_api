import h5py

from ont_fast5_api import CURRENT_FAST5_VERSION
from ont_fast5_api.fast5_file import Fast5File, Fast5FileTypeError
from ont_fast5_api.fast5_read import AbstractFast5, Fast5Read
from ont_fast5_api.static_data import HARDLINK_GROUPS, OPTIONAL_READ_GROUPS


class MultiFast5File(AbstractFast5):
    def __init__(self, filename, mode='r'):
        self.filename = filename
        self.mode = mode
        self.handle = h5py.File(self.filename, self.mode)
        self._run_id_map = None
        if mode != 'r' and 'file_version' not in self.handle.attrs:
            try:
                self.handle.attrs['file_version'] = str(CURRENT_FAST5_VERSION)
            except IOError as e:
                raise IOError("Could not write 'file_version' in mode '{}' to fast5 file: {}"
                                   "".format(self.filename, self.mode)) from e

    def get_reads(self):
        for group_name in self.handle:
            if group_name.startswith('read_'):
                yield Fast5Read(self, group_name[5:])

    def get_read_ids(self):
        # Return all groups with the 'read_' stripped from the front
        return [group_name[5:] for group_name in self.handle if group_name.startswith('read_')]

    def get_read(self, read_id):
        group_name = "read_" + read_id
        if group_name not in self.handle:
            raise KeyError("Read '{}' not in: {}".format(group_name, self.filename))
        return Fast5Read(self, read_id)

    def create_read(self, read_id, run_id):
        DeprecationWarning("'MultiFast5File.create_read()' will be deprecated. "
                           "Use `MultiFast5File.create_empty_read()` instead")
        return self.create_empty_read(read_id, run_id)

    def create_empty_read(self, read_id, run_id):
        group_name = "read_" + read_id
        if group_name not in self.handle:
            try:
                self.handle.create_group(group_name)
            except ValueError as e:
                raise ValueError("Could not create group '{}' in file: {}".format(group_name, self.filename)) from e
            try:
                for shared_group in HARDLINK_GROUPS:
                    self.handle["{}/{}".format(group_name, shared_group)] = \
                        self.handle["read_{}/{}".format(self.run_id_map[run_id], shared_group)]
            except KeyError:
                # If we can't hardlink to existing groups then continue as normal
                # registering this read as the new source of metadata for this run_id_map
                self.run_id_map[run_id] = read_id
            self.handle[group_name].attrs["run_id"] = run_id
        else:
            raise ValueError("Read '{}' already exists in: {}".format(group_name, self.filename))
        return Fast5Read(self, read_id)


    @property
    def run_id_map(self):
        if self._run_id_map is None:
            self._run_id_map = dict()
            for read in self.get_reads():
                try:
                    self._run_id_map[read.run_id] = read.read_id
                except KeyError:
                    # If we can't find the read.run_id then there is a KeyError
                    # We want to ignore these cases since they can't be linked anyway
                    pass
        return self._run_id_map

    def add_existing_read(self, read_to_add, target_compression=None, sanitize=False):
        if isinstance(read_to_add, Fast5File):
            self._add_read_from_single(read_to_add, target_compression, sanitize=sanitize)
        elif isinstance(read_to_add.parent, MultiFast5File):
            self._add_read_from_multi(read_to_add, target_compression, sanitize=sanitize)
        else:
            raise Fast5FileTypeError("Could not add read to output file from input file type '{}' with path '{}'"
                                     "".format(type(read_to_add.parent), read_to_add.parent.filename))

    def _add_read_from_multi(self, read_to_add, target_compression, sanitize=False):
        read_name = "read_" + read_to_add.read_id
        self.handle.create_group(read_name)
        output_group = self.handle[read_name]
        copy_attributes(read_to_add.handle.attrs, output_group)
        for subgroup in read_to_add.handle:
            if sanitize and subgroup in OPTIONAL_READ_GROUPS:
                # skip optional groups when sanitizing
                continue
            elif subgroup == read_to_add.raw_dataset_group_name \
                    and target_compression is not None \
                    and str(target_compression) not in read_to_add.raw_compression_filters:
                raw_attrs = read_to_add.handle[read_to_add.raw_dataset_group_name].attrs
                raw_data = read_to_add.handle[read_to_add.raw_dataset_name]
                output_read = self.get_read(read_to_add.read_id)
                output_read.add_raw_data(raw_data, raw_attrs, compression=target_compression)
                continue
            elif subgroup in HARDLINK_GROUPS:
                if read_to_add.run_id in self.run_id_map:
                    # There may be a group to link to, but we must check it actually exists!
                    hardlink_source = "read_{}/{}".format(self.run_id_map[read_to_add.run_id], subgroup)
                    if hardlink_source in self.handle:
                        hardlink_dest = "read_{}/{}".format(read_to_add.read_id, subgroup)
                        self.handle[hardlink_dest] = self.handle[hardlink_source]
                        continue
                # If we couldn't hardlink to anything we need to make the group we create available for future reads
                self.run_id_map[read_to_add.run_id] = read_to_add.read_id
            # If we haven't done a special-case copy then we can fall back on the default copy
            output_group.copy(read_to_add.handle[subgroup], subgroup)

    def _add_read_from_single(self, read_to_add, target_compression, sanitize=False):
        read_name = "read_" + read_to_add.read_id
        self.handle.create_group(read_name)
        output_group = self.handle[read_name]
        copy_attributes(read_to_add.handle.attrs, output_group)
        for subgroup in read_to_add.handle:
            if sanitize and subgroup in OPTIONAL_READ_GROUPS:
                # skip optional groups when sanitizing
                continue
            elif subgroup == "UniqueGlobalKey":
                for unique_group in read_to_add.handle["UniqueGlobalKey"]:
                    if unique_group in HARDLINK_GROUPS and read_to_add.run_id in self.run_id_map:
                        hardlink_source = "read_{}/{}".format(self.run_id_map[read_to_add.run_id], unique_group)
                        if hardlink_source in self.handle:
                            hardlink_dest = "read_{}/{}".format(read_to_add.read_id, unique_group)
                            self.handle[hardlink_dest] = self.handle[hardlink_source]
                    else:
                        output_group.copy(read_to_add.handle["UniqueGlobalKey/{}".format(unique_group)],
                                          unique_group)
                self.run_id_map[read_to_add.run_id] = read_to_add.read_id
            elif subgroup == "Raw":
                if target_compression is None or str(target_compression) in read_to_add.raw_compression_filters:
                    output_group.copy(read_to_add.handle[read_to_add.raw_dataset_group_name], "Raw")
                else:
                    raw_attrs = read_to_add.handle[read_to_add.raw_dataset_group_name].attrs
                    raw_data = read_to_add.handle[read_to_add.raw_dataset_name]
                    output_read = self.get_read(read_to_add.read_id)
                    output_read.add_raw_data(raw_data, raw_attrs, compression=target_compression)
            else:
                if not sanitize:
                    output_group.copy(read_to_add.handle[subgroup], subgroup)


def copy_attributes(input_attrs, output_group):
    for k, v in input_attrs.items():
        output_group.attrs[k] = v

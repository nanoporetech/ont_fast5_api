""" Basic API for reading/writing single-read fast5 files.
"""
import os
import h5py
import numpy as np

from ont_fast5_api import CURRENT_FAST5_VERSION
from ont_fast5_api.compression_settings import VBZ, raise_missing_vbz_error_read
from ont_fast5_api.fast5_read import Fast5Read
from ont_fast5_api.fast5_info import Fast5Info, ReadInfo
from ont_fast5_api.static_data import supported_modes, mode_docstring

# This unused import is included for backwards compatibilty and can be removed in future.
from ont_fast5_api.data_sanitisation import _sanitize_data_for_reading, _sanitize_data_for_writing


class Fast5FileTypeError(ValueError):
    pass


class Fast5File(Fast5Read):
    """ This object encapsulates a read fast5 file. It can be used
    instead of directly using the h5py API in order to help maintain
    consistency in fast5 file format, and simplify common tasks.

    The object will contain a field called **status** that is a
    Fast5Status object with details about the file.
    """

    def __init__(self, fname, mode='r'):
        """ Constructor. Opens the specified file.

        :param fname: Filename to open.
        :param mode: File open mode (r, r+, w, w-, x, a).
        """
        self.global_key = "UniqueGlobalKey/"
        if mode not in supported_modes:
            raise IOError("Unsupported file handle mode : '{}' {}".format(mode, mode_docstring))
        self.filename = fname
        self.handle = None
        self.mode = mode
        self._initialise_file()

    def get_reads(self):
        # We yield here for consistency with MultiFast5File
        yield self.get_read(self.get_read_id())

    def get_read_ids(self):
        return [self.get_read_id()]

    def get_read(self, read_id):
        if read_id != self.get_read_id():
            raise KeyError("read_id given: {} does not match read_id in file: {}"
                           "".format(read_id, self.get_read_id()))
        return self

    @property
    def read_id(self):
        return self.status.read_info[0].read_id

    def get_read_id(self):
        DeprecationWarning("'read.get_read_id()' will be deprecated. Use the property 'read.read_id' instead")
        return self.read_id

    @property
    def raw_dataset_group_name(self):
        read_number = self._get_only_read_number()
        return 'Raw/Reads/Read_{}'.format(read_number)

    @property
    def has_context_tags(self):
        return 'context_tags' in self.handle[self.global_key[:-1]]

    def add_channel_info(self, data, clear=False):
        """ Add channel info data to the channel_id group.

        :param data: A dictionary of key/value pairs. Keys must be strings.
            Values can be strings or numeric values.
        :param clear: If set, any existing channel info data will be removed. 
        """
        self.assert_writeable()
        self._add_attributes(self.global_key + 'channel_id', data, clear)

    def get_raw_data(self, read_number=None, start=None, end=None, scale=False):
        """ Pull raw data from the file.
        
        :param read_number: The read number you want raw data from. Pass None
            if there is only one read, and will grab that read's raw data.
        :param start: The first sample to pull. Default is to pull from the
            beginning.
        :param end: One past the last sample to pull. Default is to pull until
            the end.
        :param scale: If set, the returned data will be scaled floating point
            values, in pA. Otherwise raw DAQ values are returned as 16 bit
            integers.
        :returns: Raw data as either 32 bit floats, or 16 bit integers.
        """
        self.assert_open()
        if self.raw_dataset_name not in self.handle:
            msg = 'Fast5 file has no raw data for read {} in {}'.format(read_number, self.filename)
            raise KeyError(msg)
        return self._load_raw(self.raw_dataset_name, start, end, scale)

    def add_raw_data(self, data, attrs=None, compression=VBZ):
        """ Add raw data for a read.
        
        :param read_number: The number of the read the raw data is for.
        :param data: The raw data DAQ values (16 bit integers).
        
        The read must already exist in the file. It must not already
        have raw data.
        """
        self.assert_writeable()

        if self.raw_dataset_name in self.handle:
            msg = 'Fast5 file already has raw data for read {} in {}'
            raise KeyError(msg.format(self.raw_dataset_name, self.filename))

        group_name = self.raw_dataset_group_name
        if group_name not in self.handle:
            self.handle.create_group(group_name)

        try:
            self.handle[group_name].create_dataset('Signal', data=data, dtype='i2', **vars(compression))
        except ValueError as e:
            raise_missing_vbz_error_read(e)

        read_index = self.status.read_number_map[self._get_only_read_number()]
        self.status.read_info[read_index].has_raw_data = True
        if attrs is not None:
            self._add_attributes(group_name, attrs, clear=True)

    def add_read(self, read_number, read_id, start_time, duration, mux, median_before):
        """ Add a new read to the file.

        :param read_number: The read number to assign to the read.
        :param read_id: The unique read-id for the read.
        :param start_time: The start time (in samples) of the read.
        :param duration: The duration (in samples) of the read.
        :param mux: The mux set at the time of the read.
        :param median_before: The median level of the data before the read.

        Note that most tools assume a file contains only one read.
        Putting multiple reads into a file severely limits the
        ability to operate on those reads with standard tools.
        """
        self.assert_writeable()
        read_info = ReadInfo(read_number, read_id, start_time, duration, mux=mux, median_before=median_before)
        self.status.read_info.append(read_info)
        n = len(self.status.read_info) - 1
        self.status.read_number_map[read_number] = n
        self.status.read_id_map[read_id] = n
        group_name = self.raw_dataset_group_name
        attrs = {'read_number': read_number,
                 'read_id': read_id,
                 'start_time': start_time,
                 'duration': duration,
                 'start_mux': mux,
                 'median_before': median_before}
        self._add_group(group_name, attrs)

    #########################
    #
    #  Static methods below
    #
    #########################

    @staticmethod
    def update_legacy_file(fname):
        """ Update a fast5 file from an older version to the new standard.
        
        :param fname: The filename of the fast5 file.
        """
        status = Fast5Info(fname)
        if not status.valid:
            raise IOError('Cannot update invalid file: {}'.format(fname))
        with h5py.File(fname, 'r+') as handle:
            # Add Raw/Read/Read_## groups for reads if they are missing.
            for read_info in status.read_info:
                read_group_name = 'Raw/Reads/Read_{}'.format(read_info.read_number)
                if read_group_name in handle:
                    rgh = handle[read_group_name]
                else:
                    rgh = handle.create_group(read_group_name)
                rgh.attrs['read_number'] = read_info.read_number
                rgh.attrs['read_id'] = read_info.read_id
                rgh.attrs['duration'] = read_info.duration
                rgh.attrs['start_time'] = read_info.start_time
                rgh.attrs['start_mux'] = read_info.start_mux

            # Add the Analyses and tracking_id groups, if they are missing.
            if not 'Analyses' in handle:
                handle.create_group('Analyses')
            if not 'tracking_id' in handle['UniqueGlobalKey']:
                handle.create_group('UniqueGlobalKey/tracking_id')

            # Update the EventDetection_000 created by old versions of MinKNOW, if needed.
            if status.version < 1.1:
                if 'Analyses/EventDetection_000' in handle:
                    reads_group = handle['Analyses/EventDetection_000/Reads']
                    data_group_names = reads_group.keys()
                    for data_group_name in data_group_names:
                        read_group = reads_group[data_group_name]
                        read_number = read_group.attrs['read_number']
                        read_info = status.read_info[status.read_number_map[read_number]]
                        read_group.attrs['read_id'] = read_info.read_id
                        if 'Events' in read_group:
                            dataset = read_group['Events']
                            if 'variance' in dataset.dtype.names:
                                old_data = read_group['Events'][()]
                                new_data = np.empty(old_data.size, dtype=[('mean', float), ('stdv', float),
                                                                          ('start', int), ('length', int)])
                                new_data[:]['mean'] = old_data['mean']
                                new_data[:]['stdv'] = np.sqrt(old_data['variance'])
                                new_data[:]['start'] = old_data['start']
                                new_data[:]['length'] = old_data['length']
                                del read_group['Events']
                                read_group.create_dataset('Events', data=new_data, compression='gzip')

            # Update the version number.
            handle.attrs['file_version'] = CURRENT_FAST5_VERSION

    @staticmethod
    def read_summary_data(fname, component):
        """ Read summary data suitable to encode as a json packet.

        :param fname: The fast5 file to pull the summary data from.
        :param component: The component name to pull summary data for.

        :returns: A dictionary containing the summary data.
        """
        summary = {}
        with Fast5File(fname, mode='r') as fh:
            summary['tracking_id'] = fh.get_tracking_id()
            summary['channel_id'] = fh.get_channel_info()
            read_info = fh.status.read_info
            read_summary = []
            for read in read_info:
                read_summary.append({'read_number': read.read_number,
                                     'read_id': read.read_id,
                                     'start_time': read.start_time,
                                     'duration': read.duration,
                                     'start_mux': read.start_mux})
            summary['reads'] = read_summary
            analyses_list = fh.list_analyses(component)
            _, group_names = zip(*analyses_list)
            group_names = sorted(group_names)
            group = group_names[-1]
            summary['software'] = fh.get_analysis_attributes(group)
            summary['software']['component'] = group[:-4]

            summary['data'] = fh.get_summary_data(group)
            summary['filename'] = os.path.basename(fname)
        return summary

    ##########################
    #
    #  Private methods below
    #
    ##########################

    def _get_only_read_number(self):
        read_number = self.status.read_info[0].read_number
        return read_number

    def _initialise_file(self):
        try:
            if self.mode in ['w', 'w-', 'x']:
                with h5py.File(self.filename, self.mode) as fh:
                    fh.attrs['file_version'] = CURRENT_FAST5_VERSION
                    fh.create_group('Analyses')
                    fh.create_group('Raw/Reads')
                    fh.create_group(self.global_key + 'channel_id')
                    fh.create_group(self.global_key + 'context_tags')
                    fh.create_group(self.global_key + 'tracking_id')
                self.mode = 'r+'
            self.status = Fast5Info(self.filename)
            if self.status.valid:
                self.handle = h5py.File(self.filename, self.mode)
        except Exception:
            raise Fast5FileTypeError("Failed to initialise single-read Fast5File: '{}'".format(self.filename))


class EmptyFast5(Fast5File):
    def _initialise_file(self):
        # Enable creation of Fast5File without metadata, which we will populate later
        self.handle = h5py.File(self.filename, self.mode)
        self.handle.attrs['file_version'] = CURRENT_FAST5_VERSION

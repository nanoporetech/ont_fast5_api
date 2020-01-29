""" Helper class for working with event detection type analyses.
"""
import numpy as np

from ont_fast5_api.analysis_tools.base_tool import BaseTool


class EventDetectionTools(BaseTool):
    """ Provides helper methods specific to event detection analyses.
    """

    group_id = 'EventDetection'
    analysis_id = 'event_detection'

    def set_event_data(self, data, read_attrs):
        """ Set event data with the specied attributes.
        
        :param data: Event data table.
        :param read_attrs: Attributes to put on the read group. This must include
            the read_number, which must refer to a read present in the object. The
            attributes should not include the standard read attributes:

             * read_id
             * start_time
             * duration
             * start_mux

            Those will be pulled from the read information already present in the
            object for the specified read.
        """
        if self.handle.mode == 'r':
            raise Exception('File is not open for writing.')
        read_number = read_attrs['read_number']
        read_group = '{}/Reads/Read_{}'.format(self.group_name, read_number)
        read_info = self.handle.status.read_info
        read_number_map = self.handle.status.read_number_map
        index = read_number_map.get(read_number)
        if index is None:
            raise Exception('Cannot add event detection data for a read that does not exist.')
        info = read_info[index]
        read_attrs.update({'read_id': info.read_id,
                           'start_time': info.start_time,
                           'duration': info.duration,
                           'start_mux': info.start_mux,
                           'median_before': info.median_before})
        attrs = self.handle.get_analysis_attributes(read_group)
        if attrs is None:
            self.handle.add_analysis_subgroup(self.group_name, 'Reads/Read_{}'.format(read_number),
                                              attrs=read_attrs)
            self.handle.add_analysis_dataset(read_group, 'Events', data)
        else:
            raise Exception('Event detection data already exists for this analysis and read.')

    def get_event_data(self, read_number=None, time_in_seconds=False):
        """ Get event data for the specified (or only) read.
        
        :param read_number: The read number to grab event data for. If this
            is None, and there is only one read, it will grab event data for
            that read.
        :param time_in_seconds: If True, this will convert (if necessary) the
            start and length fields from samples to seconds. If they are already
            in seconds, this option has no effect.
        :return: A tuple containing the event data, and the read attributes.
        """
        read_info = self.handle.status.read_info
        if read_number is None:
            if len(read_info) != 1:
                raise Exception('Must specify a read number if there is not exactly 1 read.')
            read_number = read_info[0].read_number
        else:
            read_numbers = [info.read_number for info in read_info]
            if read_number not in read_numbers:
                raise Exception('Specified read does not exist.')
        group = '{}/Reads/Read_{}'.format(self.group_name, read_number)
        attrs = self.handle.get_analysis_attributes(group)
        dataset = self.handle.get_analysis_dataset(group, 'Events', skip_decoding=True)
        if dataset is None:
            raise Exception('Read number {} has no event data.'.format(read_number))
        if time_in_seconds and dataset['start'].dtype.kind in ['i', 'u']:
            channel_info = self.handle.get_channel_info()
            sample_size = 1.0 / channel_info['sampling_rate']
            descr = [(x[0], 'float64') if x[0] in ('start', 'length') else x
                     for x in dataset.dtype.descr]
            with dataset.astype(np.dtype(descr)):
                data = dataset[()]
            data['start'] *= sample_size
            data['length'] *= sample_size
        else:
            data = dataset[()]
        return data, attrs

    def has_event_data(self, read_number=None):
        """ Find out if the specified (or only) read has event data.

        :param read_number: The read number to check for event data. If this
            is ``None``, and there is only one read, it will check that read.
        :returns: True if event data exists for the read number.
        """
        read_info = self.handle.status.read_info
        if read_number is None:
            if len(read_info) != 1:
                raise Exception('Must specify a read number if there is not exactly 1 read.')
            read_number = read_info[0].read_number
        else:
            read_numbers = [info.read_number for info in read_info]
            if read_number not in read_numbers:
                raise Exception('Specified read does not exist.')
        group = '{}/Reads/Read_{}'.format(self.group_name, read_number)
        dataset = self.handle.get_analysis_dataset(group, 'Events', skip_decoding=True)
        return dataset is not None

    ##########################
    #
    #  Private methods below
    #
    ##########################

    def _new_analysis(self, meta, config):
        if self.handle.mode == 'r':
            raise Exception('Cannot create new event detection group. File is not open for writing.')
        self.handle.add_analysis('event_detection', self.group_name, meta, config)
        self.handle.add_analysis_subgroup(self.group_name, 'Reads')

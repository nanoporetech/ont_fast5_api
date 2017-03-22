""" Helper class for working with 1D basecall type analyses.
"""
import numpy as np
from ont_fast5_api.fast5_file import Fast5File


class Basecall1DTools(object):
    """ Provides helper methods specific to 1D basecall analyses.
    """
    
    def __init__(self, source, mode='r', group_name=None, meta=None, config=None):
        """ Create a new basecall 1d tools object.
        
        :param source: Either an open Fast5File object, or a filename
            of a fast5 file.
        :param mode: The open mode (r or r+). Only if a filename is used
            for the source argument.
        :param group_name: The specific basecall 1d analysis instance
            you are interested in.
        :param meta: Metadata for a new 1d basecall analysis.
        :param config: Configuration data for a new 1d basecall analysis.
        
        To create a new 1d basecall analysis, provide a group name that
        does not already exist, and an optional dictionary with the metadata.
        The following fields are recommended, as a minimum:
            
            * name - The name of the basecall software used.
            * time_stamp - The time at which the analysis was performed.
        
        If the group name already exists, the "meta" parameter is ignored. If
        the specified group has a "component" attribute, and its value is not
        "basecall_1d", an exception will be thrown.
        """
        if isinstance(source, Fast5File):
            self.handle = source
            self.close_handle_when_done = False
        elif isinstance(source, str):
            self.handle = Fast5File(source, mode)
            self.close_handle_when_done = True
        else:
            raise Exception('Unrecognized type for argument "source".')
        if group_name is None:
            group_name = self.handle.get_latest_analysis('Basecall_1D')
            if group_name is None:
                raise Exception('No Basecall_1D analysis group found in file.')
        self.group_name = group_name
        attrs = self.handle.get_analysis_attributes(group_name)
        if attrs is None:
            self.handle.add_analysis('basecall_1d', group_name, meta, config)
            attrs = self.handle.get_analysis_attributes(group_name)
        if 'component' in attrs and attrs['component'] != 'basecall_1d':
            raise Exception('Analysis does not appear to be a 1d basecall component.')
    
    def __enter__(self):
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return False
    
    def close(self):
        """ Closes the object.
        """
        if self.handle and self.close_handle_when_done:
            self.handle.close()

    def get_event_data(self, section):
        """ Return either the template or complement event data, if present.
        
        :param section: Either template or complement.
        :return: Event data table.
        """
        event_group = '{}/BaseCalled_{}'.format(self.group_name, section)
        data = self.handle.get_analysis_dataset(event_group, 'Events')
        return data

    def add_event_data(self, section, data):
        """ Add template or complement basecalled event data.
        
        :param section: Either template or complement.
        :param data: Event data table to be written.
        """
        event_group = 'BaseCalled_{}'.format(section)
        if not event_group in self.handle.handle['Analyses/{}'.format(self.group_name)]:
            self.handle.add_analysis_subgroup(self.group_name, event_group)
        self.handle.add_analysis_dataset('{}/{}'.format(self.group_name, event_group), 'Events', data)

    def get_called_sequence(self, section, fastq=False):
        """ Return either the template or complement sequence data, if present.
        
        :param section: Either template or complement.
        :param fastq: If True, return a single, multiline fastq string. If
            False, return a tuple of (name, sequence, qstring).
        :return: Either the fastq string or the (name, sequence, qstring) tuple.
        """
        event_group = '{}/BaseCalled_{}'.format(self.group_name, section)
        data = self.handle.get_analysis_dataset(event_group, 'Fastq')
        if fastq:
            return data
        name, sequence, _,qstring = data.strip().split('\n')
        name = name[1:]
        return name, sequence, qstring

    def add_called_sequence(self, section, name, sequence, qstring):
        """ Add template or complement basecalled sequence data.
        
        :param section: Either template or complement.
        :param name: The record ID to use for the fastq.
        :param sequence: The called sequence.
        :param qstring: The quality string.
        """
        event_group = 'BaseCalled_{}'.format(section)
        if not event_group in self.handle.handle['Analyses/{}'.format(self.group_name)]:
            self.handle.add_analysis_subgroup(self.group_name, event_group)
        fastq_text = '@{}\n{}\n+\n{}\n'.format(name, sequence, qstring)
        fastq_arr = np.array(fastq_text, dtype=str)
        self.handle.add_analysis_dataset('{}/{}'.format(self.group_name, event_group), 'Fastq', fastq_arr)

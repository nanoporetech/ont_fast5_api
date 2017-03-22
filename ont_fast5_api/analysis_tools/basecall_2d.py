""" Helper class for working with 2D basecall type analyses.
"""
import numpy as np
from ont_fast5_api.fast5_file import Fast5File


class Basecall2DTools(object):
    """ Provides helper methods specific to 2D basecall analyses.
    """
    
    def __init__(self, source, mode='r', group_name=None, meta=None, config=None):
        """ Create a new basecall 2d tools object.
        
        :param source: Either an open Fast5File object, or a filename
            of a fast5 file.
        :param mode: The open mode (r or r+). Only if a filename is used
            for the source argument.
        :param group_name: The specific basecall 2d analysis instance
            you are interested in.
        :param meta: Metadata for a new 2d basecall analysis.
        :param config: Configuration data for a new 2d basecall analysis.
        
        To create a new 2d basecall analysis, provide a group name that
        does not already exist, and an optional dictionary with the metadata.
        The following fields are recommended, as a minimum:
            
            * name - The name of the basecall software used.
            * time_stamp - The time at which the analysis was performed.
        
        If the group name already exists, the "meta" parameter is ignored. If
        the specified group has a "component" attribute, and its value is not
        "basecall_2d", an exception will be thrown.
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
            group_name = self.handle.get_latest_analysis('Basecall_2D')
            if group_name is None:
                raise Exception('No Basecall_2D analysis group found in file.')
        self.group_name = group_name
        attrs = self.handle.get_analysis_attributes(group_name)
        if attrs is None:
            self.handle.add_analysis('basecall_2d', group_name, meta, config)
            attrs = self.handle.get_analysis_attributes(group_name)
        if 'component' in attrs and attrs['component'] != 'basecall_2d':
            msg = 'Analysis does not appear to be a 2d basecall component.'
            raise Exception(msg)

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

    def get_prior_alignment(self):
        """ Return the prior alignment that was used for 2D basecalling.

        :return: Alignment data table.
        """
        data_group = '{}/HairpinAlign'.format(self.group_name)
        data = self.handle.get_analysis_dataset(data_group, 'Alignment')
        return data

    def get_2d_call_alignment(self):
        """ Return the alignment and model_states from the 2D basecall.

        :return: Alignment data table.
        """
        data_group = '{}/BaseCalled_2D'.format(self.group_name)
        data = self.handle.get_analysis_dataset(data_group, 'Alignment')
        return data

    def add_prior_alignment(self, data):
        """ Add template or complement basecalled event data.
        
        :param data: Alignment table to be written.
        """
        path = 'Analyses/{}'.format(self.group_name)
        if 'HairpinAlign' not in self.handle.handle[path]:
            self.handle.add_analysis_subgroup(self.group_name, 'HairpinAlign')

        path = '{}/HairpinAlign'.format(self.group_name)
        self.handle.add_analysis_dataset(path, 'Alignment', data)

    def add_2d_call_alignment(self, data):
        """ Add the alignment and model_state data table..
        
        :param data: Alignment and model_state table to be written.
        """
        path = 'Analyses/{}'.format(self.group_name)
        if 'BaseCalled_2D' not in self.handle.handle[path]:
            self.handle.add_analysis_subgroup(self.group_name, 'BaseCalled_2D')

        path = '{}/BaseCalled_2D'.format(self.group_name)
        self.handle.add_analysis_dataset(path, 'Alignment', data)

    def get_called_sequence(self, fastq=False):
        """ Return the 2D sequence data, if present.

        :param fastq: If True, return a single, multiline fastq string. If
            False, return a tuple of (name, sequence, qstring).
        :return: Either the fastq string or the (name, sequence, qstring)
            tuple.
        :rtype: tuple or str
        """
        event_group = '{}/BaseCalled_2D'.format(self.group_name)
        data = self.handle.get_analysis_dataset(event_group, 'Fastq')
        if fastq:
            return data
        name, sequence, _, qstring = data.strip().split('\n')
        name = name[1:]
        return name, sequence, qstring

    def add_called_sequence(self, name, sequence, qstring):
        """ Add 2D basecalled sequence data.

        :param name: The record ID to use for the fastq.
        :param sequence: The called sequence.
        :param qstring: The quality string.
        """
        event_group = 'BaseCalled_2D'
        path = 'Analyses/{}'.format(self.group_name)
        if event_group not in self.handle.handle[path]:
            self.handle.add_analysis_subgroup(self.group_name, 'BaseCalled_2D')
        fastq_text = '@{}\n{}\n+\n{}\n'.format(name, sequence, qstring)
        fastq_arr = np.array(fastq_text, dtype=str)
        path = '{}/BaseCalled_2D'.format(self.group_name)
        self.handle.add_analysis_dataset(path, 'Fastq', fastq_arr)

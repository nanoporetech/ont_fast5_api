""" Helper class for working with 1D basecall type analyses.
"""
import numpy as np

from ont_fast5_api.analysis_tools.base_tool import BaseTool


class Basecall1DTools(BaseTool):
    """ Provides helper methods specific to 1D basecall analyses.
    """
    group_id = 'Basecall_1D'
    analysis_id = 'basecall_1d'


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
        """ Return either the called sequence data, if present.
        
        :param section: ['template', 'complement' or '2D']
        :param fastq: If True, return a single, multiline fastq string. If
            False, return a tuple of (name, sequence, qstring).
        :return: Either the fastq string or the (name, sequence, qstring) tuple.
        """

        event_group = '{}/BaseCalled_{}'.format(self.group_name, section)
        data = self.handle.get_analysis_dataset(event_group, 'Fastq')
        if data is None:
            raise KeyError("No fastq data in: {} {}".format(event_group, self.filename))
        if fastq:
            return data
        name, sequence, _, qstring = data.strip().split('\n')
        name = name[1:]
        return name, sequence, qstring

    def add_called_sequence(self, section, name, sequence, qstring):
        """ Add basecalled sequence data
        
        :param section: ['template', 'complement' or '2D']
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

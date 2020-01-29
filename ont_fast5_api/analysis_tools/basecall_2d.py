""" Helper class for working with 2D basecall type analyses.
"""
import warnings
from ont_fast5_api.analysis_tools.basecall_1d import Basecall1DTools


class Basecall2DTools(Basecall1DTools):
    """ Provides helper methods specific to 2D basecall analyses.
    """

    group_id = 'Basecall_2D'
    analysis_id = 'basecall_2d'

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

    def get_called_sequence(self, section=None, fastq=False):
        """ Return either the called sequence data, if present.
        :param section: ['template', 'complement' or '2D']
        :param fastq: If True, return a single, multiline fastq string. If
            False, return a tuple of (name, sequence, qstring).
        :return: Either the fastq string or the (name, sequence, qstring) tuple.
        """
        if section != "2D":
            warnings.warn("Basecall2DTools.get_called_sequence() should specify section='2D'", DeprecationWarning)
            # Backwards compatibilty to 0.3.3, if no "2D" section, bump args by 1 and pass to super
            if section == None:
                # We assume that a named arg or no-arg was given
                return super(Basecall2DTools, self).get_called_sequence("2D", fastq)
            # We assume that a single unnamed arg was given for fastq
            return super(Basecall2DTools, self).get_called_sequence("2D", section)
        return super(Basecall2DTools, self).get_called_sequence(section, fastq)

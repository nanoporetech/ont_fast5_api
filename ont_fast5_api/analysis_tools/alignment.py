""" Helper class for working with alignment type analyses.
"""
import numpy as np

from ont_fast5_api.analysis_tools.base_tool import BaseTool
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.segmentation import SegmentationTools
from ont_fast5_api.fast5_read import Fast5Read


class AlignmentTools(BaseTool):
    """ Provides helper methods specific to alignment analyses.
    """
    
    def __init__(self, source, mode='r', group_name=None, meta=None, config=None):
        """ Create a new alignment tools object.
        
        :param source: Either an open Fast5File object, or a filename
            of a fast5 file.
        :param mode: The open mode (r or r+). Only if a filename is used
            for the source argument.
        :param group_name: The specific alignment analysis instance
            you are interested in.
        :param meta: Metadata for a new alignment analysis.
        :param config: Configuration data for a new alignment analysis.
        
        To create a new alignment analysis, provide a group name that
        does not already exist, and an optional dictionary with the metadata.
        The following fields are recommended, as a minimum:
            
            * name - The name of the basecall software used.
            * time_stamp - The time at which the analysis was performed.
        
        If the group name already exists, the "meta" parameter is ignored. If
        the specified group has a "component" attribute, and its value is not
        "alignment", an exception will be thrown.
        """
        if isinstance(source, Fast5Read):
            self.handle = source
            self.close_handle_when_done = False
        elif isinstance(source, str):
            self.handle = Fast5File(source, mode)
            self.close_handle_when_done = True
        else:
            raise Exception('Unrecognized type for argument "source".')
        if group_name is None:
            group_name = self.handle.get_latest_analysis('Alignment')
            if group_name is None:
                raise Exception('No Alignment analysis group found in file.')
        self.group_name = group_name
        attrs = self.handle.get_analysis_attributes(group_name)
        if attrs is None:
            if meta is None:
                meta = {}
            self.handle.add_analysis('alignment', group_name, meta, config)
            attrs = self.handle.get_analysis_attributes(group_name)
        if ('component' in attrs
                and attrs['component'] not in ['alignment',
                                               'calibration_strand']):
            self.close()
            raise Exception('Analysis does not appear to be an alignment component.')

    def get_results(self):
        """ Get details about the alignments that have been performed.

        :return: A dict of dicts.

        The keys of the top level are 'template', 'complement' and '2d'.
        Each of these dicts contains the following fields:

            * status: Can be 'no data', 'no match found', or 'match found'.
            * direction: Can be 'forward', 'reverse'.
            * ref_name: Name of reference.
            * ref_span: Section of reference aligned to, as a tuple (start, end).
            * seq_span: Section of the called sequence that aligned, as a tuple (start, end).
            * seq_len: Total length of the called sequence.
            * num_aligned: Number of bases that aligned to bases in the reference.
            * num_correct: Number of aligned bases that match the reference.
            * num_deletions: Number of bases in the aligned section of the
                reference that are not aligned to bases in the called sequence.
            * num_insertions: Number of bases in the aligned section of the called
                sequence that are not aligned to bases in the reference.
            * identity: The fraction of aligned bases that are correct (num_correct /
                num_aligned).
            * accuracy: The overall basecall accuracy, according to the alignment.
                (num_correct / (num_aligned + num_deletions + num_insertions)).
        
        Note that if the status field is not 'match found', then all the other
        fields will be absent.
        """
        summary = self.handle.get_summary_data(self.group_name)
        results = {'template': {'status': 'no data'},
                   'complement': {'status': 'no data'},
                   '2d': {'status': 'no data'}}
        if 'genome_mapping_template' in summary:
            results['template'] = self._get_results(summary['genome_mapping_template'])
        if 'genome_mapping_complement' in summary:
            results['complement'] = self._get_results(summary['genome_mapping_complement'])
        if 'genome_mapping_2d' in summary:
            results['2d'] = self._get_results(summary['genome_mapping_2d'])
        return results

    def get_alignment_data(self, section):
        """ Get the alignment SAM and Fasta, if present.
        
        :param section: Can be 'template', 'complement', or '2d'.
        :return: A tuple containing the SAM and the section of the reference
            aligned to (both as strings). Returns None if no alignment is
            present for that section.
        """
        subgroup = '{}/Aligned_{}'.format(self.group_name, section)
        sam = self.handle.get_analysis_dataset(subgroup, 'SAM')
        fasta = self.handle.get_analysis_dataset(subgroup, 'Fasta')
        if sam is None or fasta is None:
            return None
        sequence = fasta.split('\n')[1]
        return sam, sequence

    def add_alignment_data(self, section, sam, sequence):
        """ Add the SAM and Fasta alignment data for a section.
        
        :param section: Can be 'template', 'complement', or '2d'.
        :param sam: A string containing the SAM contents.
        :param sequence: A string containing the section of the
            reference the basecall aligned to.
        """
        subgroup = 'Aligned_{}'.format(section)
        if not subgroup in self.handle.handle['Analyses/{}'.format(self.group_name)]:
            self.handle.add_analysis_subgroup(self.group_name, subgroup)
        sam_arr = np.array(sam, dtype=str)
        self.handle.add_analysis_dataset('{}/{}'.format(self.group_name, subgroup), 'SAM', sam_arr)
        fasta_arr = np.array('>{}\n{}\n'.format(section, sequence), dtype=str)
        self.handle.add_analysis_dataset('{}/{}'.format(self.group_name, subgroup), 'Fasta', fasta_arr)

    def calculate_speed(self, section, alignment_results=None):
        """ Calculate speed using alignment information.

        :param section: The section (template or complement) we're calculating
            speed for.
        :param alignment_results: Optional dictionary of the alignment summary,
            so that speed can be calculated without having to write the summary
            out to the fast5 file first.
        :return: Speed in bases per second or zero if the speed could not be
            calculated.

        The only reliable way we have of finding out how many bases have gone through the pore is by
        looking at how much of the reference the sequence aligned to. This takes that information and
        uses it to calculate speed in reference-bases-per-second.
        """
        speed = 0.0
        if alignment_results:
            results = self._get_results(alignment_results)
        else:
            results = self.get_results()[section]
        if results['status'] != 'match found':
            return 0.0
        ref_span = results['ref_span']
        ref_len = ref_span[1] - ref_span[0]
        seq_span = results['seq_span']
        seq_len = seq_span[1] - seq_span[0]
        total_len = results['seq_len']

        sample_rate = self.handle.get_channel_info()['sampling_rate']

        # We need the duration from the segmentation results
        chain = self.handle.get_chain(self.group_name)
        if chain is not None:
            segmentation_group = dict(chain).get('segmentation')
        else:
            segmentation_group = None
        duration = 0
        if segmentation_group is not None:
            with SegmentationTools(self.handle, group_name=segmentation_group) as seg:
                summary = seg.get_results()
                if summary is not None:
                    duration = summary['duration_{}'.format(section)]
        if duration == 0:
            return 0.0

        normalized_duration = duration * seq_len / float(total_len)
        speed = sample_rate * ref_len / normalized_duration
        return speed

    ##########################
    #
    #  Private methods below
    #
    ##########################

    def _get_results(self, summary):
        results = {'status': 'no data'}
        ref_name = summary['genome']
        if ref_name == 'no_match':
            results['status'] = 'no match found'
            return results
        results['status'] = 'match found'
        results['direction'] = 'forward'
        if ref_name.endswith('_rc'):
            ref_name = ref_name[:-3]
            results['direction'] = 'reverse'
        results['ref_name'] = ref_name
        results['ref_span'] = (summary['genome_start'], summary['genome_end'])
        results['seq_span'] = (summary['strand_start'], summary['strand_end'])
        results['seq_len'] = summary['num_events']
        results.update({key: summary[key] for key in ['num_aligned', 'num_correct', 'num_insertions',
                                                      'num_deletions', 'identity', 'accuracy']})
        return results

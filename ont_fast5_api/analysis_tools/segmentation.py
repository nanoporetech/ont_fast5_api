""" Helper class for working with segmentation type analyses.
"""
import numpy as np

from ont_fast5_api.analysis_tools.base_tool import BaseTool
from ont_fast5_api.analysis_tools.event_detection import EventDetectionTools


class SegmentationTools(BaseTool):
    """ Provides helper methods specific to segmentation analyses.
    """
    group_id = 'Segmentation'
    analysis_id = 'segmentation'

    def get_results(self):
        """ Returns the segmentation summary data.
        
        This data is normalized, to eliminate differences in what is stored
        for different types of segmentation analyses.
        
        The following fields are output:

        * has_template - True if the segmentation found template data.
        * has_complement - True if the segmentation found complement data.
        * first_sample_template - The first sample of the template data in
            the raw data. Only present if has_template is True.
        * duration_template - The duration (in samples) of the template
            data. Only present if has_template is True.
        * first_sample_complement - The first sample of the complement data
            in the raw data. Only present if has_complement is True.
        * duration_complement - The duration (in samples) of the complement
            data. Only present if has_complement is True.
            
        """
        summary = self._get_summary_data()
        if summary is None:
            results = {'has_template': False,
                       'has_complement': False}
        else:
            results = {}
            if 'has_template' in summary:
                results['has_template'] = bool(summary['has_template'])
            else:
                results['has_template'] = True if summary['num_temp'] > 0 else False
            if 'has_complement' in summary:
                results['has_complement'] = bool(summary['has_complement'])
            else:
                results['has_complement'] = True if summary['num_comp'] > 0 else False
            need_raw_info = False
            if results['has_template']:
                if 'start_index_temp' in summary:
                    summary['start_event_template'] = summary['start_index_temp']
                    summary['end_event_template'] = summary['end_index_temp']
                if 'first_sample_template' not in summary:
                    need_raw_info = True
            if results['has_complement']:
                if 'start_index_comp' in summary:
                    summary['start_event_complement'] = summary['start_index_comp']
                    summary['end_event_complement'] = summary['end_index_comp']
                if 'first_sample_complement' not in summary:
                    need_raw_info = True
            if need_raw_info:
                self._get_raw_info(summary)
            if results['has_template']:
                results['first_sample_template'] = summary['first_sample_template']
                results['duration_template'] = summary['duration_template']
                if 'start_event_template' in summary:
                    results['start_event_template'] = summary['start_event_template']
                    results['end_event_template'] = summary['end_event_template']
            if results['has_complement']:
                results['first_sample_complement'] = summary['first_sample_complement']
                results['duration_complement'] = summary['duration_complement']
                if 'start_event_complement' in summary:
                    results['start_event_complement'] = summary['start_event_complement']
                    results['end_event_complement'] = summary['end_event_complement']
        return results

    def get_event_data(self, section, time_in_seconds=False):
        """ Get the template or complement event data.
        
        :param section: Either template, complement, or both.
        :param time_in_seconds: Return the start and length fields
            in seconds, rather than samples.
        :return: The event dataset for the section. If section=both
            then it returns a tuple with both sections. Returns None
            if the section does not exist.
        """
        if section not in ['template', 'complement', 'both']:
            raise Exception('Unrecognized section: {} Expected: "template", "complement" or "both"'.format(section))
        results = self.get_results()
        if results is None:
            return None, None if section is 'both' else None
        if section == 'both':
            sections = ['template', 'complement']
        else:
            sections = [section]
        evdet_group, _ = self._find_event_data()
        with EventDetectionTools(self.handle, group_name=evdet_group) as evdet:
            event_data, _ = evdet.get_event_data(time_in_seconds=time_in_seconds)
        datasets = [None, None]
        for n, this_section in enumerate(sections):
            if not results['has_{}'.format(this_section)]:
                continue
            ev1 = results['start_event_{}'.format(this_section)]
            ev2 = results['end_event_{}'.format(this_section)]
            datasets[n] = event_data[ev1:ev2]
        if section == 'both':
            return tuple(datasets)
        return datasets[0]

    def get_raw_data(self, section, scale=False):
        """ Get the template or complement raw data.
        
        :param section: Either template, complement, or both.
        :param scale: Scale the raw data to pA.
        :return:  The raw data for the section. If section=both
            then it returns a tuple with both sections. Returns None
            if the section does not exist.
        """
        results = self.get_results()
        datasets = [None, None]
        if section == 'both':
            sections = ['template', 'complement']
        else:
            sections = [section]
        for n, this_section in enumerate(sections):
            if not results['has_{}'.format(this_section)]:
                continue
            start = results['first_sample_{}'.format(this_section)]
            dur = results['duration_{}'.format(this_section)]
            datasets[n] = self.handle.get_raw_data(start=start, end=start+dur, scale=scale)
        if section == 'both':
            return tuple(datasets)
        return datasets[0]


    ##########################
    #
    #  Private methods below
    #
    ##########################
    
    def _get_summary_data(self):
        summary = self.handle.get_summary_data(self.group_name)
        if summary is None:
            return None
        if 'segmentation' in summary:
            results = summary['segmentation']
        elif 'split_hairpin' in summary:
            results = summary['split_hairpin']
        else:
            results = None
        return results

    def _find_event_data(self):
        attrs = self.handle.get_analysis_attributes(self.group_name)
        evdet_group = attrs.get('event_detection')
        if evdet_group is None:
            evdet_group = self.handle.get_latest_analysis('EventDetection')
        else:
            evdet_group = evdet_group[9:]
        if evdet_group is None:
            return None
        # We directly use the Fast5Read interface here, rather than the
        # EventDetectionTools one, because we don't want to load the entire
        # event table into memory.
        read_info = self.handle.status.read_info[0] # We assume only one read.
        read_number = read_info.read_number
        event_table_group = '{}/Reads/Read_{}'.format(evdet_group, read_number)
        dataset = self.handle.get_analysis_dataset(event_table_group, 'Events', skip_decoding=True)
        return evdet_group, dataset

    def _get_raw_info(self, summary):
        _, dataset = self._find_event_data()
        read_info = self.handle.status.read_info[0] # We assume only one read.
        if dataset is None:
            summary['first_sample_template'] = None
            summary['duration_template'] = None
            summary['first_sample_complement'] = None
            summary['duration_complement'] = None
            return
        if summary.get('start_event_template', -1) >= 0:
            ev1 = summary['start_event_template']
            ev2 = summary['end_event_template']
            summary['first_sample_template'] = dataset[ev1]['start'] - read_info.start_time
            end = dataset[ev2-1]['start'] + dataset[ev2-1]['length'] - read_info.start_time
            summary['duration_template'] = end - summary['first_sample_template']
        if summary.get('start_event_complement', -1) >= 0:
            ev1 = summary['start_event_complement']
            ev2 = summary['end_event_complement']
            summary['first_sample_complement'] = dataset[ev1]['start'] - read_info.start_time
            end = dataset[ev2-1]['start'] + dataset[ev2-1]['length'] - read_info.start_time
            summary['duration_complement'] = end - summary['first_sample_complement']

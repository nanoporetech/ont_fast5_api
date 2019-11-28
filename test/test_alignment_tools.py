import os
import numpy as np
from ont_fast5_api.fast5_file import Fast5File
from ont_fast5_api.analysis_tools.alignment import AlignmentTools
from test.helpers import TestFast5ApiHelper


class TestAlignmentTools(TestFast5ApiHelper):

    def test_001_put_and_retrieve(self):
        fname = self.generate_temp_filename()
        summary_temp = {'genome': 'Lambda',
                        'genome_start': 100,
                        'genome_end': 200,
                        'strand_start': 1,
                        'strand_end': 101,
                        'num_events': 125,
                        'num_aligned': 92,
                        'num_correct': 87,
                        'num_insertions': 8,
                        'num_deletions': 8,
                        'identity': 0.9457,
                        'accuracy': 0.8056}
        summary_comp = {'genome': 'Lambda_rc',
                        'genome_start': 100,
                        'genome_end': 200,
                        'strand_start': 0,
                        'strand_end': 96,
                        'num_events': 120,
                        'num_aligned': 90,
                        'num_correct': 88,
                        'num_insertions': 6,
                        'num_deletions': 10,
                        'identity': 0.9778,
                        'accuracy': 0.8302}
        summary_2d = {'genome': 'Lambda',
                      'genome_start': 100,
                      'genome_end': 200,
                      'strand_start': 0,
                      'strand_end': 100,
                      'num_events': 125,
                      'num_aligned': 98,
                      'num_correct': 96,
                      'num_insertions': 4,
                      'num_deletions': 4,
                      'identity': 0.9796,
                      'accuracy': 0.9057}
        sam1 = 'Dummy string for template SAM.'
        sam2 = 'Dummy string for complement SAM.'
        sam3 = 'Dummy string for 2D SAM.'
        sequence1 = ''.join(np.random.choice(['A', 'C', 'G', 'T'], 100))
        bc = {'A': 'T', 'T': 'A', 'C': 'G', 'G': 'C'}
        sequence2 = ''.join([bc[letter] for letter in sequence1[::-1]])
        with Fast5File(fname, mode='w') as fh:
            fh.add_channel_info({'channel_number': 1,
                                 'sampling_rate': 4000,
                                 'digitisation': 8192,
                                 'range': 819.2,
                                 'offset': 0})
            fh.add_read(12, 'unique_snowflake', 12345, 4000, 0, 120.75)
            attrs = {'name': 'test', 'version': 0, 'time_stamp': 'just now', 'component': 'segmentation'}
            fh.add_analysis('segmentation', 'Segmentation_000', attrs)
            seg_data = {'has_template': 1,
                        'has_complement': 1,
                        'first_sample_template': 0,
                        'duration_template': 2000,
                        'first_sample_complement': 2000,
                        'duration_complement': 2000}
            fh.set_summary_data('Segmentation_000', 'segmentation', seg_data)
            attrs['component'] = 'alignment'
            attrs['segmentation'] = 'Analyses/Segmentation_000'
            fh.add_analysis('alignment', 'Alignment_000', attrs)
            fh.set_summary_data('Alignment_000', 'genome_mapping_template', summary_temp)
            fh.set_summary_data('Alignment_000', 'genome_mapping_complement', summary_comp)
            fh.set_summary_data('Alignment_000', 'genome_mapping_2d', summary_2d)
            with AlignmentTools(fh, group_name='Alignment_000') as align:
                align.add_alignment_data('template', sam1, sequence1)
                align.add_alignment_data('complement', sam2, sequence2)
                align.add_alignment_data('2d', sam3, sequence1)
        with Fast5File(fname, mode='r') as fh:
            with AlignmentTools(fh, group_name='Alignment_000') as align:
                sam, seq = align.get_alignment_data('template')
                self.assertEqual(sam1, sam)
                self.assertEqual(sequence1, seq)
                sam, seq = align.get_alignment_data('complement')
                self.assertEqual(sam2, sam)
                self.assertEqual(sequence2, seq)
                sam, seq = align.get_alignment_data('2d')
                self.assertEqual(sam3, sam)
                self.assertEqual(sequence1, seq)
                results = align.get_results()
                speed_temp = align.calculate_speed('template')
                speed_comp = align.calculate_speed('complement')
                # Make sure we can calculate speed using only what's in the
                # summary
                summary = fh.get_summary_data('Alignment_000')
                template_summary = summary['genome_mapping_template']
                summary_speed_temp = align.calculate_speed('template',
                                                           template_summary)
        self.assertEqual(250, speed_temp)
        self.assertEqual(250, speed_comp)
        self.assertEqual(speed_temp, summary_speed_temp)
        self.assertDictEqual({'status': 'match found',
                              'direction': 'forward',
                              'ref_name': 'Lambda',
                              'ref_span': (100, 200),
                              'seq_span': (1, 101),
                              'seq_len': 125,
                              'num_aligned': 92,
                              'num_correct': 87,
                              'num_insertions': 8,
                              'num_deletions': 8,
                              'identity': 0.9457,
                              'accuracy': 0.8056}, results['template'])
        self.assertDictEqual({'status': 'match found',
                              'direction': 'reverse',
                              'ref_name': 'Lambda',
                              'ref_span': (100, 200),
                              'seq_span': (0, 96),
                              'seq_len': 120,
                              'num_aligned': 90,
                              'num_correct': 88,
                              'num_insertions': 6,
                              'num_deletions': 10,
                              'identity': 0.9778,
                              'accuracy': 0.8302}, results['complement'])
        self.assertDictEqual({'status': 'match found',
                              'direction': 'forward',
                              'ref_name': 'Lambda',
                              'ref_span': (100, 200),
                              'seq_span': (0, 100),
                              'seq_len': 125,
                              'num_aligned': 98,
                              'num_correct': 96,
                              'num_insertions': 4,
                              'num_deletions': 4,
                              'identity': 0.9796,
                              'accuracy': 0.9057}, results['2d'])

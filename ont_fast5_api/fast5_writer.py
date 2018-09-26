""" Module for writing fast5 read files. """
import os
from collections import defaultdict
from ont_fast5_api.fast5_file import Fast5File


REQUIRED_FIELDS = ['duration',
                   'median_before',
                   'read_id',
                   'read_number',
                   'scaling_used',
                   'start_mux',
                   'start_time']


class Fast5Writer(object):
    """ Write fast5 read files. """

    def __init__(self, path, basename, reads_per_file=1, tracking_id=None,
                 context_tags=None, config=None):
        """ Constructor. Initializes the stream.

        :param path: The path to write the files to.
        :param basename: The main part of the filename to use.
        :param reads_per_file: The maximum number of reads to write to a file.
            All reads written to a file must be from the same channel.
        :param tracking_id: Dictionary with tracking id to write to the file.
        :param context_tags: Dictionary of context tags to write to the file.
        :param config: Dictionary of dictionaries containing configuration
            parameters for event detection.
        """
        self._tracking_id = tracking_id if tracking_id is not None else {}
        self._context_tags = context_tags if context_tags is not None else {}
        self._config = config if config is not None else {}
        self._path = path
        self._basename = basename
        self._reads_per_file = reads_per_file
        self._current_file = 0
        self._strand_counter = 0
        self._current_channel = 0
        self._index_file = os.path.join(path, basename + '_index.txt')
        self._index = open(self._index_file, 'w')
        self._index.write('channel\tread_number\tfile_number\tfilename\n')
        self.is_open = True

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return False

    def write_strand(self, strand):
        """ Writes a Strand object to the stream. """
        if strand['channel'] != self._current_channel \
           or self._strand_counter == self._reads_per_file:
            self._start_new_file(strand)
        fname = self._write_strand(strand)
        self._index.write('{}\t{}\t{}\t{}\n'.format(strand['channel'],
                                                    strand['read_attrs']['read_number'],
                                                    self._current_file, fname))
        return

    def close(self):
        """ Closes the stream. """
        if self.is_open:
            self._index.close()
        self.is_open = False

    def __del__(self):
        """ Finalizer. Closes the stream if necessary. """
        self.close()

    # ######## Private methods below ########## #

    def _start_new_file(self, strand):
        self._current_file = strand['read_attrs']['read_number']
        self._strand_counter = 0
        self._current_channel = strand['channel']
        channel_info = {'channel_number': strand['channel'],
                        'offset': strand['offset'],
                        'range': strand['range'],
                        'digitisation': strand['digitisation'],
                        'sampling_rate': strand['sampling_rate']}
        fname = '{}_ch{}_read{}_strand.fast5'.format(self._basename, strand['channel'],
                                                     self._current_file)
        full_path = os.path.join(self._path, fname)
        with Fast5File(full_path, 'w') as fh:
            fh.set_tracking_id(self._tracking_id)
            fh.add_context_tags(self._context_tags)
            fh.add_channel_info(channel_info)

    def _write_strand(self, strand):
        event_data = strand.get('event_data', None)
        raw_data = strand.get('raw_data', None)
        fname = '{}_ch{}_read{}_strand.fast5'.format(self._basename, strand['channel'],
                                                     self._current_file)
        full_path = os.path.join(self._path, fname)
        
        with Fast5File(full_path, 'r+') as fh:
            fh.add_read(strand['read_attrs']['read_number'], strand['read_attrs']['read_id'],
                        strand['read_attrs']['start_time'], strand['read_attrs']['duration'],
                        strand['read_attrs'].get('start_mux', 0),
                        strand['read_attrs'].get('median_before', -1.0))
            if raw_data is not None:
                fh.add_raw_data(strand['read_attrs']['read_number'], raw_data)
            if event_data is not None:
                ev_attrs = {'name': 'MinKNOW',
                            'version': self._tracking_id.get('version', 'unknown')}
                cfg_items = {}
                for key, subgroup in self._config.items():
                    cfg_items[key] = {name: value for name, value in subgroup.items()}
                group_name = fh.get_latest_analysis('EventDetection')
                if group_name is None:
                    group_name = 'EventDetection_000'
                    fh.add_analysis('event_detection', group_name, ev_attrs, cfg_items)
                read_attrs = {name: strand['read_attrs'][name] for name in REQUIRED_FIELDS}
                fh.add_analysis_subgroup(group_name, 'Reads/Read_{}'.format(strand['read_attrs']['read_number']),
                                         attrs=read_attrs)
                fh.add_analysis_dataset('{}/Reads/Read_{}'.format(group_name, strand['read_attrs']['read_number']),
                                        'Events', event_data)
        self._strand_counter += 1
        return fname

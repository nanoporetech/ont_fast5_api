""" Basic API for reading/writing single-read fast5 files.
"""
import os
import sys
import h5py
import numpy as np
from collections import deque
try:
    from ConfigParser import ConfigParser
except ImportError:  # python3
    from configparser import ConfigParser
from ont_fast5_api.fast5_info import _clean, Fast5Info, ReadInfo
from ont_fast5_api import CURRENT_FAST5_VERSION

# Current workflows add the component name to the hdf group as
# an attribute. For older files, the following dict allows us
# to work out the component name from the group name.
LEGACY_COMPONENT_NAMES = {'Alignment': 'alignment',
                          'Basecall_1D': 'basecall_1d',
                          'OnlineBasecall': 'basecall_1d',
                          'Basecall_2D': 'basecall_2d',
                          'Calibration_Strand': 'calibration_strand',
                          'EventDetection': 'event_detection',
                          'Segmentation': 'segmentation',
                          'Hairpin_Split': 'segmentation',
                          'Segment_Linear': 'segmentation',
                          'Validation': 'segmentation',
                          'AlignToRef': 'align_to_ref',
                          'Barcoding': 'barcoding',
                          'Classification': 'classification',
                          'Evaluation': 'evaluation',
                          'Multiple_Alignment': 'multiple_alignment',
                          'Squiggle_Map': 'squiggle_map',
                          'Sam_Segmentor': 'sam_segmentor',
                          'arma': 'arma',
                          'Basic_component': 'basic_component',
                          }
supported_modes = ('r', 'r+', 'w', 'w-', 'x', 'a')
mode_docstring = """Supported file modes:
    r        Readonly, file must exist (default)
    r+       Read/write, file must exist
    w        Create file, truncate if exists
    w- or x  Create file, fail if exists
    a        Read/write if exists, create otherwise""" # Taken from h5py


class AbstractFast5File(object):
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return False

    def get_read_ids(self):
        raise NotImplementedError()

    def get_read(self, read_id):
        raise NotImplementedError()


    def assert_open(self):
        if not self._is_open:
            raise IOError("Fast5 file is not open: {}".format(self.filename))

    def assert_writeable(self):
        self.assert_open()
        if self.mode == 'r':
            raise IOError("Fast5 file is in read-only mode '{}' {}".format(self.mode, self.filename))


    def close(self):
        """ Closes the object.
        """
        if self._is_open:
            self.mode = None
            if self.handle:
                self.handle.close()
                self.handle = None
            self.filename = None
            self._is_open = False
            self.status = None



class Fast5File(AbstractFast5File):
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
        self._is_open = False
        self.mode = mode
        self._initialise_file()

    def get_run_id(self):
        return self.handle[self.global_key + 'tracking_id'].attrs['run_id']

    def get_read_ids(self):
        return [self.get_read_id()]

    def get_read(self, read_id):
        if read_id != self.get_read_id():
            raise KeyError("read_id given: {} does not match read_id in file: {}"
                           "".format(read_id, self.get_read_id()))
        return self

    def get_read_id(self):
        return self.status.read_info[0].read_id

    def get_tracking_id(self):
        """ Returns a dictionary of tracking-id key/value pairs.
        """
        self.assert_open()
        tracking = self.handle[self.global_key +'tracking_id'].attrs.items()
        tracking = {key: _clean(value) for key, value in tracking}
        return tracking

    def set_tracking_id(self, data, clear=False):
        """ Add tracking-id data to the tracking_id group.
        
        :param data: A dictionary of key/value pairs. Keys must be strings.
            Values can be strings or numeric values.
        :param clear: If set, any existing tracking-id data will be removed. 
        """
        self.assert_writeable()
        self._add_attributes(self.global_key + 'tracking_id', data, clear)
        return

    def get_channel_info(self):
        """ Returns a dictionary of channel information key/value pairs.
        """
        self.assert_open()
        channel_info = self.handle[self.global_key + 'channel_id'].attrs.items()
        channel_info = {key: _clean(value) for key, value in channel_info}
        channel_info['channel_number'] = int(channel_info['channel_number'])
        return channel_info

    def add_channel_info(self, data, clear=False):
        """ Add channel info data to the channel_id group.
        
        :param data: A dictionary of key/value pairs. Keys must be strings.
            Values can be strings or numeric values.
        :param clear: If set, any existing channel info data will be removed. 
        """
        self.assert_writeable()
        self._add_attributes(self.global_key + 'channel_id', data, clear)

    @property
    def has_context_tags(self):
        return 'context_tags' in self.handle[self.global_key[:-1]]

    def get_context_tags(self):
        """ Returns a dictionary of context tag key/value pairs.
        """
        self.assert_open()
        if self.has_context_tags:
            tags = self.handle[self.global_key + 'context_tags'].attrs.items()
            return {key: _clean(value) for key, value in tags}
        return {}

    def add_context_tags(self, data, clear=False):
        """ Replaces any existing context tag data with the provided values.
        
        :param data: A dictionary of key/value pairs. Keys must be strings.
            Values can be strings or numeric values.
        :param clear: If set, any existing context tag data will be removed. 
        """
        self.assert_writeable()
        if self.has_context_tags:
            self._add_attributes(self.global_key + 'context_tags', data, clear)
        else:
            self._add_group(self.global_key + 'context_tags', data)

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
        if read_number is None:
            read_number = self._get_only_read_number()
        read_index = self.status.read_number_map[read_number]
        if not self.status.read_info[read_index].has_raw_data:
            msg = 'Fast5 file has no raw data for read {} in {}'.format(read_number, self.filename)
            raise KeyError(msg)
        if end is None:
            end = self.status.read_info[read_index].duration
        if start is None:
            start = 0
        dataset_name = 'Raw/Reads/Read_{}/Signal'.format(read_number)
        return self._load_raw(dataset_name, start, end, scale)

    def add_raw_data(self, read_number, data):
        """ Add raw data for a read.
        
        :param read_number: The number of the read the raw data is for.
        :param data: The raw data DAQ values (16 bit integers).
        
        The read must already exist in the file. It must not already
        have raw data.
        """
        self.assert_writeable()
        read_index = self.status.read_number_map[read_number]
        if self.status.read_info[read_index].has_raw_data:
            msg = 'Fast5 file already has raw data for read {} in {}'
            raise KeyError(msg.format(read_number, self.filename))
        self._save_raw(read_number, data)

    def list_analyses(self, component=None):
        """ Provides a list of all analyses groups.
        
        :param component: Optional component name. If provided, only
            analyses of that component will be returned.
        :returns: A list of component-name/group-name pairs (tuples).
        """
        self.assert_open()
        analyses = []
        if 'Analyses' not in self.handle:
            return analyses
        ana_groups = self.handle['Analyses'].keys()
        for group_name in ana_groups:
            group_attrs = self.handle['Analyses/{}'.format(group_name)].attrs
            if 'component' in group_attrs:
                comp = _clean(group_attrs['component'])
            elif group_name[:-4] in LEGACY_COMPONENT_NAMES:
                comp = LEGACY_COMPONENT_NAMES[group_name[:-4]]
            else:
                # We don't know anything about this component!
                comp = None
            if comp is not None and (component is None or comp == component):
                analyses.append((comp, group_name))
        return analyses

    def get_latest_analysis(self, group_base, increment=False):
        """ Return the latest analysis group with the specified
            base name.
        
        :param group_base: The name of the analysis group, excluding
            the _### at the end e.g. 'Alignment' rather than 'Alignment_000'
        :param increment: If set, this will find the name of the latest
            analysis group, increment the index by 1, and return
            the results. If there aren't any, it will give an index
            of 000.
        :returns: The group name with the highest index, or None if
            there are no analyses with the specified base name.
        """
        self.assert_open()
        all_analyses = self.list_analyses()
        selected = []
        for analysis in all_analyses:
            if analysis[1][:-4] == group_base:
                selected.append(analysis[1])
        if len(selected) == 0:
            result = None
            if increment:
                result = '{}_000'.format(group_base)
            return result
        result = sorted(selected)[-1]
        if increment:
            count = int(result[-3:]) + 1
            result = '{}_{}'.format(group_base, str(count).zfill(3))
        return result

    def add_chain(self, group_name, component_map):
        """
        Adds the component chain to ``group_name`` in the fast5.
        These are added as attributes to the group.

        :param group_name: The group name you wish to add chaining data to,
            e.g. ``Test_000``
        :param component_map: The set of components and corresponding
            group names or group paths that contribute data to the analysis.
            If group names are provided, these will be converted into group
            paths.

            If ``Test_000`` uses data from the results of
            ``first_component`` stored at ``Analyses/First_000/``
            the component_map could be ``{'first_component': 'First_000'}`` or
            ``{'first_component': 'Analyses/First_000'}``.

        """
        self.assert_writeable()

        for component, path in component_map.items():
            if not path.startswith('Analyses/'):
                path = 'Analyses/{}'.format(path)
            component_map[component] = path

        self.add_analysis_attributes(group_name, component_map)

    def get_chain(self, group_name):
        """ Provides the component and group names for an analysis chain.
        
        :param group_name: The group name of the last step in the analysis
            chain e.g. 'Basecall_1D_000'
        :returns: A list of component-name/group-name pairs (tuples). This
            will include each component of the chain, in order.
        """
        self.assert_open()
        endgroup = 'Analyses/{}'.format(group_name)
        attr = self.handle[endgroup].attrs
        if 'component' in attr:
            component = attr['component']
        else:
            component = LEGACY_COMPONENT_NAMES[group_name[:-4]]
        chain = deque()
        chain.append((component, group_name))
        groups_to_check = deque()
        groups_to_check.append(endgroup)
        while len(groups_to_check) > 0:
            group = groups_to_check.popleft()
            attr = self.handle[group].attrs
            for key, value in attr.items():
                if str(value).startswith('Analyses/'):
                    chain_entry = (key, value[9:])
                    # We need to maintain the order of the components, so
                    # we'll move any we see again to the end of the chain.
                    if chain_entry in chain:
                        chain.remove(chain_entry)
                    chain.append(chain_entry)
                    groups_to_check.append(value)
        return list(chain)

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
        group_name = 'Raw/Reads/Read_{}'.format(read_number)
        attrs = {'read_number': read_number,
                 'read_id': read_id,
                 'start_time': start_time,
                 'duration': duration,
                 'start_mux': mux,
                 'median_before': median_before}
        self._add_group(group_name, attrs)

    def add_analysis(self, component, group_name, attrs, config=None):
        """ Add a new analysis group to the file.
        
        :param component: The component name.
        :param group_name: The name to use for the group. Must not already
            exist in the file e.g. 'Test_000'.
        :param attrs: A dictionary containing the key-value pairs to
            put in the analysis group as attributes. Keys must be strings,
            and values must be strings or numeric types.
        :param config: A dictionary of dictionaries. The top level keys
            should be the name of analysis steps, and should contain
            key value pairs for analysis parameters used.
        """
        self.assert_writeable()
        group = 'Analyses/{}'.format(group_name)
        cfg_group = '{}/Configuration'.format(group)
        group_attrs = attrs.copy()
        group_attrs['component'] = component
        self._add_group(group, group_attrs)
        self.handle[group].create_group('Summary')
        self.handle[group].create_group('Configuration')
        if config is not None:
            self._add_attribute_tree(cfg_group, config)

    def add_log(self, group_name, field_name, log_string):
        """ Add a log the file.
        :param group_name: Global group name. Cave: No 'Analyses' is prepended.
        :param field_name: A field {group_name}/{field_name} will be written.
        :param log_string: content to be written.
        """
        self.assert_writeable()
        if group_name not in self.handle:
            self._add_group(group_name, {})
        sanitized_data = _sanitize_data_for_writing(log_string)
        self.handle[group_name].create_dataset(field_name, data=sanitized_data)

    def set_summary_data(self, group_name, section_name, data):
        """ Set the summary data for an analysis group.

        :param group_name: The name of the analysis group.
        :param section_name: The analysis step. This will become a
            subfolder in the Summary section.
        :param data: A dictionary containing keys which are the summary
            fields, and values which are the summary values.
        """
        self.assert_writeable()
        group = 'Analyses/{}/Summary/{}'.format(group_name, section_name)
        self._add_group(group, data)

    def set_analysis_config(self, group_name, config):
        """ Set Configuration data for analysis group.  The ``config`` can
            be passed as a dict of dicts e.g. ``{'section': {'key': 'value'}}``
            or can be passed directly as a ConfigParser object.

        :param group_name: The name of the analysis group e.g. Example_000
        :param config: Representation of configuration as ConfigParser or
            dict of dicts.
        :raises KeyError: if group_name does not exist in fast5 or
            fast5 is not open for writing
        :raises TypeError: if ``config`` is not ConfigParser or dict obj.
        """
        self.assert_writeable()
        if 'Analyses/{}'.format(group_name) not in self.handle:
            msg = 'Dataset cannot be added to non-existent group: Analyses/{} in {}'
            raise KeyError(msg.format(group_name, self.filename))
        if isinstance(config, ConfigParser):
            config_dict = {}
            for section in config.sections():
                config_dict[section] = {k: v for k, v in config.items(section)}
        elif isinstance(config, dict):
            config_dict = config
        else:
            raise TypeError('config must be a ConfigParser or dict not {}'.format(type(config)))

        config_path = 'Analyses/{}/Configuration'.format(group_name)
        self._add_attribute_tree(config_path, config_dict)

    def get_analysis_config(self, group_name):
        """ Gets any config data saved for the analysis.

        :param group_name: The name of the analysis group.
        :returns: A dictionary of dictionaries. Each key represents
            an analysis step. Each value is a dictionary containing the
            analysis parameters as key/value pairs. Returns None if no
            configuration exists for the analysis.
        """
        self.assert_open()
        group = 'Analyses/{}/Configuration'.format(group_name)
        config = None
        if group in self.handle:
            config = self._parse_attribute_tree(group)
        return config

    def get_summary_data(self, group_name):
        """ Get the summary data for an analysis group.
        
        :param group_name: The name of the analysis group to pull summary
            data for.
        :returns: A dictionary whose keys are analysis steps, and whose
            values are dictionaries of key/value pairs for the results of
            that step.
        """
        self.assert_open()
        group = 'Analyses/{}/Summary'.format(group_name)
        summary = None
        if group in self.handle:
            summary = self._parse_attribute_tree(group)
        return summary

    def add_analysis_subgroup(self, group_name, subgroup_name, attrs=None):
        """ Add a new subgroup to an existing analysis group.
        
        :param group_name: The name of the analysis group you are adding to.
        :param subgroup_name: The name of the new subgroup.
        :param attrs: A dictionary representing the attributes to assign to
            the subgroup.
        
        The new subgroup must not already exist.
        
        The subgroup name can be a nested name, such as "Template/Data". This
        will create the "Template" subgroup (if it does not exist), and the
        "Data" subgroup below it.
        """
        self.assert_writeable()
        group = 'Analyses/{}/{}'.format(group_name, subgroup_name)
        self._add_group(group, attrs)

    def add_analysis_attributes(self, group_name, attrs, clear=False):
        """ Add attributes on the group or dataset specified.
        
        :param group_name: The name of the group (or dataset).
        :param attrs: A dictionary representing the attributes to add.
        :param clear: If set, any existing attributes will be cleared.
        
        The specified group name can be any existing path (relative to the
        "Analyses" group. It can be a group or a dataset.
        """
        self.assert_writeable()
        group = 'Analyses/{}'.format(group_name)
        self._add_attributes(group, attrs, clear)

    def get_analysis_attributes(self, group_name):
        """ Returns the attributes for the specified group or dataset.
        
        :param group_name: The path of the group or dataset, relative to the
            "Analyses" group.
        :returns: A dictionary representing the attributes (if any).
        """
        self.assert_open()
        group = 'Analyses/{}'.format(group_name)
        attr = None
        if group in self.handle:
            attr = self.handle[group].attrs.items()
            attr = {key: _clean(value) for key, value in attr}
        return attr

    def add_analysis_dataset(self, group_name, dataset_name, data, attrs=None):
        """ Add a dataset to the specified group.
        
        :param group_name: The path of the group the dataset will be added to,
            relative to the "Analyses" group.
        :param dataset_name: The name of the new dataset.
        :param data: A numpy array representing the data to be written.
        :param attrs: A dictionary of attributes to be added to the dataset.
        :raises KeyError: If dataset is being added to non-existant group or
            if file is not open for writing.
        """
        self.assert_writeable()
        group_path = 'Analyses/{}'.format(group_name)
        if group_path not in self.handle:
            msg = 'Dataset cannot be added to non-existent group: Analyses/{} in {}'
            raise KeyError(msg.format(group_name, self.filename))

        sanitized_data = _sanitize_data_for_writing(data)
        if np.shape(sanitized_data) == ():  # We can't compress scalar datasets
            self.handle[group_path].create_dataset(dataset_name,
                                                   data=sanitized_data)
        else:
            self.handle[group_path].create_dataset(dataset_name,
                                                   data=sanitized_data,
                                                   compression='gzip')
        if attrs is not None:
            path = '{}/{}'.format(group_path, dataset_name)
            self._add_attributes(path, attrs)

    def get_analysis_dataset(self, group_name, dataset_name, proxy=False):
        """ Return the specified dataset, as a numpy array.
        
        :param group_name: The path of the group containing the dataset,
            relative to the "Analyses" group.
        :param dataset_name: The name of the dataset.
        :param proxy: If True, this will directly return the h5py dataset.
            If False, it will load the data into a Numpy array.
        :return: A numpy array containing the data. Returns None if the dataset
            does not exist.
        """
        self.assert_open()
        dataset_name = 'Analyses/{}/{}'.format(group_name, dataset_name)
        data = None
        if dataset_name in self.handle:
            data = self.handle[dataset_name]
            if not proxy:
                data = _sanitize_data_for_reading(data[()])
        return data

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

    def _load_raw(self, dataset_name, start, end, scale):
        raw = self.handle[dataset_name]
        if scale:
            channel_info = self.handle[self.global_key + 'channel_id'].attrs
            digi = channel_info['digitisation']
            parange = channel_info['range']
            offset = channel_info['offset']
            scaling = parange / digi
            data = np.empty(end - start, dtype=np.float32)
            data[:] = scaling * (raw[start:end] + offset)
        else:
            data = raw[start:end]
        return data

    def _save_raw(self, read_number, data):
        group_name = 'Raw/Reads/Read_{}'.format(read_number)
        self.handle[group_name].create_dataset('Signal', data=data, compression='gzip', shuffle=True)
        read_index = self.status.read_number_map[read_number]
        self.status.read_info[read_index].has_raw_data = True

    def _get_only_read_number(self):
        read_number = self.status.read_info[0].read_number
        return read_number

    def _add_group(self, group, attrs):
        """
        :param group: group_name
        :param attrs:
        :return:
        """
        self.handle.create_group(group)
        if attrs is not None:
            self._add_attributes(group, attrs)

    def _add_attributes(self, path, attrs, clear=False):
        path_grp = self.handle[path]
        path_attr = path_grp.attrs
        if clear:
            for key in path_attr.keys():
                del path_attr[key]
        for key, value in attrs.items():
            path_attr[key] = value

    def _get_attributes(self, path):
        """
        :param path: filepath within fast5
        :return: dictionary of attributes found at ``path``
        :rtype dict
        """
        path_grp = self.handle[path]
        path_attr = path_grp.attrs
        return dict(path_attr)

    def _add_attribute_tree(self, group, config):
        for folder, data in config.items():
            path = '{}/{}'.format(group, folder)
            self._add_group(path, data)

    def _parse_attribute_tree(self, group):
        data = {}
        folders = self.handle[group].keys()
        for folder in folders:
            path = '{}/{}'.format(group, folder)
            attr = self.handle[path].attrs.items()
            data[folder] = {key: _clean(value) for key, value in attr}
        return data

    def _initialise_file(self):
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
            self._is_open = True

def _sanitize_data_for_writing(data):
    # We only really need to do interesting conversions for python3
    if sys.version_info.major == 3:
        if isinstance(data, str):
            return data.encode()
        elif isinstance(data, np.ndarray) and data.dtype.kind == np.dtype(np.unicode):
            return data.astype('S')
        elif isinstance(data, np.ndarray) and len(data.dtype) > 1:
            dtypes = data.dtype.descr
            for index, entry in enumerate(dtypes):
                if entry[1].startswith('<U'):
                    # numpy.astype can't handle empty string datafields for some
                    # reason, so we'll explicitly state that.
                    if len(entry[1]) <= 2 or (len(entry[1]) == 3 and
                                              entry[1][2] == '0'):
                        raise TypeError('Empty datafield {} cannot be converted'
                                        ' by np.astype.'.format(entry[0]))
                    dtypes[index] = (entry[0], '|S{}'.format(entry[1][2:]))
            return data.astype(dtypes)
    return data


def _sanitize_data_for_reading(data):
    # This is all python 3 conversions, where we need to check for byte strings
    if sys.version_info.major == 3:
        if isinstance(data, bytes):
            return data.decode()
        elif isinstance(data, np.ndarray) and data.dtype.kind == 'S':
            return np.char.decode(data)
        elif isinstance(data, np.ndarray) and len(data.dtype) > 1:
            dtypes = data.dtype.descr
            for index, entry in enumerate(dtypes):
                if entry[1].startswith('|S'):
                    # numpy.astype can't handle empty datafields for some
                    # reason, so we'll explicitly state that.
                    if len(entry[1]) <= 2 or (len(entry[1]) == 3 and
                                              entry[1][2] == '0'):
                        raise TypeError('Empty datafield {} cannot be converted'
                                        ' by np.astype.'.format(entry[0]))
                    dtypes[index] = (entry[0], '<U{}'.format(entry[1][2:]))
            return data.astype(dtypes)
    return data

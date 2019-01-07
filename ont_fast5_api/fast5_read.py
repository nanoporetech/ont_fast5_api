import warnings
from ont_fast5_api.fast5_file import Fast5File


class Fast5Read(Fast5File):

    def __init__(self, parent, read_id):
        self.parent = parent
        self.mode = parent.mode
        self.filename = parent.filename
        self.handle = parent.handle["read_" + read_id]
        self.read_id = read_id
        self.global_key = ""

    def get_read_id(self):
        return self.read_id

    @property
    def _is_open(self):
        return self.parent._is_open

    @property
    def has_context_tags(self):
        return 'context_tags' in self.handle

    def add_raw_data(self, data, attrs):
        """ Add raw data for a read.

        :param data: The raw data DAQ values (16 bit integers).

        The read must already exist in the file. It must not already
        have raw data.
        """
        self.assert_writeable()
        if "Raw" not in self.handle:
            self.handle.create_group("Raw")
        if "Signal" in self.handle['Raw']:
            msg = "Fast5 file already has raw data for read '{}' in: {}"
            raise KeyError(msg.format(self.read_id, self.filename))
        self.handle['Raw'].create_dataset('Signal', data=data, compression='gzip', shuffle=True, dtype='i2')
        self._add_attributes("Raw", attrs, clear=True)

    def add_channel_info(self, attrs, clear=False):
        """ Add channel info data to the channel_id group.

        :param data: A dictionary of key/value pairs. Keys must be strings.
            Values can be strings or numeric values.
        :param clear: If set, any existing channel info data will be removed.
        """
        self.assert_writeable()
        if 'channel_id' not in self.handle:
            self.handle.create_group('channel_id')
        self._add_attributes('channel_id', attrs, clear)

    def add_tracking_id(self, attrs, clear=False):
        self.assert_writeable()
        if 'tracking_id' not in self.handle:
            self.handle.create_group("tracking_id")
        self.set_tracking_id(attrs, clear)

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
        if "Analyses" not in self.handle:
            self.handle.create_group("Analyses")
        super(Fast5Read, self).add_analysis(component, group_name, attrs, config)

    def get_raw_data(self, read_number=None, start=None, end=None, scale=False):
        if read_number:
            warnings.warn("Read number is not used for MultiReadFast5")
        return self._load_raw("Raw/Signal", start, end, scale)

    def add_read(self, read_number, read_id, start_time, duration, mux, median_before):
        raise NotImplementedError("Cannot add_read() to a Fast5Read(). "
                                  "Use MultiFast5File.create_read() instead")

    @staticmethod
    def read_summary_data(fname, component):
        raise NotImplementedError("read_summary_data() is not implemented for MultiFast5 reads")

    @staticmethod
    def update_legacy_file(fname):
        raise NotImplementedError("Cannot update legacy file for a MultiFast5Read")

""" Helper class for getting information about a fast5 file.
"""
import os
import sys
import h5py
import numpy as np


class ReadInfo(object):
    """ This object provides basic details about a read.
    """
    
    def __init__(self, read_number, read_id, start_time, duration,
                 mux=0, median_before=-1.0):
        """ Constructs an object describing a read.
        
        :param read_number: A read number, unique for the channel.
        :param read_id: A globally unique read id.
        :param start_time: The start time of the read (in samples).
        :param duration: The duration of the read (in samples).
        :param mux: The mux of the channel when the read occurred.
        :param median_before: The median current before the read.
        """
        self.read_number = read_number
        self.read_id = read_id
        self.has_raw_data = False
        self.duration = duration
        self.has_event_data = False
        self.event_data_count = 0
        self.start_time = start_time
        self.start_mux = mux
        self.median_before = median_before


class Fast5Info(object):
    """ This object provides some basic details about a read fast5 file.
    
    **Fields**
      * **valid:** Indicates whether the fast5 file is valid or not.
      * **version:** Indicates the version of the read fast5 file
        specification the file conforms to (if any).
      * **read_info:** A list of ReadInfo objects. One entry for each read.
      * **read_number_map:** A dictionary giving the index into the read_info
        list for each read number.
      * **read_id_map:** A dictionary giving the index into the read_info
        list for each read-id.
    """

    def __init__(self, fname):
        """ Constructs a status object from a file.

        :param fname: Filename of fast5 file to read status from.
        """
        self.valid = True
        self.channel = None
        self.read_info = []
        self.read_number_map = {}
        self.read_id_map = {}
        try:
            with h5py.File(fname, 'r') as handle:
                if 'file_version' in handle.attrs:
                    self.version = _clean(handle.attrs['file_version'])
                    if self.version < 0.6:
                        self.valid = False
                else:
                    self.valid = False
                    self.version = 0.0

                # Check for required groups.
                top_groups = handle.keys()
                if 'UniqueGlobalKey' in top_groups:
                    global_keys = handle['UniqueGlobalKey'].keys()
                if 'tracking_id' not in global_keys and self.version >= 1.1:
                    self.valid = False
                if 'channel_id' not in global_keys:
                    self.valid = False

                self.channel = handle['UniqueGlobalKey/channel_id'].attrs.get('channel_number')
                if self.channel is None and self.version < 1.1:
                    self.valid = False

                # Get the read information.
                if 'Raw' in top_groups:
                    reads = handle['Raw/Reads'].keys()
                    for read in reads:
                        read_group_name = 'Raw/Reads/{}'.format(read)
                        read_group = handle[read_group_name]
                        read_attrs = read_group.attrs
                        read_number = _clean(read_attrs['read_number'])
                        if 'read_id' in read_attrs:
                            read_id = _clean(read_attrs['read_id'])
                        else:
                            if self.version >= 1.1:
                                self.valid = False
                            else:
                                read_id = os.path.basename(fname)
                        start_time = _clean(read_attrs['start_time'])
                        duration = _clean(read_attrs['duration'])
                        mux = _clean(read_attrs.get('start_mux',0))
                        median_before = _clean(read_attrs.get('median_before',-1.0))
                        read_info = ReadInfo(read_number, read_id, start_time, duration, mux, median_before)
                        if 'Signal' in read_group:
                            read_info.has_raw_data = True
                        elif self.version < 1.1:
                            if 'Data' in read_group:
                                read_info.has_raw_data = True
                            else:
                                self.valid = False
                        self.read_info.append(read_info)
                        n = len(self.read_info) - 1
                        self.read_number_map[read_number] = n
                        self.read_id_map[read_id] = n
                else:
                    if self.version >= 1.1:
                        self.valid = False
                analyses = sorted(handle['Analyses'].keys()) if 'Analyses' in handle else []
                for ana in analyses[::-1]:
                    if ana.startswith('EventDetection'):
                        reads_group_name = 'Analyses/{}/Reads'.format(ana)
                        if reads_group_name not in handle:
                            continue
                        reads = handle[reads_group_name].keys()
                        for read in reads:
                            read_group_name = '{}/{}'.format(reads_group_name, read)
                            read_group = handle[read_group_name]
                            read_attrs = read_group.attrs
                            read_number = _clean(read_attrs['read_number'])
                            if 'read_id' in read_attrs:
                                read_id = _clean(read_attrs['read_id'])
                            else:
                                if self.version >= 1.1:
                                    self.valid = False
                                    continue
                                else:
                                    read_id = os.path.basename(fname)
                            start_time = _clean(read_attrs['start_time'])
                            duration = _clean(read_attrs['duration'])
                            mux = _clean(read_attrs.get('start_mux', 0))
                            median_before = _clean(read_attrs.get('median_before', -1.0))
                            read_info = ReadInfo(read_number, read_id, start_time, duration, mux, median_before)
                            if 'Events' in read_group:
                                read_info.has_event_data = True
                                read_info.event_data_count = len(read_group['Events'])
                            else:
                                read_info.has_event_data = False
                                read_info.event_data_count = 0
                            if read_number in self.read_number_map:
                                read_index = self.read_number_map[read_number]
                                self.read_info[read_index].has_event_data = read_info.has_event_data
                                self.read_info[read_index].event_data_count = read_info.event_data_count
                            else:
                                if self.version >= 1.1:
                                    self.valid = False
                                self.read_info.append(read_info)
                                n = len(self.read_info) - 1
                                self.read_number_map[read_number] = n
                                self.read_id_map[read_id] = n
                        break
        except:
            self.valid = False
            raise

        if self.version < 1.1:
            # There must be either raw data or event data (or both).
            if len(self.read_info) == 0:
                self.valid = False


def _clean(value):
    """ Convert numpy numeric types to their python equivalents. """
    if isinstance(value, np.ndarray):
        if value.dtype.kind == 'S':
            return np.char.decode(value).tolist()
        else:
            return value.tolist()
    elif type(value).__module__ == np.__name__:
        # h5py==2.8.0 on windows sometimes fails to cast this from an np.float64 to a python.float
        # We have explicitly cast in Albacore (merge 488) to avoid this bug, since casting here could be dangerous
        # https://github.com/h5py/h5py/issues/1051
        conversion = np.asscalar(value)
        if sys.version_info.major == 3 and isinstance(conversion, bytes):
            conversion = conversion.decode()
        return conversion
    elif sys.version_info.major == 3 and isinstance(value, bytes):
        return value.decode()
    else:
        return value

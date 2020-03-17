""" Helper class for getting information about a fast5 file.
"""
import os
import h5py

from packaging import version as packaging_version

# This unused import is included for backwards compatibilty and can be removed in future.
from ont_fast5_api.data_sanitisation import _clean

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
                    minimum_valid_version = packaging_version.Version('0.6')
                    if packaging_version.parse(str(self.version)) \
                       < minimum_valid_version:
                        self.valid = False
                else:
                    self.valid = False
                    self.version = 0.0

                # Check for required groups.
                top_groups = handle.keys()
                if 'UniqueGlobalKey' in top_groups:
                    global_keys = handle['UniqueGlobalKey'].keys()
                if 'tracking_id' not in global_keys and not self._legacy_version():
                    self.valid = False
                if 'channel_id' not in global_keys:
                    self.valid = False

                self.channel = handle['UniqueGlobalKey/channel_id'].attrs.get('channel_number')
                if self.channel is None and self._legacy_version():
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
                            if not self._legacy_version():
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
                        elif self._legacy_version():
                            if 'Data' in read_group:
                                read_info.has_raw_data = True
                            else:
                                self.valid = False
                        self.read_info.append(read_info)
                        n = len(self.read_info) - 1
                        self.read_number_map[read_number] = n
                        self.read_id_map[read_id] = n
                else:
                    if not self._legacy_version():
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
                                if not self._legacy_version():
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
                                if not self._legacy_version():
                                    self.valid = False
                                self.read_info.append(read_info)
                                n = len(self.read_info) - 1
                                self.read_number_map[read_number] = n
                                self.read_id_map[read_id] = n
                        break
        except:
            self.valid = False
            raise

        if self._legacy_version():
            # There must be either raw data or event data (or both).
            if len(self.read_info) == 0:
                self.valid = False

    def _legacy_version(self):
        legacy_cutoff = packaging_version.Version("1.1")
        return packaging_version.parse(str(self.version)) < legacy_cutoff

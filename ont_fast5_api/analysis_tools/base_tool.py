import abc
from abc import abstractmethod

from ont_fast5_api.fast5_file import Fast5File, Fast5FileTypeError
from ont_fast5_api.fast5_read import Fast5Read


class BaseTool(object):
    @property
    def group_id(self):
        raise NotImplementedError("BaseTool does not have a group_id")

    @property
    def analysis_id(self):
        raise NotImplementedError("BaseTool does not have a analysis_id")

    def __init__(self, source, mode='r', group_name=None, meta=None, config=None):
        """ Create a new analysis_tools object.

        :param source: Either an open Fast5File object, or a filename
            of a fast5 file.
        :param mode: The open mode (r or r+). Only if a filename is used
            for the source argument.
        :param group_name: The specific analysis instance you are interested in.
        :param meta: Metadata for a new analysis.
        :param config: Configuration data for a new analysis.

        To create a new analysis group, provide a group name that
        does not already exist, and an optional dictionary with the metadata.
        The following fields are recommended, as a minimum:

            * name - The name of the software used.
            * time_stamp - The time at which the analysis was performed.

        If the group name already exists, the "meta" parameter is ignored. If
        the specified group has a "component" attribute, and its value does not
        match self.analysis_id, an exception will be thrown.
        """
        if isinstance(source, Fast5Read):
            self.filename = source.filename  # Useful for debugging purposes
            self.handle = source
            self.close_handle_when_done = False
        elif isinstance(source, str):
            self.filename = source  # Useful for debugging purposes
            try:
                self.handle = Fast5File(source, mode)
            except Fast5FileTypeError :
                raise NotImplementedError("AnalysisTools do not support accessing MultiReadFast5 files by filepath")
            self.close_handle_when_done = True
        else:
            raise KeyError('Unrecognized type for argument "source": {}'.format(source))
        if group_name is None:
            group_name = self.handle.get_latest_analysis(self.group_id)
            if group_name is None:
                raise KeyError('No group: {} found in file: {}'.format(group_name, self.filename))
        self.group_name = group_name
        attrs = self.handle.get_analysis_attributes(group_name)

        if attrs is None:
            self.handle.add_analysis(self.analysis_id, group_name, meta, config)
            attrs = self.handle.get_analysis_attributes(group_name)
        if 'component' in attrs and attrs['component'] != self.analysis_id:
            raise ValueError('Component {} is not {}'.format(attrs.get('component'), self.analysis_id))

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return False

    def close(self):
        """ Closes the object.
        """
        if self.handle and self.close_handle_when_done:
            self.handle.close()

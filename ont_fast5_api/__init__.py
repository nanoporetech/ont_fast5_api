__version__ = '3.0.1'
__version_info__ = tuple([int(num) for num in __version__.split('.')])
CURRENT_FAST5_VERSION = 2.0

# Set up a default NullHandler in case we don't end up using another one
# Taken from http://docs.python-guide.org/en/latest/writing/logging/
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

from ont_fast5_api.compression_settings import register_plugin
register_plugin()

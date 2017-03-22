import logging

__version__ = '0.3.2'
__version_info__ = tuple([int(num) for num in __version__.split('.')])

# Set up a default NullHandler in case we don't end up using another one
# Taken from http://docs.python-guide.org/en/latest/writing/logging/
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

CURRENT_FAST5_VERSION = 1.1

__version__ = '3.1.6'
__version_info__ = tuple([int(num) for num in __version__.split('.')])
CURRENT_FAST5_VERSION = 2.0

import sys
if sys.version_info < (3,):
    raise ImportError(
    """ont-fast5-api requires Python 3, ideally >=3.5.

    Somehow you have ended up running this on Python 2, which reached its end of
    life in 2019. Apologies! To avoid this issue, either:

    - Upgrade to Python 3, or

    - Download an older ont-fast5-api version:

    $ pip install 'ont-fast5-api<3.0'

    Note that you will be missing features and bug fixes by running older versions
    of ont-fast5-api.

    """)

# Set up a default NullHandler in case we don't end up using another one
# Taken from http://docs.python-guide.org/en/latest/writing/logging/
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

from ont_fast5_api.compression_settings import register_plugin
register_plugin()

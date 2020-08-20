import unittest
from unittest.mock import patch
import os

from ont_fast5_api.compression_settings import register_plugin

class TestCheckCompression(unittest.TestCase):

    def test_register_plugin_no_prepend(self):
        # GIVEN a version of h5py that has the h5pl module
        with patch('h5py.h5pl', create=True) as mock:

            # WHEN h5py.h5pl doesn't have the prepend method
            del mock.prepend
            # and the hdf5 plugin path variable hasn't been set
            if 'HDF5_PLUGIN_PATH' in os.environ:
                del os.environ['HDF5_PLUGIN_PATH']
            self.assertTrue('HDF5_PLUGIN_PATH' not in os.environ)

            # THEN when we try and register the vbz plugin we fall back to setting
            # HDF5_PLUGIN_PATH (because we can't use prepend)
            plugin_path = register_plugin()
            self.assertTrue(os.environ['HDF5_PLUGIN_PATH'] == plugin_path)
        
            

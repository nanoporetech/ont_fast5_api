import h5py
import os
import unittest
import numpy as np
from numpy import array, ndarray, dtype
from pkg_resources import parse_version

from ont_fast5_api.data_sanitisation import _sanitize_data_for_reading, _sanitize_data_for_writing, _clean, \
    check_version_compatibility
from ont_fast5_api.multi_fast5 import MultiFast5File

from test.helpers import TestFast5ApiHelper, test_data


class TestDataSanitisation(TestFast5ApiHelper):

    def test__clean(self):
        self.assertEqual(_clean(1), 1)
        self.assertEqual(_clean(b''), '')
        self.assertEqual(_clean(''), '')
        self.assertEqual(_clean('str'), 'str')
        self.assertEqual(_clean(b'str'), 'str')
        self.assertTrue(isinstance(_clean('str'), str))

        # _clean should convert byte strings into utf-8 ones
        test_str = array(b'Hello!', dtype=bytes)
        self.assertEqual(type(test_str), ndarray)
        self.assertEqual(_clean(test_str), 'Hello!')

        # _clean shouldn't do anything to python strings
        test_str = array('Hello!', dtype=str)
        self.assertEqual(type(test_str), ndarray)
        self.assertEqual(_clean(test_str), test_str)

        self.assertEqual(_clean(array([1, 2, 3])), [1, 2, 3])

    def test__sanitize_data(self):
        # We expect conversion from utf8 to bytestrings and vice-versa
        test_string = 'Avast'
        self.assertEqual(test_string,
                         _sanitize_data_for_reading(test_string.encode()))
        self.assertEqual(test_string.encode(),
                         _sanitize_data_for_writing(test_string))

        test_array = array('Arr', dtype=str)
        self.assertEqual(test_array,
                         _sanitize_data_for_reading(np.char.encode(test_array)))
        self.assertEqual(np.char.encode(test_array),
                         _sanitize_data_for_writing(test_array))

        test_ndarray_utf8 = array([('Narr', 0)],
                                  dtype=[('string', (str, 4)),
                                         ('int', int)])
        test_ndarray_bytes = array([(b'Narr', 0)],
                                   dtype=[('string', (bytes, 4)),
                                          ('int', int)])
        self.assertEqual(test_ndarray_utf8,
                         _sanitize_data_for_reading(test_ndarray_bytes))
        self.assertEqual(test_ndarray_bytes,
                         _sanitize_data_for_writing(test_ndarray_utf8))

    @unittest.skipUnless(parse_version(h5py.__version__) < parse_version("2.7"), "h5py==2.6 specific test")
    def test__sanitize_data_emptystrings_h5py_26(self):
        test_ndarray_utf8 = array([('', '')], dtype=[('empty', str),
                                                     ('string', str)])
        test_ndarray_bytes = array([('', '')], dtype=[('empty', bytes),
                                                      ('string', bytes)])

        try:
            str_array = _sanitize_data_for_reading(test_ndarray_bytes)
            byte_array = _sanitize_data_for_writing(test_ndarray_utf8)
            self.assertTrue(np.array_equal(test_ndarray_utf8, str_array))
            self.assertTrue(np.array_equal(test_ndarray_bytes, byte_array))
        except UnicodeError:
            # In h5py==2.6 this is non-deterministic and sometimes fails - at least it raises a nice error message
            pass

    @unittest.skipIf(parse_version(h5py.__version__) < parse_version("2.7"), "h5py==2.6 has a different test")
    def test__sanitize_data_emptystrings(self):
        test_ndarray_utf8 = array([('', '')], dtype=[('empty', str),
                                                     ('string', str)])
        test_ndarray_bytes = array([('', '')], dtype=[('empty', bytes),
                                                      ('string', bytes)])

        str_array = _sanitize_data_for_reading(test_ndarray_bytes)
        byte_array = _sanitize_data_for_writing(test_ndarray_utf8)
        self.assertTrue(np.array_equal(test_ndarray_utf8, str_array))
        self.assertTrue(np.array_equal(test_ndarray_bytes, byte_array))

    @unittest.skipUnless(parse_version(h5py.__version__) < parse_version("2.7"), "h5py==2.6 specific test")
    def test_sanitise_array_empty_string_h5py_26(self):
        input_list = [('', 1, 4.8), ('', 2, 7.6)]
        input_types = [('base', str), ('length', 'i4'), ('score', 'f8')]
        input_array = array(input_list, dtype=input_types)
        input_rec = input_array.view(np.recarray)
        expected_types = [('base', 'S'), ('length', 'i4'), ('score', 'f8')]

        try:
            output_array = _sanitize_data_for_writing(input_array)
            self.assertEqual(expected_types, output_array.dtype)

            roundtrip_array = _sanitize_data_for_reading(output_array)
            self.assertTrue(np.array_equal(input_array, roundtrip_array))

            output_recarray = _sanitize_data_for_writing(input_rec)
            self.assertEqual(expected_types, output_recarray.dtype)
        except UnicodeError:
            # This sometimes fails on older h5py versions - at least it raises a nice error message
            return None

    @unittest.skipIf(parse_version(h5py.__version__) < parse_version("2.7"), "h5py==2.6 has a different test")
    def test_sanitise_array_empty_string(self):
        input_list = [('', 1, 4.8), ('', 2, 7.6)]
        input_types = [('base', str), ('length', 'i4'), ('score', 'f8')]
        input_array = array(input_list, dtype=input_types)
        output_array = _sanitize_data_for_writing(input_array)

        expected_types = [('base', 'S'), ('length', 'i4'), ('score', 'f8')]
        self.assertEqual(expected_types, output_array.dtype)

        roundtrip_array = _sanitize_data_for_reading(output_array)
        self.assertTrue(np.array_equal(input_array, roundtrip_array))

        # Check things works with numpy.recarrays too
        input_rec = input_array.view(np.recarray)
        output_recarray = _sanitize_data_for_writing(input_rec)
        self.assertEqual(expected_types, output_recarray.dtype)

    def test_real_example_file(self):
        with MultiFast5File(os.path.join(test_data, 'rle_basecall_table', 'rle_example.fast5'), 'r') as mf5:
            for read in mf5.get_reads():
                actual_data = read.handle['Analyses/Basecall_1D_000/BaseCalled_template/RunlengthBasecall']
                expected_dtypes = [('base', '<U1'),  # After cleaning this is a unicode string
                                   ('scale', '<f4'),
                                   ('shape', '<f4'),
                                   ('weight', '<f4'),
                                   ('index', '<u4'),
                                   ('runlength', '<u4')]

                for field, expected_type in expected_dtypes:
                    if field != 'base':
                        self.assertEqual(dtype(expected_type), actual_data[field].dtype)
                    else:
                        # Before cleaning the 'base' column is of type byte-string length=1
                        self.assertEqual(dtype('|S1'), actual_data[field].dtype)

                try:
                    clean_data = _sanitize_data_for_reading(actual_data)
                    self.assertEqual(dtype(expected_dtypes), clean_data.dtype)
                except UnicodeError:
                    if parse_version(h5py.__version__) < parse_version("2.7"):
                        # h5py==2.6 often fails to decode these arrays correctly
                        pass
                    else:
                        raise

    @unittest.skipUnless(parse_version(h5py.__version__) < parse_version("2.7")
                         and parse_version(np.__version__) >= parse_version("1.13"),
                         "Only need to test on incompatible versions")
    def test_version_compatibility(self):
        with self.assertRaises(EnvironmentError):
            check_version_compatibility()
        with self.assertRaises(EnvironmentError):
            self.test_real_example_file()

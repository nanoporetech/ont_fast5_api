import h5py
import numpy as np
from pkg_resources import parse_version


def check_version_compatibility():
    if parse_version(h5py.__version__) < parse_version("2.7") \
            and parse_version(np.__version__) >= parse_version("1.13"):
        raise EnvironmentError("Incompatible h5py=={} and numpy=={} versions detected. \n"
                               "Array reading/decoding may not proceed as expected. \n"
                               "Please upgrade to the latest compatible verions"
                               "".format(h5py.__version__, np.__version__))


def _clean(value):
    """ Convert numpy numeric types to their python equivalents. """
    if isinstance(value, np.ndarray):
        if value.dtype.kind == 'S':
            return np.char.decode(value).tolist()
        else:
            return value.tolist()
    elif type(value).__module__ == np.__name__:
        # h5py==2.8.0 on windows sometimes fails to cast this from an np.float64 to a python.float
        # We have to let the user do this themselves, since casting here could be dangerous
        # https://github.com/h5py/h5py/issues/1051
        conversion = value.item()  # np.asscalar(value) was deprecated in v1.16
        if isinstance(conversion, bytes):
            conversion = conversion.decode()
        return conversion
    elif isinstance(value, bytes):
        return value.decode()
    else:
        return value


def _sanitize_data_for_writing(data):
    # To make the interface more user friendly we encode python strings as  byte-strings when writing datasets
    check_version_compatibility()
    if isinstance(data, str):
        # Plain python-strings can be encoded trivially
        return data.encode()
    elif isinstance(data, np.ndarray) and data.dtype.kind == np.dtype(np.unicode):
        # If the array is all of one type, unicode-string, we can encode with numpy
        return data.astype('S')
    elif isinstance(data, np.ndarray) and len(data.dtype) > 1:
        # If the array is of mixed types we have to set the encoding column by column
        encoded_dtypes = []
        for field_name in data.dtype.names:
            field_dtype, field_byte_index = data.dtype.fields[field_name]
            if field_dtype.kind == 'U':
                str_len = field_dtype.itemsize // field_dtype.alignment
                field_dtype = np.dtype("|S{}".format(str_len))
            encoded_dtypes.append((field_name, field_dtype))
        try:
            return data.astype(encoded_dtypes)
        except (ValueError, UnicodeEncodeError):
            if parse_version(h5py.__version__) < parse_version("2.7"):
                raise UnicodeError("Cannot encode array with types: {}.\n"
                                   "There are known bugs in h5py<2.7 which yield non-deteministic results when decoding "
                                   "arrays with empty strings and additional bugs with compatibility between "
                                   "h5py<2.7 and numpy>=1.13 when decoding arrays with  mixed/padded data types.\n"
                                   "Please try upgrading to the latest h5py and numpy versions"
                                   "".format(encoded_dtypes))
            else:
                raise
    return data


def _sanitize_data_for_reading(data):
    # To make the interface more user friendly we decode byte-strings into unicode strings when reading datasets
    check_version_compatibility()
    if isinstance(data, h5py.Dataset):
        data = data[()]

    if isinstance(data, bytes):
        # Plain byte-strings can be decoded trivially
        return data.decode()
    elif isinstance(data, np.ndarray) and data.dtype.kind == 'S':
        # If the array is all of one type, byte-string, we can decode with numpy
        return np.char.decode(data)
    elif isinstance(data, np.ndarray) and len(data.dtype) > 1:
        # If the array is of mixed types we have to decode column by column
        decoded_dtypes = []
        for field_name in data.dtype.names:
            field_dtype, field_byte_index = data.dtype.fields[field_name]
            if field_dtype.kind == 'S':
                field_dtype = np.dtype("<U{}".format(field_dtype.itemsize))
            decoded_dtypes.append((field_name, field_dtype))
        try:
            return data.astype(decoded_dtypes)
        except (UnicodeDecodeError, SystemError):
            # On h5py==2.6 we can't decode padded string-arrays properly - we should advise users to upgrade
            if parse_version(h5py.__version__) < parse_version("2.7"):
                raise UnicodeError("Cannot encode array with types: {}.\n"
                                   "There are known bugs in h5py<2.7 which yield non-deteministic results when decoding "
                                   "arrays with empty strings and additional bugs with compatibility between "
                                   "h5py<2.7 and numpy>=1.13 when decoding arrays with  mixed/padded data types.\n"
                                   "Please try upgrading to the latest h5py and numpy versions".format(decoded_dtypes))
            else:
                raise
    return data

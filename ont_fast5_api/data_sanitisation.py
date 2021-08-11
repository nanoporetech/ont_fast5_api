import h5py
import numpy as np


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
        return data.astype(encoded_dtypes)

    return data


def _sanitize_data_for_reading(data):
    # To make the interface more user friendly we decode byte-strings into unicode strings when reading datasets
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
        return data.astype(decoded_dtypes)

    return data

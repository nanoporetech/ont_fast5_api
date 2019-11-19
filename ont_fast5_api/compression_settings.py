import pkg_resources
import sys


def register_plugin():
    plugin_path = pkg_resources.resource_filename('ont_fast5_api', 'vbz_plugin')
    try:
        from h5py import h5pl
        if sys.version_info.major == 2:
            # TODO we can remove this when we drop python2 support
            h5pl.prepend(bytes(plugin_path))
        else:
            h5pl.prepend(bytes(plugin_path, 'UTF-8'))
    except ImportError:
        # We don't have the plugin library in h5py<2.10 so we fall back on an environment variable
        import os
        os.environ['HDF5_PLUGIN_PATH'] = plugin_path
    return plugin_path


class AbtractCompression:
    compression = "AbtractCompression"
    compression_opts = ()
    shuffle = False
    scaleoffset = False
    fletcher32 = False

    def __repr__(self):
        return self.compression


class VbzCompression(AbtractCompression):
    def __init__(self):
        self.compression = 32020  # https://portal.hdfgroup.org/display/support/Registered+Filters
        self.compression_opts = (1, 2, 1, 1)  # VBZ_VERSION, VBZ_PACKING, VBZ_ZIG_ZAG, VBZ_ZSTD_COMPRESSION

    def __repr__(self):
        return "vbz"


class GzipCompression(AbtractCompression):
    def __init__(self):
        self.compression = "gzip"
        self.shuffle = True


VBZ_ERROR_MESSAGE = "Failed to read compressed raw data. " \
                    "VBZ compression filter (id=32020) may be missing from expected path: '{}'"


def raise_missing_vbz_error_read(err):
    if str(VBZ.compression) in str(err):
        # TODO python2 compatibility - we can replace this with 'raise Exception() from e'
        from six import raise_from
        raise_from(IOError(VBZ_ERROR_MESSAGE.format(register_plugin())), err)
    # If we don't see anything relating to VBZ just raise the existing error without additional info
    raise


def raise_missing_vbz_error_write(err):
    if type(err) is OSError and "Can't read data" in str(err):
        # TODO python2 compatibility - we can replace this with 'raise Exception() from e'
        from six import raise_from
        raise_from(IOError(VBZ_ERROR_MESSAGE.format(register_plugin())), err)
    # If we don't see anything relating to VBZ just raise the existing error without additional info
    raise


VBZ = VbzCompression()
GZIP = GzipCompression()

COMPRESSION_MAP = {str(comp): comp for comp in (VBZ, GZIP)}

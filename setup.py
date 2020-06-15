import os
import re
from setuptools import setup, find_packages

__pkg_name__ = 'ont_fast5_api'


def get_version():
    init_file = os.path.join(__pkg_name__, '__init__.py')
    with open(init_file, 'r') as init_fh:
        verstrline = init_fh.read()
    vsre = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(vsre, verstrline, re.M)
    if mo:
        return mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in '{}'".format(init_file))


with open('README.rst') as readme:
    documentation = readme.read()

installation_requirements = []
if 'IGNORE_INCLUDES' not in os.environ:
    installation_requirements = ['h5py>=2.6', 'numpy>=1.11',
                                 'packaging', 'progressbar33>=2.3.1']

setup(name=__pkg_name__.replace("_", "-"),
      author='Oxford Nanopore Technologies, Limited',
      description='Oxford Nanopore Technologies fast5 API software',
      license='MPL 2.0',
      long_description=documentation,
      version=get_version(),
      url='https://github.com/nanoporetech/{}'.format(__pkg_name__),
      install_requires=installation_requirements,
      packages=find_packages(),
      package_data={__pkg_name__: ['vbz_plugin/*.so', 'vbz_plugin/*.dylib', 'vbz_plugin/*.dll']},
      python_requires='>=3.5',
      entry_points={'console_scripts': [
          "multi_to_single_fast5={}.conversion_tools.multi_to_single_fast5:main".format(__pkg_name__),
          "single_to_multi_fast5={}.conversion_tools.single_to_multi_fast5:main".format(__pkg_name__),
          "fast5_subset={}.conversion_tools.fast5_subset:main".format(__pkg_name__),
          "compress_fast5={}.conversion_tools.compress_fast5:main".format(__pkg_name__),
          "check_compression={}.conversion_tools.check_file_compression:main".format(__pkg_name__),
      ]},
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
          'Natural Language :: English',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX :: Linux',
          'Operating System :: MacOS',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
      ],
      keywords='fast5 nanopore')

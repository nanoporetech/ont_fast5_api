import os
import re
from setuptools import setup

verstrline = open('ont_fast5_api/__init__.py', 'r').read()
vsre = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(vsre, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError('Unable to find version string in "ont_fast5_api/__init__.py".')

installation_requirements = []
if 'IGNORE_INCLUDES' not in os.environ:
    installation_requirements = ['h5py', 'numpy>=1.8.1']

setup(name='ont-fast5-api',
      author='Oxford Nanopore Technologies, Limited',
      description='Oxford Nanopore Technologies fast5 API software',
      long_description=open('README.md').read(),
      version=verstr,
      url='https://github.com/nanoporetech/ont_fast5_api',
      install_requires=installation_requirements,
      license='MPL 2.0',
      packages=['ont_fast5_api', 'ont_fast5_api.analysis_tools'],
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
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
      ],
      keywords='fast5 nanopore')

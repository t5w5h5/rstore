# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.

import codecs
import os
from setuptools import setup, Command


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system('rm -vrf ./build ./dist ./*.egg-info')


def read_file(filename):
    """Read a utf8 encoded text file and return its contents."""
    with codecs.open(filename, 'r', 'utf8') as f:
        return f.read()


setup(
    name='rlib-store',
    packages=['rstore'],
    version='0.1',
    description='General purpose store for key/value pairs, time series and event sequences with optional encryption.',
    long_description=read_file('README.rst'),
    author='t5w5h5',
    author_email='t5w5h5@gmail.com',
    url='https://github.com/t5w5h5/rstore',
    download_url = 'https://github.com/t5w5h5/rstore/archive/0.1.tar.gz',
    keywords=['database'],
    license='MIT',
    platforms='any',
    zip_safe=False,
    extras_require={
        'PostgreSQL': ['psycopg2'],
        'Redis': ['redis'],
        'Cryptography': ['cryptography']
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    cmdclass={'clean': CleanCommand}
)

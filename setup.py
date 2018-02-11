"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
# To use a consistent encoding
from codecs import open
from os import path
from pkg_resources import require, DistributionNotFound, VersionConflict
import sys

maj_ver = sys.version_info.major

if maj_ver == 3:
    from setuptools import setup, find_packages
else:
    from distutils.core import setup

try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print("warning: pypandoc module not found, could not convert Markdown to RST")
    read_md = lambda f: open(f, 'r').read()

here = path.abspath(path.dirname(__file__))
# Get the long description from the README file
long_description=read_md('README.md')

try:
    require('ConflictingDistribution')
    print()
    print ('You have ConflictingDistribution installed.')
    print ( 'You need to remove ConflictingDistribution from your site-packages')
    print ( 'before installing this software, or conflicts may result.')
    print ()
    sys.exit()

except (DistributionNotFound, VersionConflict):
    pass

if maj_ver == 3:
    setup(
        name='dattasa',
        packages=find_packages(),
        # Versions should comply with PEP440.  For a discussion on single-sourcing
        # the version across setup3.py and the project code, see
        # https://packaging.python.org/en/latest/single_source_version.html
        version='1.1',

        description='Python wrapper for connecting to postgres/greenplum, mysql, mongodb, kafka, redis, mixpanel and salesforce.\
        Also included are modules for performing secure file transfer and sourcing environment variables.',
        long_description=long_description,

        # The project's main homepage.
        url='https://github.com/kartikra/dattasa',

        # Author details
        author='Kartik Ramasubramanian',
        author_email='r.kartik@berkeley.edu',

        # Choose your license
        license='GNU',

        # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
        classifiers=[
            # How mature is this project? Common values are
            #   3 - Alpha
            #   4 - Beta
            #   5 - Production/Stable
            'Development Status :: 4 - Beta',

            # Indicate who your project is intended for

            # Pick your license as you wish (should match "license" above)
            'License :: OSI Approved :: MIT License',

            # Specify the Python versions you support here. In particular, ensure
            # that you indicate whether you support Python 2, Python 3 or both.
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
        ],

        # What does your project relate to?
        keywords='greenplum postgres mysql mongodb kafka redis rabbitmq salesforce mixpanel delighted wootric data pipeline',

        # You can just specify the packages manually here if your project is
        # simple. Or you can use find_packages().
        # packages=find_packages(exclude=['contrib', 'docs', 'tests']),

        # Alternatively, if you want to distribute just a my_module.py, uncomment
        # this:
        #   py_modules=["my_module"],

        # List run-time dependencies here.  These will be installed by pip when
        # your project is installed. For an analysis of "install_requires" vs pip's
        # requirements files see:
        # https://packaging.python.org/en/latest/requirements.html
        install_requires=['paramiko', 'pyyaml', 'simple_salesforce', 'pymongo', 'python-gnupg',
                          'psycopg2', 'PyMySQL', 'sqlalchemy', 'redis', 'pika', 'kafka', 'kafka-python'],

        # List additional groups of dependencies here (e.g. development
        # dependencies). You can install these using the following syntax,
        # for example:
        # $ pip install -e .[dev,test]

    )

else:
    setup(
        name='dattasa',
        version='1.1',
        packages=['dattasa'],
        url='https://github.com/kartikra/dattasa',
        license='MIT',
        author='Kartik Ramasubramanian',
        author_email='r.kartik@berkeley.edu',
        description='Python wrapper for connecting to postgres/greenplum, mysql, mongodb, kafka, redis, mixpanel  salesforce \
        delighted and wootric. Also included are modules for performing secure file transfer and sourcing environment variables.'
    )

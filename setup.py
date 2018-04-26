#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools.command.test import test as TestCommand
import sys
import os
import re
from pip.req import parse_requirements

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.md') as readme_file:
    readme = readme_file.read()

# parse version
with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       'cattledb', "__init__.py")) as fdp:
    pattern = re.compile(r".*__version__ = '(.*?)'", re.S)
    VERSION = pattern.match(fdp.read()).group(1)

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

test_requirements = [
    "pytest"
]


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)

setup(
    name='cattledb',
    version=VERSION,
    description="Storing Anthill data",
    long_description=readme,
    author="Matthias Wutte",
    author_email='matthias.wutte@smaxtec.com',
    url='https://anthill.smaxtec.com',
    packages=[
        'cattledb',
    ],
    package_dir={'cattledb':
                 'cattledb'},
    include_package_data=True,
    install_requires=reqs,
    zip_safe=False,
    keywords='cattledb',
    test_suite='tests',
    tests_require=test_requirements,
    cmdclass={'test': PyTest},
    entry_points={'console_scripts':
                  ['run-the-app = cattledb.__main__:main']}
)

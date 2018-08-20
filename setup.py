#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from builtins import str

from setuptools.command.test import test as TestCommand
import sys
import os
import re

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('readme.md') as readme_file:
    readme = readme_file.read()

# parse version
with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       'cattledb', "__init__.py")) as fdp:
    pattern = re.compile(r".*__version__ = '(.*?)'", re.S)
    VERSION = pattern.match(fdp.read()).group(1)

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=False)
server_reqs = parse_requirements('requirements_server.txt', session=False)
httpserver_reqs = parse_requirements('requirements_httpserver.txt', session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]
s_reqs = [str(ir.req) for ir in server_reqs]
hs_reqs = [str(ir.req) for ir in httpserver_reqs]

test_reqs = [
    "pytest",
    "mock"
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
    description="Device Data Store on BigTable",
    long_description=readme,
    author="Matthias Wutte",
    author_email='matthias.wutte@gmail.com',
    url='https://github.com/wuttem',
    packages=[
        'cattledb',
    ],
    extras_require={
        'server':  s_reqs,
        'httpserver': hs_reqs
    },
    package_dir={'cattledb':
                 'cattledb'},
    include_package_data=True,
    install_requires=reqs,
    dependency_links=['git+https://github.com/trauter/google-cloud-python-happybase.git#egg=google-cloud-happybase'],
    zip_safe=False,
    keywords='cattledb',
    test_suite='tests',
    tests_require=test_reqs,
    cmdclass={'test': PyTest},
    # entry_points={'console_scripts':
    #               ['run-the-app = cattledb.__main__:main']}
)

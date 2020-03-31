#!/usr/bin/env python
# -*- coding: utf-8 -*-
from builtins import str

from setuptools.command.test import test as TestCommand

import sys
import os
import re
import subprocess
import platform

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

try:
    from setuptools import setup, Extension, Command, find_packages
except ImportError:
    from distutils.core import setup, Extension, Command, find_packages

from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError

with open('README.md') as readme_file:
    readme = readme_file.read()

# parse version
with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                       'cattledb', "__init__.py")) as fdp:
    pattern = re.compile(r".*__version__ = '(.*?)'", re.S)
    VERSION = pattern.match(fdp.read()).group(1)

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements('requirements.txt', session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

test_reqs = [
    "pytest",
    "mock"
]

extra_reqs = {
    "rest": ["Flask>=1.1.1"],
}

entry_points = {
    "console_scripts": [
        "cattledb=cattledb.commands:cli"
    ]
}


class BuildFailed(Exception):
    pass


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise BuildFailed("CMake must be installed to build the following extensions: " +
                               ", ".join(e.name for e in self.extensions))

        if platform.system() == "Windows":
            cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise BuildFailed("CMake >= 3.1.0 is required on Windows")

        try:
            for ext in self.extensions:
                self.build_extension(ext)
        except (CCompilerError, DistutilsExecError, DistutilsPlatformError, subprocess.CalledProcessError) as ex:
            raise BuildFailed("Error in Extension Build: {}".format(ex))

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(cfg.upper(), extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
            build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
            build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(env.get('CXXFLAGS', ''),
                                                              self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=self.build_temp)


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ["tests"]
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


def run_setup(with_extension):
    cmdclass = dict(test=PyTest)

    if with_extension:
        all_reqs = [x for x in reqs] + ["cmake"]
        kw = dict(
            ext_modules = [
                CMakeExtension("cdb_ext_ts"),
            ],
            cmdclass=dict(cmdclass, build_ext=CMakeBuild),
        )
    else:
        all_reqs = [x for x in reqs]
        kw = dict(cmdclass=cmdclass)

    print("CattleDB Requirements: {}".format(all_reqs))

    setup(
        name='cattledb',
        version=VERSION,
        description="Device Data Store on BigTable",
        long_description=readme,
        author="Matthias Wutte",
        author_email='matthias.wutte@gmail.com',
        url='https://github.com/wuttem',
        extras_require=extra_reqs,
        packages=find_packages(),
        package_dir={'cattledb':
                     'cattledb'},
        include_package_data=True,
        install_requires=all_reqs,
        dependency_links=[],
        entry_points=entry_points,
        zip_safe=False,
        keywords='cattledb',
        test_suite='tests',
        tests_require=test_reqs,
        **kw
    )


try:
    run_setup(True)
except BuildFailed as ex:
    if os.environ.get('REQUIRE_SPEEDUPS'):
        raise
    print('*' * 75)
    print(ex)
    print("WARNING: The C extension could not be compiled.")
    print("Failure information, if any, is above.")
    print("I'm retrying the build without the C extension now.")
    print('*' * 75)

    run_setup(False)

    print('*' * 75)
    print("Plain-Python installation succeeded.")
    print('*' * 75)

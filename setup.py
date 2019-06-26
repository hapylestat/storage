#!/usr/bin/env python3

import codecs
import os
import re

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
  with codecs.open(os.path.join(here, *parts), 'r') as fp:
    return fp.read()


def find_version(*file_paths):
  version_file = read(*file_paths)
  version_match = re.search(
    r"^__version__ = ['\"]([^'\"]*)['\"]",
    version_file,
    re.M,
  )
  if version_match:
    return version_match.group(1)

  raise RuntimeError("Unable to find version string.")


def load_requirements():
  data = read("requirements.txt")
  return data.split("\n")


setup(
  name="storage",
  version=find_version("src", "storage", "internal", "__init__.py"),
  description="Storage cli interface",
  long_description="Storage cli interface",
  license='MIT',
  classifiers=[
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
  ],

  author='The storage developers',

  package_dir={"": "src"},
  packages=find_packages(
    where="src",
    exclude=["contrib", "docs", "tests*", "tasks"],
  ),
  install_requires=load_requirements(),
  entry_points={
    "console_scripts": [
      "storage=storage.internal:main_entry",
      ],
  },
  zip_safe=True,
  python_requires='>=3.6',
)

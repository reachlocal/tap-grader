#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-grader",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["grader"],
    install_requires=[
        "singer-python",
        "futures3==1.0.0",
        "pymongo"
    ],
    entry_points="""
    [console_scripts]
    tap-grader=tap_grader:main
    """,
    packages=["tap_grader"],
    package_data = {
        "schemas": ["tap_grader/schemas/*.json"]
    },
    include_package_data=True,
)

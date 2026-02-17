"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup

LOCAL_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
]

setup(
    name="my_package",
    setup_requires=["setuptools","wheel"],
    packages=find_packages(exclude=["tests", "tests.*"]),
    extras_require={"local": LOCAL_REQUIREMENTS},
    entry_points = {
        "console_scripts": [
            "etl_job = my_package.tasks.ufo_etl_job:entrypoint"]},
)
# noinspection Mypy

from setuptools import setup, find_packages
from os import path, getcwd

# from https://packaging.python.org/tutorials/packaging-projects/

# noinspection SpellCheckingInspection
package_name = "sparkautomapper"

with open("README.md", "r") as fh:
    long_description = fh.read()

try:
    with open(path.join(getcwd(), "VERSION")) as version_file:
        version = version_file.read().strip()
except IOError:
    raise

# classifiers list is here: https://pypi.org/classifiers/

# create the package setup
setup(
    install_requires=[
        "py4j==0.10.9.7",
        "pyspark==3.5.5",
        "logger>=1.4",
        "sparkdataframecomparer==2.0.14",
        "deprecated>=1.2.12",
        "numpy>=1.15",
    ],
    name=package_name,
    version=version,
    author="Imran Qureshi",
    author_email="imranq2@hotmail.com",
    description="AutoMapper for Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/imranq2/SparkAutoMapper",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3",
    dependency_links=[],
    include_package_data=True,
    zip_safe=False,
    package_data={package_name: ["py.typed"]},
    data_files=[],
)

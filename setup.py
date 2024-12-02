import setuptools
import os

def read_version():
    if not os.path.exists("version.txt"):
        return '0.1.0'
    with open("version.txt", "r") as fh:
        version = fh.read().strip()
    return version

def increment_version(version):
    major, minor, patch = map(int, version.split('.'))
    patch += 1
    return f"{major}.{minor}.{patch}"

def write_version(version):
    with open("version.txt", "w") as fh:
        fh.write(version)

current_version = read_version()
new_version = increment_version(current_version)
write_version(new_version)

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="DatabricksHelper", # Replace with your own username
    version='0.1.0',
    author="binyan",
    author_email="bin.y@live.com",
    description="Databricks Helper",
    long_description="Databricks Helper",
    long_description_content_type="text/markdown",
    url="https://github.com/binyancode/DatabricksLibrary.git",
    packages=['DatabricksHelper'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
)
import setuptools
import os
import datetime
import shutil

def read_version():
    with open('version.txt', 'r') as file:
        lines = file.readlines()
        if lines:
            return lines[-1].strip().split(',')[1]
        return '0.1.0'

def increment_version(version):
    major, minor, patch = map(int, version.split('.'))
    patch += 1
    return f"{major}.{minor}.{patch}"

def write_version(version):
    with open('version.txt', 'a') as file:
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f"{current_time},{version}\n")

def write_library_version(version):
    with open("DatabricksHelper/Version.py", "r") as fh:
        lines = fh.readlines()
    with open("DatabricksHelper/Version.py", "w") as fh:
        for line in lines:
            if line.startswith('__version__'):
                line = f"__version__ = '{version}'\n"
            fh.write(line)
            
def backup_databricks_helper(version):
    src_folder = "DatabricksHelper"
    dst_folder = f"Backup/DatabricksHelper_{version}"
    shutil.copytree(src_folder, dst_folder)

current_version = read_version()
new_version = increment_version(current_version)
write_version(new_version)
write_library_version(new_version)

backup_databricks_helper(new_version)

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
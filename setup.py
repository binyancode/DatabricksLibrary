import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="DatabricksHelper", # Replace with your own username
    version="0.1.0",
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
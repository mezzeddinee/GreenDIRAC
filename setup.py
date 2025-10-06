from setuptools import setup, find_packages

setup(
    name="greendirac",                   # Package name on PyPI
    version="0.0.1",
    author="CPPM/CNRS",
    author_email="atsareg@in2p3.fr",
    description="GreenDIGIT extension to DIRAC",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/GreenDIGIT-project/GreenDIRAC",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)

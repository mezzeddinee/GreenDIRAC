from setuptools import setup, find_packages

setup(
    name="GreenDIRAC",                   # Package name on PyPI
    version="0.1.0",
    author="Your Name",
    author_email="you@example.com",
    description="A short description of your package",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/GreenDIGIT-project/GreenDIRAC",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)

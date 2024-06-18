from setuptools import setup, find_packages

setup(
    name='library',
    version='1.0.0',
    packages=find_packages(),
    install_requires = [
        'azure-storage-blob',
        'pandas',
        'pyspark',
        'dataclasses',
    ]
)
from setuptools import setup, find_packages
"""
Command to build wheel files:
            python setup.py bdist_wheel
"""
setup(
    name='data_ingestion_framework',
    version='1.0',
    author="Akhil Kamal",
    author_email="akhil.kamal@xyz.com",
    packages=find_packages(),
)

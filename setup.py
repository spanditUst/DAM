from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name='Data Access Module',
    version='0.1',
    author='Santosh Kumar Pandit',
    author_email='santosh.pandit@ust-global.com',
    packages=find_packages(),
    long_description=open('README.md').read()

)
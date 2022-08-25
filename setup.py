from setuptools import setup, find_packages

with open('requirements.txt') as f:
	install_requires = f.read().strip().split('\n')

# get version from __version__ variable in temporal_table/__init__.py
from temporal_table import __version__ as version

setup(
	name='temporal_table',
	version=version,
	description='Temporal table for import Journal Entry',
	author='Mentum',
	author_email='aryrosa.fuentes@MENTUM.group',
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)

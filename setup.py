from setuptools import setup, find_packages

exec(open('gandalf/_version.py').read())

setup(
    name='gandalf',
    version=__version__,
    description='Validating AMP decisions',
    url='https://github.kdc.capitalone.com/IanWhitestone/gandalf',
    author='Ian Whitestone',
    author_email='ian.whitestone@capitalone.com',
    license='Copyright (C) Capital One',
    packages=find_packages(),
    entry_points = {
        'console_scripts': [
            'gandalf = gandalf.__main__:main'
            ]
        },
    include_package_data=True,
    install_requires=[
        'cerberus==1.2', 'PyYAML==5.4.1', 'glom==18.1.1', 'dask==2021.10.0',
        's3fs==0.1.6', 'fastavro==0.21.4'
    ],
    zip_safe=False,
)

from setuptools import setup, find_packages

setup(
    name='py27maasclient',
    version='0.0.1',
    description='MAAS client that works with python2.7',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=[
        'requests_oauthlib',
        ],
)

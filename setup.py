from setuptools import setup, find_packages

setup(
    name='db',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
        'pytest',
    ],
    entry_points={
        'console_scripts': [
            'run-database=db.data:main',
        ],
    },
)

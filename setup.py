from setuptools import find_packages, setup

setup(
    name="mongo_bulk_updater",
    packages=find_packages(),
    version="1.0.0",
    description="Async bulk updater for MongoDB from parquet files",
    author="Dim Kharitonov",
    author_email="dimdasci@fastmail.com",
    license="MIT License",
    python_requires=">=3.9",
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'generate_data = src.cli.generate_data:main',
            'mongo_update = src.cli.mongo_update:main'
        ],
    },
)

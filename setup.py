from setuptools import setup

setup(
    name="db_bench",
    version="0.1",
    packages=['bench', 'db'],
    install_requires=[
        'sqlalchemy',
        'alembic',
        'psycopg2-binary',
        'psutil',
        'xxhash',
        'lmdb',
        'redis',
        'trino',
        'pandas',
        'Faker',
        'mysqlclient',
    ],
)

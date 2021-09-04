# -*- coding: utf-8 -*-
import logging

import sqlalchemy
from sqlalchemy.orm import sessionmaker

# log level
# if you want to see SQL in logs uncomment this line
# logging.getLogger('sqlalchemy.engine').setLevel('INFO')


POOL_SIZE = 1


def create_session_maker(db_url, pool_size=POOL_SIZE):
    return sessionmaker(bind=sqlalchemy.create_engine(db_url, pool_size=pool_size))


class SessionContext:
    def __init__(self, cls):
        self._session = cls()

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()
        self._session = None


# with get_db_session() as session:
#     session.add(model)
#     session.commit()
get_db_session = SessionContext

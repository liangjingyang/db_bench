import logging

from sqlalchemy import Column, BIGINT, String, Integer, Text, text, Boolean, Float, func
from sqlalchemy.orm import declarative_base

_logger = logging.getLogger(__name__)


Base = declarative_base()


class RCommit(Base):
    __tablename__ = 'r_commit'

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    analysis_id = Column(String(40))
    hexsha = Column(String(40))

    author_email = Column(String(255))
    author_name = Column(String(255))
    author_time = Column(Integer)
    author_offset = Column(Integer)
    committer_email = Column(String(255))
    committer_name = Column(String(255))
    committer_time = Column(Integer)
    committer_offset = Column(Integer)
    message = Column(Text)
    parents = Column(Text)
    files_changed = Column(Integer, nullable=False, server_default=text("0"))
    insertions = Column(Integer, nullable=False, server_default=text("0"))
    deletions = Column(Integer, nullable=False, server_default=text("0"))
    supported_insertions = Column(Integer, nullable=False, server_default=text("0"))
    supported_deletions = Column(Integer, nullable=False, server_default=text("0"))
    complexity = Column(Integer, nullable=False, server_default=text("0"))
    cyclomatic_complexity = Column(Integer, nullable=False, server_default=text("0"))
    big_cc_func_count = Column(Integer, nullable=False, server_default=text("0"))
    cherry_pick_from = Column(String(40))
    large_insertion = Column(Boolean, nullable=False, server_default=text("false"))
    large_deletion = Column(Boolean, nullable=False, server_default=text("false"))
    revert = Column(Boolean, nullable=False, server_default=text("false"))

    dev_eq = Column(Integer, nullable=False, server_default=text("0"))
    dev_rank = Column(Float(53), nullable=False, server_default=text("0"))

    @staticmethod
    def get_rand_analysis_id(session, limit: int = 10):
        dialect = session.bind.dialect.name
        query = session.query(RCommit.analysis_id).group_by(RCommit.analysis_id)
        if dialect == 'MySQL':
            query = query.order_by(func.rand())
        elif dialect == 'PostgreSQL' or dialect == 'SQLite':
            query = query.order_by(func.random())
        return query.limit(limit).all()



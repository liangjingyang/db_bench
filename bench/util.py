
def create_db():

    pass


def drop_db():
    pass


class TestDB:
    def __init__(self):
        create_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        drop_db()

from sql.query import Q


class Field(Q):
    name = None
    table = None

    def __init__(self, name=None):
        self.name = name

    def __str__(self):
        if self.table:
            return f'"{self.table.__alias__}"."{self.name}"'
        else:
            raise Exception('Unbounded field')

    def __repr__(self):
        if self.table:
            return f'{self.table} field {self}'
        else:
            raise Exception('Unbounded field')

    def compile(self, args):
        return str(self)

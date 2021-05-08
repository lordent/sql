from sql.query import Q


class Field(Q):
    name = None
    table = None

    def __init__(self, name=None, column_type='text', default=None, nullable=True,
                 unique=False, primary=False,
                 help='', verbose_name=''):
        self.column_type = column_type
        self.name = name
        self.default = default
        self.nullable = nullable
        self.unique = unique
        self.primary = primary
        self.help = help
        self.verbose_name = verbose_name

    def __str__(self):
        if self.table:
            return f'"{self.table.Meta.alias}"."{self.name}"'
        else:
            return f'"{self.name}"'

    def __repr__(self):
        if self.table:
            return f'{self.table} field {self}'
        else:
            return super().__repr__()

    def compile_constraint(self, args):
        return ''

    def compile(self, args):
        return str(self)

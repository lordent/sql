from sql.query import Q
from sql.mixins import SelectValuesMixin


class Select(SelectValuesMixin, Q):

    alias = ''

    def __init__(self, *args, **kwargs):
        self.dependencies = set()

        self._fields = {}
        self._joins = {}
        self._filters = []
        self._groups = []
        self._limit = None, None

        self.values(*args, **kwargs)

    def join(self, table, condition, mode='LEFT'):
        self.dependencies.discard(table)
        self._joins[table] = mode, condition
        return self

    def join_right(self, table, condition):
        return self.join(table, condition, mode='RIGHT')

    def join_full(self, table, condition):
        return self.join(table, condition, mode='FULL')

    def filter(self, *args):
        self._filters.extend(args)
        return self

    def group(self, *args):
        self._groups = args
        return self

    def _compile_values(self, args):
        values = []

        for name, value in self._fields.items():
            if isinstance(value, Q):
                values.append(f'{value.compile(args)} "{name}"')
            elif isinstance(value, dict):
                values.append(f'{self._json_build_object_recursive(value, args)} "{name}"')
            else:
                values.append(f'{{}} "{name}"')
                args.append(value)

        return ', '.join(values)

    def _compile_dependencies(self):
        dependencies = []
        for table in self.dependencies - set(self._joins):
            dependencies.append(str(table))
        return ', '.join(dependencies)

    def _compile_join(self, args):
        joins = []
        for table, (mode, condition) in self._joins.items():
            if isinstance(condition, Q):
                joins.append(
                    f'{mode} JOIN {table} ON {condition.compile(args)}'
                )
        return joins

    def _compile_filters(self, args):
        filters = []
        for condition in self._filters:
            if isinstance(condition, Q):
                filters.append(condition.compile(args))
        return ' AND '.join(filters)

    def _compile_group_by(self, args):
        groups = []
        for value in self._groups:
            if isinstance(value, Q):
                groups.append(value.compile(args))
        return ', '.join(groups)

    def _compile_limits(self, args):
        limits = []

        offset, limit = self._limit
        if offset is not None:
            args.append(offset)
            limits.append('OFFSET {}')

        if limit is not None:
            args.append(limit)
            limits.append('LIMIT {}')

        return limits

    def compile(self, args):
        sql = ['SELECT']

        sql.append(self._compile_values(args))
        sql.append('FROM')
        sql.append(self._compile_dependencies())
        sql.extend(self._compile_join(args))

        filters = self._compile_filters(args)
        if filters:
            sql.append('WHERE')
            sql.append(filters)

        groups = self._compile_group_by(args)
        if groups:
            sql.append('GROUP BY')
            sql.append(groups)

        # TODO: Order by
        # TODO: Having

        sql.extend(self._compile_limits(args))

        return ' '.join(sql)

    def __getitem__(self, val):
        if isinstance(val, slice):
            self._limit = val.start, val.stop
        else:
            self._limit = None, val
        return self

    # Finalize methods

    def __iter__(self):
        args = []
        yield self.compile(args).format(*(
            f'${i}' for i in range(1, len(args) + 1)
        ))
        yield from args

    def exists(self):
        iterator = iter(self)
        yield f'SELECT 1 FROM ({next(iterator)})'
        yield from iterator

    def as_json(self):
        iterator = iter(self)
        yield f'''SELECT JSON_AGG(ROW_TO_JSON) FROM (SELECT ROW_TO_JSON(s) FROM ({next(iterator)}) s) s'''
        yield from iterator

    def as_list(self):
        iterator = iter(self)
        yield f'SELECT ARRAY({next(iterator)})'
        yield from iterator

    def count(self):
        iterator = iter(self)
        yield f'SELECT COUNT(1) FROM ({next(iterator)})'
        yield from iterator

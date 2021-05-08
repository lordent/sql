class F:

    def __init__(self, query: str):
        self.query = query

    def __str__(self):
        return self.query


class Q:

    def __init__(self, query: str, *args, _dependencies=None, **kwargs):
        self.query = query
        self.dependencies = _dependencies or set()
        self.args = args

        if kwargs:
            for key, val in kwargs.items():
                self.args += (val, )
                kwargs[key] = '{}'
            self.query = query % kwargs

    def __str__(self):
        return f'<query: {self.query.format(*self.args)}>'

    def __repr__(self):
        return str(self)

    def compile(self, args):
        args.extend(self.args)
        return self.query

    # Operators

    def __operand__(self, operand, value, before='', after=''):
        args = []
        query = self.compile(args)

        if isinstance(value, Q):
            value_args = []
            value_query = value.compile(value_args)
            return Q(
                f'(({query}) {operand} {before}({value_query}){after})',
                *args, *value_args,
                _dependencies=(
                    self.dependencies.union(value.dependencies)
                    if value.dependencies else self.dependencies
                )
            )
        else:
            return Q(
                f'(({query}) {operand} {before}{{}}{after})',
                *args, value,
                _dependencies=self.dependencies
            )

    def __eq__(self, val):
        return self.__operand__('=', val)

    def __ne__(self, val):
        return self.__operand__('<>', val)

    def __or__(self, val):
        return self.__operand__('OR', val)

    def __and__(self, val):
        return self.__operand__('AND', val)

    def __ge__(self, val):
        return self.__operand__('>=', val)

    def __le__(self, val):
        return self.__operand__('<=', val)

    def __lt__(self, val):
        return self.__operand__('<', val)

    def __gt__(self, val):
        return self.__operand__('>', val)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.range(key.start, key.stop)

        # Json path
        args = []
        query = self.compile(args)

        path_chunks = query.rsplit('->>', 1)
        if len(path_chunks) > 1:
            return Q(
                f'({path_chunks[0]})->{path_chunks[~0]}->>{{}}',
                *args, key
            )
        else:
            return Q(f'({path_chunks[0]})->>{{}}', *args, key)

    def any(self, val):
        return self.__operand__('IN', val)

    def range(self, start, stop):
        args = []
        query = self.compile(args)
        return Q(
            f'({query}) BETWEEN {{}} AND {{}}',
            *args, start, stop,
            _dependencies=self.dependencies
        )

    # LIKE

    def contains(self, value):
        return self.__operand__('LIKE', value, "'%' || ", " || '%'")

    def icontains(self, value):
        return self.__operand__('ILIKE', value, "'%' || ", " || '%'")

    def endswith(self, value):
        return self.__operand__('LIKE', value, "'%' || ")

    def iendswith(self, value):
        return self.__operand__('ILIKE', value, "'%' || ")

    def startswith(self, value):
        return self.__operand__('LIKE', value, after=" || '%'")

    def istartswith(self, value):
        return self.__operand__('ILIKE', value, after=" || '%'")

    def contains_regex(self, value):
        return self.__operand__('~', value)

    # ARRAY

    def array_contains(self, value):
        return self.__operand__('@>', value)

    def array_contained_by(self, value):
        return self.__operand__('<@', value)

    def array_overlap(self, value):
        return self.__operand__('&&', value)

    def array_concat(self, value):
        return self.__operand__('||', value)

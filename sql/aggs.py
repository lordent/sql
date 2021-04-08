from sql.query import Q
from sql.mixins import SelectValuesMixin


class List(SelectValuesMixin, Q):

    def __init__(self, *args, **kwargs):
        self.dependencies = set()
        self._fields = {}

        self.values(*args, **kwargs)

    def compile(self, args):
        return f'JSON_AGG({self._json_build_object_recursive(self._fields, args)})'

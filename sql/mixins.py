from sql.query import Q
from sql.field import Field


class SelectValuesMixin:

    def values(self, *args, **kwargs):
        for value in args:
            if isinstance(value, Field):
                self.dependencies.update(value.dependencies)
                self._fields[value.name] = value
            else:
                raise Exception('Position argument must be instance of Field')

        for name, value in kwargs.items():
            if isinstance(value, Q):
                self.dependencies.update(value.dependencies)
            elif isinstance(value, dict):
                self._fields[name] = value
                self._collect_dependencies_recursive(value)

            self._fields[name] = value

        return self

    def _collect_dependencies_recursive(self, values):
        for value in values.values():
            if isinstance(value, Q):
                self.dependencies.update(value.dependencies)
            elif isinstance(value, dict):
                self._collect_dependencies_recursive(value)

    def _json_build_object_recursive(self, fields, args):
        json_object = []
        for name, value in fields.items():
            if isinstance(value, Q):
                json_object.append(f"'{name}', {value.compile(args)}")
            elif isinstance(value, dict):
                json_object.append(f"'{name}', {self._json_build_object_recursive(value, args)}")
            else:
                json_object.append(f"'{name}', {{}}")
                args.append(value)

        json_object = ','.join(json_object)
        return f'JSON_BUILD_OBJECT({json_object})'

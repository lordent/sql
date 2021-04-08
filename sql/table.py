from copy import copy

from sql.field import Field


class ModelManager(type):
    aliases = {}
    tables = {}

    def __new__(mcs, name, bases, initial):
        if bases:
            initial.setdefault('__table__', f'{name.lower()}s')
            initial.setdefault('__alias__', initial['__table__'])

            cls = super().__new__(mcs, name, bases, initial)

            mcs.aliases[cls.__alias__] = cls
            if cls.__alias__ == cls.__table__:
                mcs.tables[cls.__table__] = cls

            cls.__fields__ = {}
            for field_name in dir(cls):
                field = getattr(cls, field_name)
                if isinstance(field, Field):
                    mcs.bind_field(cls, field, field_name)

            return cls
        else:
            return super().__new__(mcs, name, bases, initial)

    def __iter__(self):
        yield from self.__fields__.values()

    def __getitem__(cls, postfix):
        return (
            type(cls).aliases.get(f'{cls.__table__}{postfix}') or
            type(cls)(cls.__name__, (cls, ), {
                '__table__': cls.__name__,
                '__alias__': f'{cls.__table__}{postfix}',
            })
        )

    @classmethod
    def bind_field(mcs, table, field, name):
        field = copy(field)
        field.table = table
        field.dependencies = {table}
        field.name = field.name or name
        setattr(table, name, field)
        table.__fields__[name] = field
        return field


class Model(metaclass=ModelManager):
    id = Field()

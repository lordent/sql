from copy import copy

from sql.field import Field

ON_COMMIT_PRESERVE_ROWS = 'PRESERVE ROWS'
ON_COMMIT_DELETE_ROWS = 'DELETE ROWS'
ON_COMMIT_DROP = 'DROP'


class MetaManager(type):
    def __new__(mcs, name, bases, initial):
        cls = super().__new__(mcs, name, bases, initial)
        if 'model' in initial:
            cls.fields = {}
            cls.table_name = cls.table_name or cls.model.__name__.lower()
            cls.alias = cls.alias or cls.table_name
        return cls


class Meta(metaclass=MetaManager):
    alias = ''
    table_name = None
    verbose_name = None
    verbose_name_plural = None


class ModelManager(type):
    models = set()

    def __new__(mcs, name, bases, initial):
        if bases:
            alias = initial.pop('__alias__', '')

            cls = super().__new__(mcs, name, bases, initial)
            cls.Meta = create_meta(cls, alias, cls.Meta)

            if not alias:
                mcs.models.add(cls)

            for field_name in dir(cls):
                field = getattr(cls, field_name)
                if isinstance(field, Field):
                    bind_field(cls, field_name, field)

            return cls
        else:
            return super().__new__(mcs, name, bases, initial)

    def __iter__(self):
        yield from self.Meta.fields.values()

    def __getitem__(cls, alias):
        return ModelManager(cls.__name__, (cls, ), {
            '__alias__': alias,
        })

    def __str__(cls):
        if cls.Meta.alias == cls.Meta.table_name:
            return f'"{cls.Meta.table_name}"'
        else:
            return f'"{cls.Meta.table_name}" "{cls.Meta.alias}"'


class Model(metaclass=ModelManager):

    class Meta:
        pass

    id = Field(column_type='serial', nullable=False, primary=True)


def create_meta(model, alias, base_meta):
    return MetaManager('Meta', (base_meta, Meta), {
        'model': model,
        'alias': alias,
    })


def bind_field(cls, name, field):
    field = copy(field)
    field.table = cls
    field.dependencies = {cls}
    field.name = field.name or name
    setattr(cls, name, field)
    cls.Meta.fields[name] = field
    return field

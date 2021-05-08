import os
import pydoc
import json
import uuid

from sql.model import ModelManager
from sql.query import F

_structures = {}


class Operation:

    def __init__(self, conf):
        self.conf = conf

    def apply(self):
        raise NotImplemented

    def compile(self):
        raise NotImplemented


class AddColumn(Operation):

    def apply(self):
        table_name = self.conf['table_name']
        columns = _structures[table_name]['columns']
        columns[self.conf['column_name']] = self.conf['column']

    def compile(self):
        table_name = self.conf['table_name']
        column_sql = _get_column_sql(self.conf['column_name'], self.conf['column'])
        return f'ALTER TABLE "{table_name}" ADD COLUMN {column_sql}'


class DeleteColumn(Operation):

    def apply(self):
        table_name = self.conf['table_name']
        columns = _structures[table_name]['columns']
        del columns[self.conf['column_name']]

    def compile(self):
        table_name = self.conf['table_name']
        column_name = self.conf['column_name']
        return f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'


class AlterColumn(Operation):

    def apply(self):
        table_name = self.conf['table_name']
        column_name = self.conf['column_name']
        column = self.conf['column']

        columns = _structures[table_name]['columns']
        columns[column_name] = column

    def compile(self):
        table_name = self.conf['table_name']
        column_name = self.conf['column_name']
        column = self.conf['column']
        changes = self.conf['changes']

        alters = []

        alter_column_prefix = f'ALTER COLUMN "{column_name}"'

        for change in changes:
            if change == 'column_type':
                alters.append(
                    f'{alter_column_prefix} '
                    f'TYPE {column["column_type"]} '
                    f'USING "{column_name}"::{column["column_type"]}'
                )

            elif change == 'nullable':
                if column['nullable']:
                    alters.append(f'{alter_column_prefix} DROP NOT NULL')
                    if column['default'] is None:
                        alters.append(f'{alter_column_prefix} SET DEFAULT NULL')
                else:
                    alters.append(f'{alter_column_prefix} SET NOT NULL')
                    if column['default'] is None:
                        alters.append(f'{alter_column_prefix} DROP DEFAULT')

            elif change == 'unique':
                unique_constraint_name = f'{table_name}_{column_name}_key'

                if column['unique']:
                    alters.append(f'ADD UNIQUE ("{column_name}")')
                else:
                    alters.append(f'DROP CONSTRAINT "{unique_constraint_name}"')

            elif change == 'default':
                if column['default'] is None:
                    alters.append(f'{alter_column_prefix} SET DEFAULT NULL')
                else:
                    alters.append(f'{alter_column_prefix} SET DEFAULT {column["default"]}')

        return f'ALTER TABLE "{table_name}" {", ".join(alters)}'


class CreateTable(Operation):

    def apply(self):
        _structures[self.conf['name']] = self.conf

    def compile(self):
        table_name = self.conf['name']
        columns = self.conf['columns']

        sql = [f'CREATE TABLE "{table_name}" (']

        column_rules = []

        for column_name, column in columns.items():
            column_rules.append(_get_column_sql(column_name, column))

        sql.append(','.join(column_rules))

        sql.append(')')

        return ''.join(sql)


def _get_operations_diff(structure):
    operations = []

    table_name = structure['name']
    columns = structure['columns']

    current = _structures[table_name]
    current_columns = current['columns']

    current_columns_set = set(current_columns.keys())
    columns_set = set(columns.keys())

    for removed_column in current_columns_set - columns_set:
        operations.append((DeleteColumn, {
            'table_name': table_name,
            'column_name': removed_column,
        }))

    for added_column in columns_set - current_columns_set:
        operations.append((AddColumn, {
            'table_name': table_name,
            'column_name': added_column,
            'column': columns[added_column],
        }))

    for column_name in current_columns_set & columns_set:
        column = columns[column_name]
        current_column = current_columns[column_name]
        if current_column != column:
            changes = []

            for change_type in [
                'column_type',
                'nullable',
                'unique',
                'default',
            ]:
                if current_column[change_type] != column[change_type]:
                    changes.append(change_type)

            operations.append((AlterColumn, {
                'table_name': table_name,
                'column_name': column_name,
                'column': column,
                'changes': changes,
            }))

    return operations


def _apply_migration_structure(operations):
    for operation in operations:
        operation.apply()


def _create_migration_file(migrations_module_path, last_migration, operations):
    i = 0

    if last_migration:
        i = int(last_migration.split('_')[0]) + 1

    salt = str(uuid.uuid4()).split('-')[0]
    new_migration = f'{i:04d}_{salt}.py'

    fn = os.path.join(migrations_module_path.replace('.', os.path.sep), new_migration)
    with open(fn, 'w+') as fo:
        fo.write(
            'from sql import migration\n\n'
            'def up():\n'
            '    return (\n'
        )

        for operation_class, operation_kwargs in operations:
            conf = json.dumps(operation_kwargs, sort_keys=True)
            conf = json.loads(conf)

            fo.write(f'        migration.{operation_class.__name__}({conf}),\n')

        fo.write('    )\n')


def _get_model_structure(model):
    columns = {}
    structure = {
        'name': model.Meta.table_name,
        'columns': columns,
    }

    for name, field in model.Meta.fields.items():
        default = field.default

        if isinstance(default, F):
            default = str(default)
        elif default is None:
            default = None
        else:
            default = str(default).replace("'", "\'")

        columns[name] = {
            'column_type': field.column_type,
            'nullable': field.nullable,
            'unique': field.unique,
            'default': default,
        }

    return structure


def _get_column_sql(column_name, column):
    column_sql = [
        f'"{column_name}"',
        column['column_type'],
    ]

    if column.get('primary'):
        column_sql.append('PRIMARY KEY')
    elif column.get('unique'):
        column_sql.append('UNIQUE')

    nullable = column.get('nullable')

    column_sql.append(
        'NULL' if nullable else 'NOT NULL'
    )

    if 'default' in column:
        default = column['default']
        if default is None:
            default = 'NULL'

        if not (not nullable and default == 'NULL'):
            column_sql.append(f'DEFAULT {default}')

    if 'check' in column:
        column_sql.append(column['check'])

    return ' '.join(column_sql)


def migrate(migrations_module_path, last_migration=None):
    migrations_list = []
    for file_name in os.listdir(migrations_module_path.replace('.', os.path.sep)):
        file_name, file_extension = os.path.splitext(file_name)
        if file_extension == '.py':
            migrations_list.append(file_name)

    migrations_list.sort()

    if last_migration:
        start = migrations_list.index(last_migration) + 1
    else:
        start = 0

    for i in range(start, len(migrations_list)):
        migration_item = migrations_list[i]

        migration = pydoc.locate(
            os.path.join(migrations_module_path, migration_item)
            .replace(os.path.sep, '.')
        )

        yield migration_item, migration.up()


def create_migrations(migrations_module_path, last_migration=None):

    if last_migration:
        migrations_list = []
        for file_name in os.listdir(migrations_module_path.replace('.', os.path.sep)):
            file_name, file_extension = os.path.splitext(file_name)
            if file_extension == '.py':
                migrations_list.append(file_name)

        migrations_list.sort()

        stop_iteration = False
        for migration_item in migrations_list:
            if stop_iteration:
                raise 'Apply migrations before create a new one'

            stop_iteration = migration_item == last_migration

            migration = pydoc.locate(
                os.path.join(migrations_module_path, migration_item)
                .replace(os.path.sep, '.')
            )

            _apply_migration_structure(migration.up())

    operations = []
    for model in ModelManager.models:
        structure = _get_model_structure(model)
        if structure['name'] in _structures:
            operations += _get_operations_diff(structure)
        else:
            operations.append((CreateTable, structure))

    if operations:
        _create_migration_file(migrations_module_path, last_migration, operations)

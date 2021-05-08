def timeout(stmt, timeout):
    iterator = iter(stmt)
    yield f'SET statement_timeout = {timeout};{next(iterator)};SET statement_timeout = DEFAULT;'
    yield from iterator

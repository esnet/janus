from functools import reduce
from operator import eq
from tinydb import Query, where


class QueryUser:
    def query_builder(self, user=None, group=None, qargs=dict()):
        qs = list()
        user = user.split(',') if user else None
        group = group.split(',') if group else None
        if user and group:
            qs.append(where('users').any(user) | where('groups').any(group))
        elif user:
            qs.append(where('users').any(user))
        elif group:
            qs.append(where('groups').any(group))
        for k,v in qargs.items():
            if v:
                qs.append(eq(where(k), v))
        if len(qs):
            return reduce(lambda a, b: a & b, qs)
        return None

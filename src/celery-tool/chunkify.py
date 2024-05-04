from functools import wraps

from django.db.models import Model


def chunked_task(next_task=None):
    def task(fun):
        @wraps(fun)
        def outer(self, queryset_filter_func=None, offset=0, chunk_size=50000,
                  total=None, *args, **kwargs):
            end = int(offset) + int(chunk_size)
            queryset = queryset_filter_func(*args, **kwargs).order_by("id")
            total = total if total else queryset.count()
            result = fun(self, queryset=queryset[offset:end], *args, **kwargs)
            if total > end:
                self.delay(offset=end, chunk_size=chunk_size, total=total, *args, **kwargs)
            elif next_task:
                next_task(*args, **kwargs)
            return result

        return outer

    return task


def queryset_builder(model: Model):
    def get_queryset(**kwargs):
        obj_id = kwargs.get("id", None)
        objects = model.objects.all()
        if obj_id:
            objects = objects.filter(id=obj_id)
        return objects

    return get_queryset


def queryset_wrapper(queryset_func):
    def task(fun):
        @wraps(fun)
        def outer(self, *args, **kwargs):
            return fun(self, queryset_filter_func=queryset_func, *args, **kwargs)

        return outer

    return task

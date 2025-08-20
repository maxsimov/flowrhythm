from .capacity import UtilizationCapacity

class _UtilizationDecorator:
    def __init__(self, func, cap=UtilizationCapacity()):
        self.capacity = cap
        self.func = func
        self.name = None
        self.obj = None
        # functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        if self.obj:
            margs = (self.obj,) + args
        else:
            margs = args

        return self.func(*margs, **kwargs)

    def __get__(self, obj, objtype):
        self.obj = obj
        return self

def job_capacity(cap):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func, cap)
        )
        return decorator

    return factory

def job_name(name):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func)
        )
        decorator.name = name
        return decorator

    return factory

def workers(min_workers, max_workers=5, initial_workers=1):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func)
        )
        decorator.capacity.min_workers = min_workers
        decorator.capacity.max_workers = max_workers
        decorator.capacity.initial_workers = initial_workers
        return decorator

    return factory
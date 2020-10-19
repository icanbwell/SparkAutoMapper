from functools import wraps
from typing import Any


def capture_arguments(func: Any) -> Any:
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.

    .. note:: Should only be used to wrap a method where first arg is `self`
    """
    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if len(args) > 0:
            raise TypeError(
                "Method %s forces keyword arguments." % func.__name__
            )
        self._input_kwargs = kwargs
        return func(self, **kwargs)

    return wrapper

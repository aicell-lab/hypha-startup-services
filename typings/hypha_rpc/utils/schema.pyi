from collections.abc import Callable
from typing import Any, TypeVar, overload

_F = TypeVar("_F", bound=Callable[..., Any])

@overload
def schema_function(__func: _F, /) -> _F: ...
@overload
def schema_function(
    arbitrary_types_allowed: bool = ..., /, **kwargs: Any
) -> Callable[[_F], _F]: ...
@overload
def schema_function(
    *, arbitrary_types_allowed: bool | None = None, **kwargs: Any
) -> Callable[[_F], _F]: ...

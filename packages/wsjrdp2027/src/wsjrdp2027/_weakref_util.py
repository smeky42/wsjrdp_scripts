from __future__ import annotations

import typing as _typing
import weakref as _weakref


_T = _typing.TypeVar("_T")


class WeakrefAttr(_typing.Generic[_T]):
    """A descriptor for weakref-backed attributes.

    The descriptor stores the underlying
    :class:`~weakref.ReferenceType` inside the class ``__dict__``.

    On get access to the weakref attribute the weakly referenced object is returned if still alive.
    Otherwaise a :obj:`RuntimeError` is raised.

    >>> class Ham:
    ...     def __init__(self, value):
    ...         self.value = value
    >>> class Foo:
    ...     a = WeakrefAttr[Ham]()
    >>> foo = Foo()
    >>> b = Ham([1, 2, 3])
    >>> foo.a = b
    >>> foo.a.value
    [1, 2, 3]
    >>> del b
    >>> foo.a
    Traceback (most recent call last):
      ...
    RuntimeError: Attribute 'a' is no longer alive.

    >>> foo = Foo()
    >>> foo.a
    Traceback (most recent call last):
      ...
    RuntimeError: Attribute 'a' was never set or deleted.

    >>> foo = Foo()
    >>> b = Ham([1, 2, 3])
    >>> del foo.a
    >>> foo.a = b
    >>> foo.a.value
    [1, 2, 3]
    >>> del foo.a
    >>> foo.a
    Traceback (most recent call last):
      ...
    RuntimeError: Attribute 'a' was never set or deleted.
    """

    _name: str = _typing.cast(str, None)  #: Name of the attribute

    def __init__(self, name: str | None = None) -> None:
        if name is not None:
            self._name = name

    def __set_name__(self, owner, name: str) -> None:
        self._name = name

    @_typing.overload
    def __get__(self, instance: None, owner) -> _typing.Self: ...

    @_typing.overload
    def __get__(self, instance: object, owner) -> _T: ...

    def __get__(self, instance, owner):
        if instance is None:
            return self
        ref = instance.__dict__.get(self._name, None)
        if ref is None:
            raise RuntimeError(f"Attribute {self._name!r} was never set or deleted.")
        obj = ref()
        if obj is None:
            raise RuntimeError(f"Attribute {self._name!r} is no longer alive.")
        return obj

    def __set__(self, instance, value: _T) -> None:
        instance.__dict__[self._name] = _weakref.ref(value)

    def __delete__(self, instance) -> None:
        instance.__dict__.pop(self._name, None)


class OptionalWeakrefAttr(_typing.Generic[_T]):
    """A descriptor for weakref-backed optional attributes.

    The descriptor stores the underlying
    :class:`~weakref.ReferenceType` inside the class ``__dict__``.

    On get access to the weakref attribute either the original object
    if the referent is still alive or `None`. It also returns `None`
    if the attribute was never set.

    >>> class Ham:
    ...     def __init__(self, value):
    ...         self.value = value
    >>> class Foo:
    ...     a = OptionalWeakrefAttr[Ham]()
    >>> foo = Foo()
    >>> b = Ham([1, 2, 3])
    >>> foo.a = b
    >>> foo.a.value
    [1, 2, 3]
    >>> del b
    >>> foo.a is None
    True

    On :meth:`__get__`

    """

    _name: str = _typing.cast(str, None)  #: Name of the attribute

    def __init__(self, name: str | None = None) -> None:
        if name is not None:
            self._name = name

    def __set_name__(self, owner, name: str) -> None:
        self._name = name

    @_typing.overload
    def __get__(self, instance: None, owner) -> _typing.Self: ...

    @_typing.overload
    def __get__(self, instance: object, owner) -> _T | None: ...

    def __get__(self, instance, owner):
        if instance is None:
            return self
        ref = instance.__dict__.get(self._name, None)
        if ref is not None:
            return ref()
        else:
            return None

    def __set__(self, instance, value: _T | None) -> None:
        if value is None:
            instance.__dict__.pop(self._name, None)
        else:
            instance.__dict__[self._name] = _weakref.ref(value)

    def __delete__(self, instance) -> None:
        instance.__dict__.pop(self._name, None)

import importlib

import pytest
import wsjrdp2027


@pytest.mark.parametrize("name", sorted(wsjrdp2027.__all__))
def test_all_name_is_importable(name: str) -> None:
    """Every name listed in ``wsjrdp2027.__all__`` can be imported.

    Names are either imported eagerly at module level or resolved lazily via
    ``wsjrdp2027.__getattr__`` and ``__ALIASES__`` -- either way ``getattr``
    must succeed.
    """
    obj = getattr(wsjrdp2027, name)
    assert obj is not None


@pytest.mark.parametrize("name", sorted(wsjrdp2027.__ALIASES__))
def test_alias_is_importable(name: str) -> None:
    """Every entry in ``wsjrdp2027.__ALIASES__`` can be imported.

    Accessing the name goes through ``wsjrdp2027.__getattr__``, which imports the
    aliased module and looks up the qualified name -- so this fails if an alias
    points at a missing module or attribute.
    """
    obj = getattr(wsjrdp2027, name)
    assert obj is not None


@pytest.mark.parametrize("name", sorted(wsjrdp2027.__ALIASES__))
def test_alias_target_is_identical_to_getattr(name: str) -> None:
    """Every ``__ALIASES__`` entry points at the very object ``getattr`` returns.

    ``getattr(wsjrdp2027, name)`` short-circuits ``__getattr__`` for names that
    are also imported eagerly, so this resolves the alias target explicitly and
    compares identity. It catches alias entries that point at the wrong module
    or attribute even when an eager import masks them.
    """
    mod_name, qualname = wsjrdp2027.__ALIASES__[name]
    mod = importlib.import_module(mod_name, package=wsjrdp2027.__name__)
    expected = getattr(mod, qualname) if qualname else mod
    assert getattr(wsjrdp2027, name) is expected

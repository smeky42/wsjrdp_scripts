import pytest
import wsjrdp2027


@pytest.mark.parametrize("name", sorted(wsjrdp2027.__ALIASES__))
def test_alias_is_importable(name: str) -> None:
    """Every entry in ``wsjrdp2027.__ALIASES__`` can be imported.

    Accessing the name goes through ``wsjrdp2027.__getattr__``, which imports the
    aliased module and looks up the qualified name -- so this fails if an alias
    points at a missing module or attribute.
    """
    obj = getattr(wsjrdp2027, name)
    assert obj is not None

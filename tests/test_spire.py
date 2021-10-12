import spire
from six import string_types


def test_spire():
    version = spire.__version__
    assert isinstance(version, string_types)
    assert version

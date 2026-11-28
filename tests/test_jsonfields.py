"""`as_array` accepts a JSON array and rejects anything else, so Stats.from_json never iterates a scalar."""
import pytest

from citibike_pipeline.jsonfields import as_array


def test_an_array_is_returned():
    assert as_array(["a", 1]) == ["a", 1]


@pytest.mark.parametrize("value", [None, 3, "text", {"a": 1}])
def test_a_non_array_is_rejected(value):
    with pytest.raises(ValueError):
        as_array(value)

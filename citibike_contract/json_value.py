"""`JsonValue` and `JsonObject`: any value `json.loads` returns, and a JSON object of them."""
from __future__ import annotations

from typing import Mapping, Sequence, Union

JsonValue = Union[None, bool, int, float, str, Sequence["JsonValue"], Mapping[str, "JsonValue"]]

# A JSON object read off the wire or about to go on it: string keys, values still unparsed.
JsonObject = Mapping[str, JsonValue]

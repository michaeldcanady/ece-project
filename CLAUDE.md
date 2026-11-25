# CLAUDE.md

## Strong types

Every value you type in code you write or change gets its exact type, or a custom class you write for it.

- **The rule:** never use `Any`, and never use `dict` / `Dict` as a type, even for a value whose shape you don't know yet. Write the shape down as a class.
- **A record with named fields** (a parsed JSON object, a step record, a function's options) is a frozen `@dataclass`. Use a `TypedDict` only where the value must stay a real `dict`, for example when it is passed to `json.dumps` unchanged.
- **A value from outside** (Kafka bytes, an HTTP body, a capture line, a config file) is parsed into its class once, at the boundary, the way `citibike_contract.decode` parses an `Envelope`. Code past the boundary reads attributes, not string keys.
- **A closed set of strings** (event types, outcomes, reason codes) is a `Literal[...]` or an `Enum`, not a bare `str`.
- **Existing `Dict[str, Any]` code** (for example `Envelope.payload`) is not yet typed. When you change such a signature, give it a class; leave untouched code as it is.

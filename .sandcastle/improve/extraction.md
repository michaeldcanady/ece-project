Output your final answer now as a single `<output>` tag containing only JSON of this shape, and nothing else:

```
{"candidates": [{"title": "...", "strength": "strong" | "explore" | "speculative", "body": "markdown spec"}]}
```

At most 3 candidates. Use `{"candidates": []}` if nothing is at least `explore`. The `body` is the full markdown
spec from your work, as one JSON string (escape newlines and quotes).

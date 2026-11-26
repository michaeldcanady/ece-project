Output your final answer now as a single `<output>` tag containing only JSON of this shape, and nothing else:

```
{"summary": "string", "door": "one-way" | "two-way", "comments": [{"path": "string", "line": 1, "body": "string"}], "resolved": [{"threadId": "string", "reply": "string"}]}
```

`summary` is a short overall verdict. `door` is whether the head is a one-way or two-way door. Each `comments` entry is a finding on a line of the new version of a changed file. Each `resolved` entry names an open thread whose concern the new code addresses, with a one-sentence reply confirming how. Use `[]` for an empty list. Escape newlines and quotes.

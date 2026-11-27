import { test } from "node:test";
import assert from "node:assert/strict";
import { alreadyFiledList, filedBody, filesOf, speccedBody } from "./candidate-issue";

const spec = "## Problem\n\nIt hurts.\n\n## Files\n\n- `a.ts`\n- `b.ts`\n\n## Proposal\n\nFix it.";
const note = "Old idea.\n\n## Files\n\n- `old.ts`\n\n---\nmore";

test("filedBody pins the strong footer", () => {
  assert.equal(
    filedBody("SPEC", "strong"),
    "SPEC\n\n---\nFiled by the nightly improve run. This candidate is `strong`, so it is labeled `agent:implement` automatically.\n",
  );
});

test("filedBody pins the explore footer", () => {
  assert.equal(
    filedBody("SPEC", "explore"),
    "SPEC\n\n---\nFiled by the nightly improve run. Add the `agent:implement` label to have it implemented.\n",
  );
});

test("speccedBody pins the footer and the Original note block", () => {
  assert.equal(
    speccedBody("SPEC", "explore", "NOTE"),
    "SPEC\n\n---\nSpecced by the `agent:spec` workflow as `strength:explore`. Add the `agent:implement` label to have it implemented.\n\n<details><summary>Original note</summary>\n\nNOTE\n\n</details>\n",
  );
});

test("filesOf round-trips through both writers, even with a hostile note", () => {
  const files = "- `a.ts`\n- `b.ts`";
  assert.equal(filesOf(filedBody(spec, "strong")), files);
  assert.equal(filesOf(speccedBody(spec, "explore", note)), files);
});

test("filesOf reads a migrated note's **Files:** line", () => {
  assert.equal(filesOf("Idea.\n\n**Files:** `x.py`, `y.py`\n\nMore."), "`x.py`, `y.py`");
});

test("filesOf is empty when neither form is present", () => {
  assert.equal(filesOf("## Problem\n\nNothing here."), "");
});

test("filesOf cuts at 600 characters", () => {
  assert.equal(filesOf(`## Files\n\n${"x".repeat(700)}\n`).length, 600);
});

test("alreadyFiledList is (none yet) for no issues", () => {
  assert.equal(alreadyFiledList([]), "(none yet)");
});

test("alreadyFiledList lists issues in order with state, collapsing whitespace", () => {
  const out = alreadyFiledList([
    { number: 2, title: "Two", state: "OPEN", body: "## Files\n\n- a\n-   b\n" },
    { number: 1, title: "One", state: "CLOSED", body: "## Problem\n\nno files" },
  ]);
  assert.equal(out, "- #2 [OPEN] Two\n  Files: - a - b\n- #1 [CLOSED] One\n  Files: (not stated)");
});

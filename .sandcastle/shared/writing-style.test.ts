import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";

const root = path.join(import.meta.dirname, "..");
const files = (suffix: string): string[] =>
  fs
    .readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name !== "node_modules")
    .flatMap((dir) => fs.readdirSync(path.join(root, dir.name)).map((name) => path.join(root, dir.name, name)))
    .filter((file) => file.endsWith(suffix));

test("every prompt file receives the writing style", () => {
  const prompts = files("prompt.md");
  assert.ok(prompts.length >= 5);
  for (const prompt of prompts) {
    assert.match(fs.readFileSync(prompt, "utf8"), /\{\{WRITING_STYLE\}\}/, prompt);
  }
});

test("every script that loads a prompt passes WRITING_STYLE", () => {
  const scripts = files(".ts").filter((file) => /promptFile:/.test(fs.readFileSync(file, "utf8")));
  assert.ok(scripts.length >= 4);
  for (const script of scripts) {
    const source = fs.readFileSync(script, "utf8");
    const loads = (source.match(/promptFile: here\(/g) ?? []).length;
    const passes = (source.match(/WRITING_STYLE: writingStyle\(\)/g) ?? []).length;
    assert.equal(passes, loads, script);
  }
});

# TASK

Review the work on branch `{{BRANCH}}` for issue #{{ISSUE_NUMBER}}, then fix the real problems. You are unattended.

# ISSUE (the spec)

{{ISSUE_CONTEXT}}

# METHOD

1. Read the diff: `git diff origin/main...HEAD`.
2. Review along two axes, and report each separately:
   - **Standards:** `CODING_STANDARDS.md` and `.claude/rules/*` (strong types, parse at the boundary, one type per
     file, layered dependencies, refactors apart from behavior changes). The vocabulary is `GLOSSARY.md`'s.
   - **Spec:** every acceptance criterion in the issue is met, nothing outside "Scope" was changed, and behavior is
     preserved where the issue says so.
3. Fix real problems with new commits on `{{BRANCH}}`. Leave style nits and speculative ideas alone, and note them.
4. Run `mise run check` and keep it green. The DAG tests may skip when the Airflow container is absent.

Do not push, open or edit PRs, or touch issue labels.

# NOTES

Write a short markdown summary to `{{NOTES_FILE}}`: what you fixed, and what you noticed but left for the human
reviewer, under the two headings **Standards** and **Spec**. It becomes part of the PR description.

Output `<promise>COMPLETE</promise>` when done. Your final message is not read, so output only that.

# WRITING STYLE

{{WRITING_STYLE}}

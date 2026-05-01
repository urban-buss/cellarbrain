---
description: "Research a feature or bug and produce a structured analysis document in analysis/. Use when: 'analyze', 'research feature', 'investigate bug', 'design document', 'write analysis'."
argument-hint: "Describe the feature or bug to research"
agent: agent
model: "Claude Opus 4.6 (copilot)"
---
Research the topic described below thoroughly. Search the codebase, read relevant
source files, docs, and tests to understand the current state. Then produce a
structured analysis document.

If the topic is ambiguous or key design decisions could go multiple ways, ask
clarifying questions before writing. Provide 2–4 concrete options per question
so the user can pick quickly rather than explain from scratch.

## Output rules

1. **Numbering** — Look at `analysis/` for the next available number prefix
   (format: `NN-slug.md`, e.g. `09-feature-name.md`).
2. **Structure** — Follow the pattern of existing analysis docs:
   - `# NN — Title`
   - Problem Statement / Motivation
   - Current behaviour & root-cause analysis (with code references)
   - Detailed findings (tables, diagrams, code snippets where useful)
   - Impact assessment
   - Proposed solution(s) with trade-offs
   - Summary of findings (severity table)
   - Recommendations (numbered, actionable)
3. **Sub-documents** — If the topic has distinct sub-areas that deserve
   deeper exploration, create follow-up docs as `NNa-subtopic.md`,
   `NNb-subtopic.md`, etc. Keep the main doc self-contained; sub-docs go
   deeper on specific aspects.
4. **Evidence over opinion** — cite actual code paths, schemas, config keys,
   and test names. Show snippets for non-obvious behaviour.
5. **Effort estimates** — tag each recommendation as Trivial / Small /
   Medium / Large.

## Topic to research

{{input}}

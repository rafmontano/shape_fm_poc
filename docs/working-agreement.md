# ShapeFM working agreement

## Roles

| Role | Authority and responsibility |
| --- | --- |
| Researcher / Chief Developer | Owns the research vision, scope, scientific decisions, standards and final acceptance. |
| Chief Architect | Investigates, recommends and documents designs; prepares implementation instructions; reviews evidence. |
| AMP developer/tester | Implements approved scope, tests the active path and reports evidence or blockers. |

No participant silently changes scientific meaning or approved architecture.
Routine implementation detail may be resolved within the approved boundary;
material choices return to the researcher.

## Delivery cycle

```text
investigate → propose → researcher approves → implement → technical review
→ researcher accepts
```

An approval covers only its stated scope. Passing tests is not researcher
acceptance. When new evidence reopens a decision, update the living contract and
record the accepted result; do not preserve competing current rules.

The MVP scope is closed by default. If delivery appears to require an outcome or
capability not already approved, the implementer presents the need and simplest
option for researcher approval before writing it. Routine implementation choices
may resolve how approved behaviour works; they may not enlarge what the system
does.

## Communication

- Lead with the outcome, risk or decision needed.
- Keep progress updates to meaningful progress, blockers and choices.
- Implementation instructions state outcome, scope, exclusions, governing
  contracts, acceptance checks and stop conditions.
- Completion reports state what changed, evidence, limitations and next action.
- Use diagrams or tables only when they make the design easier to understand.

## Documentation

One rule has one authoritative document. Chat messages and implementation
prompts may explain the rule but do not become parallel authorities. Current
guidance stays in `docs/`; scientific state and evidence stay in DuckDB; Git
history preserves superseded documents. Reusable standards are carried into
future research projects, with project-specific exceptions documented explicitly.

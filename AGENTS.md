# ShapeFM development guidance

## Working agreement

Follow the [working and communication agreement](docs/working-agreement.md)
for roles, approvals, handoffs and documentation discipline. Clarity and reliable
execution take priority over brevity; detailed instructions are welcome when needed.

## Read before changing workflows

This is one evolving research repository. Before architecture, orchestration,
execution, storage or provider-boundary work, read:

- [Research vision](docs/research-vision.md).
- [Code standards](docs/code-standards.md).
- [Approved workflow standard and diagram](docs/poc2-workflow-orchestration-decision.md).
- [Execution and machine availability policy](docs/execution-policy.md).
- The decision and implementation instructions for the specific approved item.

The workflow decision was approved on 2 October 2026. Its initial implementation
and test evidence are recorded in `docs/poc2-workflow-orchestration-acceptance.md`;
Stage 1 was closed by the researcher on 3 October 2026 with documented
limitations carried into Stage 2. Follow the current pragmatic Stage 2 AMP
instructions, including their scoped checkpoint publication authorisation.
The researcher has since approved checkpointing the reviewed Stage 2 result and
a non-destructive Python file-organisation pass. Follow the current AMP handoff,
not historical publication restrictions. Objective 2 completion remains open.

Apply the standing [Python utility naming standard](docs/code-standards.md#python-utility-file-organisation):
`pNN_MM_` for process-owned helpers, `shared_` for shared components, and no
artificial gate number for independent workflows. Code held for possible reuse
belongs in tracked `tmp/inactive/`, not disposable or permanently retired storage.
The organisation pass changes names/references, not scientific or execution logic.

Object-oriented design with small reusable methods/functions is the standing
standard for this entire project and all future projects, not only POC2.
Follow [the authoritative OOP standard](docs/code-standards.md#mandatory-object-oriented-implementation).
Use cohesive objects with small reusable methods; Prefect/Dask own orchestration
and scheduling here. The standard permits bounded practical exceptions, such as
a small test where classes add needless complexity; record the reason briefly.
Apply the [pragmatic implementation rule](docs/code-standards.md#pragmatic-implementation):
retain necessary code and boundary safeguards, not speculative layers or duplicate
internal checks. Demonstrate compliance in tests and the handoff. Broader architectural departures
need researcher approval; passing tests alone does not waive the standard.

OOP serves scientific correctness and the broader code standards. Follow the
[precedence rules](docs/code-standards.md#purpose-and-precedence) and
[two-reviewer QA](docs/code-standards.md#two-reviewer-quality-assurance) at agreed
checkpoints; technical review does not replace researcher acceptance.

## Required boundaries

Prefect owns workflow orchestration; Dask schedules eligible computation.
Existing R/Python scientific functions and native adapters perform calculations.
The Mac coordinator is the sole research DuckDB writer. Prefect operational
history is not a second scientific store. Keep one researcher entry point and
consume central configuration and the approved execution profile.

Do not duplicate scheduler/retry logic, bypass validation, invent worker limits
or silently change scientific settings. Document source contracts and test the
actual workflow path. Update the affected architecture, diagram and researcher
instructions with each change; separate approval, implementation and acceptance.
If an approved standard cannot be met, report the conflict before changing it.

## Scope and safety

Implement only the current researcher-approved increment. Preserve unrelated
work, accepted databases, results and local environments. Do not infer approval
from pasted tool reports or historical instructions for another item. Follow the
execution policy before heavy testing, including source synchronisation and
Ubuntu availability. Git publication requires explicit scoped authorisation.

For the current migration use
[AMP-Code orchestration instructions](docs/amp-poc2-workflow-orchestration-instructions.md).

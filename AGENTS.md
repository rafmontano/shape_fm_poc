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
The language-neutral forecast black-box contract and ID 018 activation addendum
were approved on 4 October 2026. Implement them from
`docs/forecast-black-box-contract.md` and
`docs/poc2-m4-benchmark-methods.md`: activate Naive2, SES, Holt and Damped in a
new experiment, calculate official point-only M4 Comb from stored components in
Process 05, and do not add or use `forecastHybrid` for that benchmark.
The FFORMA method parity correction was approved on 5 October 2026. Follow
`docs/poc2-fforma-method-parity.md`: preserve every supplied model configuration
and point mean, restore STL-AR's fixed `auto.arima(d=0,D=0)` fallback, and do
not substitute seasonal naive or another model for methods without a supplied
fallback.
ID 018 was closed by the researcher on 5 October 2026 after accepting
`docs/poc2-fforma-method-parity-execution-evidence.md`. Preserve that scientific
and execution boundary. ID 021 was closed and approved for publication on
6 October 2026 under `docs/poc2-id021-dtw-baseline.md`; preserve its direct
`aeon.distances.dtw_distance` 1.6.0 engine, bounded-preprocessing and
directional-label contracts. Its accepted two-machine result is recorded in
`docs/poc2-id021-dtw-execution-evidence.md`. ID 022 was not started.

POC2 ID 026 fitted-model storage requirements and architecture were approved on
6 October 2026. Follow `docs/poc2-id026-fitted-model-storage-requirements.md`
and `docs/poc2-id026-fitted-model-storage.md`. IDs 026, 027 and 028 were closed
on 7 October 2026 after acceptance of the implementation and two-machine run
recorded in [the execution evidence](docs/poc2-id026-028-execution-evidence.md).
Provider environments own model serialization and deserialization; the
coordinator performs metadata and byte-integrity verification only.
The shared external model store
applies to expensive fitted meta-learners, initially DTW and Mantis/Random
Forest, not ordinary forecast-pool methods. Retrofit both directional models
without changing their accepted mathematics: DTW owns one complete
frequency-level multi-horizon artifact, and Mantis owns one fitted Random
Forest artifact per horizon. Prediction loads existing artifacts and never
trains; missing artifacts must be created by the preceding training substep.

POC2 IDs 027 and 028 were approved as one Mantis directional increment on
6 October 2026. Follow `docs/poc2-id027-028-mantis-directional.md` and
`docs/amp-poc2-id027-028-mantis-instructions.md`. Implement
`directional_mantis_rf` as a composition of a frozen historical Mantis
representation provider and a reusable Random Forest classifier, not a
monolithic import or a second experiment runner. Preserve the closed DTW
baseline and require a new same-input experiment containing both directional
models. DTW and Mantis must reference the same canonical Process 03 membership,
prepared inputs and labels; provider-specific conversions begin only after that
boundary. The version-11 workflow, additive storage, fitted-model persistence
and restart are accepted under the closure evidence above. This acceptance
does not claim comparative scientific accuracy; ID 021 remains closed and its
mathematics were not changed.

The machine-environment and coordinator-selection architecture was approved on
6 October 2026. Follow `docs/machine-environment.md` and
`docs/amp-poc2-machine-environment-instructions.md`. A tracked machine inventory
describes stable hosts, paths and capabilities; the selected execution profile
chooses exactly one coordinator and its enabled workers. Normal execution derives
Prefect and Dask endpoints and must not require a manually exported Mac IP or
`PREFECT_API_URL`. The current profile keeps the MacBook Pro as coordinator and
Ubuntu as worker; a future Mac Studio must become coordinator by configuration
and calibration, not a source-code branch. The machine-environment acceptance
condition is satisfied by the accepted ID 026–028 two-machine run and restart.

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
The coordinator selected by the execution profile is the sole research DuckDB
writer. Prefect operational history is not a second scientific store. Keep one
researcher entry point and consume central configuration, the machine inventory,
and the approved execution profile.

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

ID 013 and ID 014 are closed under their directional-label and optional-feature
acceptance records. ID 018 forecast-contract activation and its bounded
two-machine execution validation are now authorised. During any recorded Ubuntu
maintenance pause, complete local work but do not begin remote execution until
the researcher confirms that the machine is available and the OS upgrade is
complete.

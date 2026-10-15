# ShapeFM documentation

This directory contains only living project documentation. Git history is the
archive for superseded decisions, implementation prompts, acceptance transcripts,
calibration outputs and earlier diagrams.

## Start here

| Need | Document |
| --- | --- |
| Research purpose and roadmap | [Research vision](research-vision.md) |
| System structure and ownership | [Architecture](architecture.md) |
| Coding and workflow rules | [Code standards](code-standards.md) |
| Roles, approvals and hand-offs | [Working agreement](working-agreement.md) |
| Current POC2 scope and status | [POC2](poc2.md) |
| Data, windows and transformations | [Data contract](data-contract.md) |
| Experiment configuration, storage and restart | [Experiment configuration](experiment-configuration.md) |
| Forecast model contract and pool | [Forecast contract](forecast-black-box-contract.md) |
| DTW, Mantis and fitted model storage | [Directional models](directional-models.md) |
| Metrics, sensitivity, tables and figures | [Evaluation](evaluation.md) |
| Installation, machines and execution | [Environment and execution](environment.md) |

## Documentation rule

Each rule has one owner. Update that document when the rule changes. Do not add
a second document for an implementation request or completion report. Durable
scientific and execution evidence belongs in DuckDB; human-readable tables and
figures belong under `results/`; temporary logs stay untracked. A decision that
is no longer current is removed from this directory and remains available in
Git history.

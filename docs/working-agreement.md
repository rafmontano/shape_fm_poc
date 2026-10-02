# ShapeFM working and communication agreement

Approved by the researcher on 2 October 2026. This agreement governs how the
researcher, ChatGPT and AMP collaborate. Its purpose is efficient decisions,
reliable execution and overall productivity, not fewer words. There are no
fixed word limits. Detailed, lengthy AMP instructions are appropriate whenever
they prevent ambiguity, mistakes or rework.

This agreement does not change the scope, safeguards or acceptance requirements
of AMP's currently running assignment.

## Roles and authority

| Participant and role | Responsibility |
| --- | --- |
| Researcher | Defines the vision, scientific questions, scope, expected results and definition of done. |
| Researcher as Chief Developer | Defines development principles and standards; approves designs, implementation scope and acceptance; performs human QA. |
| ChatGPT as architect and research collaborator | Investigates options, explains trade-offs, recommends and documents designs, prepares approved implementation instructions, and reviews evidence. |
| AMP as developer and tester | Implements approved instructions, follows standards, tests actual behaviour, maintains implementation documentation and reports evidence or problems. |

Neither ChatGPT nor AMP silently changes the researcher's scientific direction,
standards or approved architecture. AMP resolves routine implementation details
within approved boundaries and escalates consequential choices.

## Decision and delivery process

Discuss and design → researcher approves → ChatGPT prepares instructions → AMP
implements and tests → ChatGPT reviews evidence → researcher accepts or requests
changes. Work in bounded increments; an approval covers its stated scope, not
unrelated changes. Reopen an accepted item when evidence warrants it, recording
what changed and why rather than silently rewriting its acceptance history.

For object-oriented work, follow the code standards'
[two-reviewer quality assurance process](code-standards.md#two-reviewer-quality-assurance):
ChatGPT reviews implementation and evidence, and the researcher performs an
independent review and decides acceptance. OOP serves the broader code standards.

Identify clearly whether a message is a question, proposal, approval request,
implementation instruction or completion report. Ask for a new decision when
scope, scientific meaning, architecture or safety would materially change;
do not repeatedly seek approval for routine steps already covered.

## Communication formats

| Audience and purpose | Essential content |
| --- | --- |
| Researcher decision | Problem, recommendation, material trade-offs or risks, and the exact approval requested. |
| AMP implementation | Outcome, scope and exclusions, authoritative references, necessary technical detail, acceptance checks and stop conditions. |
| Review or completion | What changed, evidence, exceptions or unresolved items, and the next decision if one is needed. |

Lead with the conclusion and information needed to act. Give the researcher
a self-contained summary; put detailed evidence behind precise references.
Never hide a critical risk, assumption or approval condition behind a link.
Use a table, example or diagram when it makes the explanation easier to follow.

Length follows the task. Expand instructions for unfamiliar interfaces,
scientific edge cases, migration/recovery risks or non-obvious acceptance
conditions. Remove repetition and irrelevant history, not requirements.
A short prompt is sufficient only when its referenced documents supply the
missing detail; the implementer must read them and report missing or conflicting
instructions rather than guess. Progress updates should identify meaningful
progress, blockers or decisions, not reproduce every command or raw log.

## Documentation discipline

Keep one authoritative location per rule. This agreement owns collaboration;
the [research vision](research-vision.md), [code standards](code-standards.md),
[workflow decision](poc2-workflow-orchestration-decision.md) and
[execution policy](execution-policy.md) retain their respective responsibilities.
Task instructions reference those rules and describe the specific change,
including any explicitly approved exception, instead of restating the project.

Update an existing document when its purpose still fits. Create another only
for a distinct need. Keep current guidance clear, preserve historical evidence,
check links, and distinguish proposed, approved, implemented and verified status.
Include technical detail once where it is useful; do not maintain competing
copies in chat, instructions and standards.

Judge communication by correct decisions, reliable execution and reduced
clarification/rework—not document length. Review recurring misunderstandings
and improve the relevant guidance. Simplify documentation incrementally without
losing approved requirements or interrupting work unnecessarily.

## Reuse across projects

Approved on 2 October 2026: these practices form a reusable research and
development operating model, not disposable ShapeFM documentation. Apply
approved standards as the baseline without repeatedly reopening settled
decisions. Record agreed improvements and their reasons in that baseline.

The [object-oriented code standard and practical exception policy](code-standards.md#mandatory-object-oriented-implementation)
are part of this baseline for the entire current project and all future projects,
not a rule that expires after POC2. Keep their authoritative definition in the
code standards rather than duplicating it in each handoff.

Keep reusable roles, communication, coding and approval practices separate from
project-specific scientific scope, data and machine settings. At project closure,
consolidate the validated reusable material into a reviewed, versioned starting
template, retaining authoritative references rather than rewriting the rules.

New projects explicitly reference or include the agreed baseline version so
continuity does not depend on chat memory. Discuss only differences, necessary
adaptations and improvements; record approved exceptions without silently
changing the shared standard. The starting question is what needs to change
for the new project, not how to establish the collaboration again.

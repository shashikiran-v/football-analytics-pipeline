# ADR-0000: Using Architecture Decision Records

## Status

Accepted — 2026-05-31

## Context

This pipeline involves several decisions where the "obvious" choice differs
from the *defensible* choice (e.g. Pandas vs Spark, SQLite vs Postgres for
metadata, two-table audit vs single-row audit). Code shows *what* we did;
it doesn't show *why* we picked that over the alternatives, or what we'd
need to know to revisit the decision.

Without a written record, future readers — including future-us — have to
re-derive the reasoning every time something looks odd. That's a waste,
and worse, it leads to silent reversion when someone "simplifies" away a
constraint they didn't realise existed.

## Decision

We will record every significant architectural decision as a short
Markdown file under `docs/adr/`, numbered sequentially, following the
format proposed by Michael Nygard (2011).

Each ADR captures:

- **Status** — proposed, accepted, deprecated, or superseded by ADR-NNNN
- **Context** — the problem and constraints
- **Decision** — the choice made, stated plainly
- **Consequences** — what we gain and what we give up
- **Alternatives considered** — what we rejected, and why

ADRs are append-only: once accepted, an ADR is not edited. If we change
our minds, we write a new ADR that supersedes the old one and update the
old one's Status field to reflect that.

## Consequences

**Gained:**

- Future readers can audit the reasoning chain without spelunking through
  PR history.
- New joiners can read `docs/adr/` and understand the why of the codebase
  in 20 minutes, not 20 days.
- Decisions become reviewable artefacts in their own right. Bad
  reasoning is easier to challenge when it's written down.

**Given up:**

- Discipline cost. Every significant decision adds a 5–10 minute writing
  cost. We accept this; the cost of *not* documenting compounds.

## Alternatives considered

- **Wiki / Confluence / Notion.** Off-platform docs drift away from the
  code. Keeping ADRs in-repo guarantees they version alongside the code
  they describe.
- **Long-form README sections.** Doesn't scale; a single README becomes
  unreadable past a handful of decisions, and it's hard to link to a
  specific reasoning step.
- **Issue tracker history.** Too low-signal — issues capture work, not
  reasoning. The signal-to-noise ratio is wrong.

## Index of ADRs

| Number | Title                              | Status   |
| ------ | ---------------------------------- | -------- |
| 0000   | Using Architecture Decision Records | Accepted |
| 0001   | Audit Table Design                  | Accepted |
| 0002   | Source Registry as a Framework      | Accepted |
| 0003   | Bronze Storage and Partitioning     | Accepted |
| 0004   | Silver Transformation Strategy      | Accepted |
| 0005   | SCD Type 2 Implementation           | Accepted |
| 0006   | Data Quality Framework Design       | Accepted |
| 0007   | Gold Layer Storage and Analytics    | Accepted |
| 0008   | Cross-Batch Semantics               | Accepted |

Future phases will add an ADR covering Spark engine scope.

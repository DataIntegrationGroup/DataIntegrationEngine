# Executive Summary: Why Orchestrated DIE Beats CLI DIE

The Data Integration Engine began as a pip-installable CLI (`die weave`): each
user ran their own integration on their own machine and got a folder of CSVs.
It now runs as a scheduled Dagster asset graph that publishes standardized,
versioned data products to GCS and GeoServer. This document summarizes why the
orchestrated model is the better architecture.

## 1. Run once, use many

Under the CLI model every consumer paid the full cost of integration — every
analyst, every report, every semester of students hammering nine agency APIs
for the same statewide dataset, waiting out the same rate limits, and burning
the same compute. Orchestrated DIE runs each integration **once**, on a
schedule, and every consumer reads the same published product. The provider
APIs see one polite client instead of N ad-hoc ones; USGS rate limits and API
keys are managed in one place instead of in every user's shell history.

## 2. Data stays current — automatically

A CLI export is stale the moment it lands on disk, and nothing tells its owner
when to re-run it. Analyses quietly drift apart as each is built on a snapshot
of a different age. Orchestrated products refresh on their own cadence (daily
for water levels, monthly for slow-moving products), with change detection that
skips republishing unchanged data. "Which version of the data is this?" has one
answer: the dated, versioned product in GCS.

## 3. Fixes roll out to everyone, immediately

This is the decisive argument, and it is not hypothetical. This branch fixed a
silent datum bug: NAD27 coordinates were never actually shifted to WGS84,
leaving affected wells ~50–100 m off in every export that ever included them.

- **CLI world:** the fix ships in a new pip release. Every user must notice the
  release, upgrade, re-run their integration, and re-derive anything built on
  the bad coordinates. Old CSVs with wrong positions live on in shared drives,
  papers, and dashboards forever. Most users never learn they were affected.
- **Orchestrated world:** the fix merges, the next scheduled run republishes
  every product, and every consumer — dashboards, web maps, OGC API clients —
  is corrected on the next read. Zero user action.

Data integrity in the CLI model is each user's problem; in the orchestrated
model it is the team's problem, solved once.

## 4. Observability instead of silence

A CLI run that half-fails writes a partial folder and exits; nobody else knows.
The orchestrated graph makes every source an observable asset: a dead agency
API surfaces as a red asset check in the UI, soft-failing that source without
blocking the rest of the graph. Run history, per-source record counts, and
"days since data last changed" metadata exist for every product. The team can
see integration health at a glance — the CLI had no equivalent at all.

## 5. Consistency and correctness by construction

CLI output depended on user-chosen flags: which sources were excluded, which
output mode, which bounding box. Two users could produce different "statewide
waterlevels" datasets and not know why they disagree. Orchestrated products are
declared once in `products.yaml` — same sources, same filters, same transforms
— so a product name means exactly one thing. Shared source assets also
deduplicate work *across* products (one fetch serves both a summary and a
timeseries product), something no CLI user could coordinate.

## 6. A dramatically simpler, safer codebase (my view)

Supporting the CLI meant carrying an entire second architecture: output-format
plumbing (CSV/GeoJSON byte assembly, directory-naming logic, cloud-upload
strategies), an ad-hoc web API + Cloud Tasks worker, and a Config object that
mixed query inputs with file-output concerns. Removing it cut ~2,600 lines and
left each layer with one job: Config is a query spec, the unifier
fetches-and-transforms, persistence is GeoPandas, Dagster owns scheduling and
delivery. Less surface means fewer places for bugs to hide — the NAD27 bug was
found *because* consolidation made the transform layer small enough to test
directly.

Two further points I'd add:

- **Products, not exports.** The orchestrated model changed what DIE produces:
  not "a folder of CSVs for whoever ran the command," but named, documented,
  spatially-served products (trends, MCL exceedance, water type, well density)
  with collection-level method descriptions. That is an institutional data
  service, not a personal tool — citable, linkable, and usable by people who
  will never install Python.
- **Institutional knowledge lives in code review, not tribal memory.** Every
  product's definition, schedule, and provenance is in one reviewed repository.
  When a source API changes (as USGS's did), one fix in one place restores
  every product — instead of a support cycle teaching each user to upgrade.

## The honest trade-offs

- **Infrastructure dependency.** The orchestrated model needs Dagster+, GCS,
  and GeoServer running; the CLI needed only a laptop. Mitigation: the backend
  remains an importable library (`unify_source_both` etc.), so ad-hoc
  programmatic pulls are still possible for power users.
- **Custom scopes.** A user who wanted a bespoke bbox/date-range export could
  self-serve with the CLI. Now that's either a products.yaml addition (fast,
  reviewed) or a library call. In practice, published statewide products cover
  the dominant use cases, and spatial filtering belongs downstream (OGC API /
  GIS clients) anyway.

Both trade-offs are real but small against the alternative: N users
independently re-running, re-paying, and re-discovering the same integration —
each with their own silent copy of every bug.

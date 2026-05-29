# MontelEQ Ingestion Pipeline

A dispatcher-driven Databricks pipeline that ingests EnergyQuantified (EQ)
curve data, following the
[Meteologica](https://github.com/Platob/Meteologica) ingestion pattern.
Deployed as a Databricks Asset Bundle.

## Architecture

A single scheduled job, `monteleq_dispatcher`, runs one self-contained
`dispatcher` notebook that fans work out to per-category worker jobs:

- **Phase 1 — scan & queue** (`dispatcher`): refreshes the curve-metadata
  referential (`curated_curve_metadata`), then either
  - *scheduled* (`seconds <= 3600`): streams EQ curve-update events and queues
    the updated curves into `pending_requests`, bucketed by `table_category`; or
  - *backfill* (`seconds > 3600`): selects every known category to ingest over
    the full window (no queue).
- **Phase 2 — cluster**: for each discovered category, get-or-create a dedicated
  all-purpose cluster via `client.databricks.clusters.get_or_create`, in
  parallel across categories using a thread pool.
- **Phase 3 — dispatch**: submit a one-off job run per category that runs the
  `ingest_category` worker on the category's dedicated cluster.

## Notebooks

- `notebooks/dispatcher.py` — orchestrator (refresh, scan, queue, cluster, dispatch).
- `notebooks/ingest_category.py` — per-category worker: fetches the category's
  curves from EQ, curates them into the shared
  `curated_<data_type>_<curve_type>_<categories>` table, then drains the pending
  rows it consumed.

## Data layers

- Curve referential: `<schema>.curated_curve_metadata` (one row per curve).
- Work queue: `<schema>.pending_requests` (one row per curve×window×variant).
- Curated curves: `<schema>.curated_<data_type>_<curve_type>_<categories>`.

## Idempotency

The dispatcher upserts the queue by `request_id`. Each worker reads the queue
for its category, ingests, and only then deletes the exact rows it consumed, so
a failed run replays cleanly by re-reading the same rows. Worker windows are
pinned (`end_date` + `seconds` forwarded from the dispatcher) so retries reuse
the identical time window.

## Deployment

One-time library sync of the package source into the workspace:

```
databricks workspace import-dir python/src /Workspace/Shared/MontelEQ/python/src
```

Deploy the bundle:

```
databricks bundle deploy -t prd --profile monteleq-prd
```

Trigger manually:

```
databricks bundle run -t prd monteleq_dispatcher --profile monteleq-prd
```

Backfill (6-month window, overwrite):

```
databricks bundle run -t prd monteleq_dispatcher \
    --params seconds=15552000 --params mode=overwrite --profile monteleq-prd
```

Restrict to specific categories:

```
databricks bundle run -t prd monteleq_dispatcher \
    --params table_category=actual_timeseries_power,forecast_instance_wind
```

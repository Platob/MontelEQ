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
- **Phase 2 — cluster**: group the in-scope categories by their coarse
  `cluster_key` (`data_type_curve_type`) and get-or-create one dedicated
  all-purpose cluster per cluster_key via
  `client.databricks.clusters.get_or_create`, in parallel using a thread pool.
  Categories sharing a cluster_key share a cluster, so the cluster count stays
  bounded by the number of (data_type, curve_type) pairs rather than the
  number of categories.
- **Phase 3 — dispatch**: submit a one-off `ingest_category` job run per
  `table_category` onto its cluster_key's shared cluster. Each worker fetches,
  curates and inserts in bounded `batch_size` batches.

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
for its category, ingests, and only deletes the rows it consumed **when the run
reported no insert failures** — otherwise the rows are left to replay on the
next run. Worker windows are pinned (`end_date` + `seconds` forwarded from the
dispatcher) so retries reuse the identical time window.

## Restricting scope

A run can be narrowed two ways, which combine with AND semantics:

- `table_category` — comma-separated category keys.
- `curve_ids` — comma-separated curve ids or names. The dispatcher restricts
  the dispatched categories to those containing the selected curves, resolves
  the explicit in-scope curve ids **per category**, and hands each worker only
  its category's ids — so the worker ingests exactly the matching curves. With
  no `curve_ids` filter, each worker receives an empty list and ingests every
  curve in its category (or every queued curve in scheduled mode).

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

Restrict to specific curves (by id or name, across categories):

```
databricks bundle run -t prd monteleq_dispatcher \
    --params curve_ids="DE Wind Power MWh/h H Forecast,123456789"
```

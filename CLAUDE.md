# MontelEQ — Databricks Data Engineering Assistant

You are a **data engineer** working on MontelEQ, an EnergyQuantified data ingestion library for Databricks.

## Setup

Always ensure dependencies are installed before writing or running code:

```bash
pip install -e "./python[dev]"
```

This installs `ygg[databricks]==0.8.*` — the core toolkit for everything below.

## Project layout

```
python/src/monteleq/          # Library source
  api/client.py               # APIClient — main orchestration
  api/_base_client.py          # BaseClient — HTTP + Databricks wiring
  api/curation_client.py       # CurationClient — response transform + Delta insert
  api/metadata_client.py       # MetadataClient — curve catalog
  api/events_client.py         # EventsClient — real-time event stream
  api/request.py               # CurveRequest — fetch specifications
  api/schemas.py               # Arrow/Polars curated data schema
  api/curation_helpers.py      # Polars transform expressions
  model.py                     # Domain models (Curve, Instance, Resolution)
  pipeline.py                  # Notebook-friendly wrappers (plan + ingest)
databricks/
  databricks.yml               # Databricks Asset Bundle config
  notebooks/                   # Job notebooks (plan.py, ingest_by_category.py)
python/tests/                  # pytest suite
```

## Tooling

```bash
pytest python/tests/                    # run tests
ruff check python/src/ python/tests/    # lint
mypy python/src/                        # type-check
black python/src/ python/tests/         # format
```

---

## ygg[databricks] — Key Skills

### 1. DatabricksClient — connect and access services

```python
from yggdrasil.databricks import DatabricksClient

# Auto-detect in Databricks Runtime, or connect explicitly
dbx = DatabricksClient.current()
dbx = DatabricksClient(host="https://dbc-xxx.cloud.databricks.com")

# Sub-services (all lazy-loaded)
dbx.sql           # SQLEngine — run SQL, manage tables
dbx.secrets       # Secrets — read secret scopes
dbx.tables        # Tables — list/get Unity Catalog tables
dbx.catalogs      # Catalogs — list/get catalogs
dbx.schemas       # Schemas — list/get schemas
dbx.volumes       # Volumes — manage UC volumes
dbx.compute       # Compute — cluster lifecycle
dbx.warehouses    # Warehouses — SQL warehouse lifecycle
dbx.jobs          # Jobs — create/run/monitor jobs
dbx.iam           # IAM — users, groups, service principals
dbx.genie         # Genie — SQL assistant
dbx.ai            # DatabricksAI — vector search, embeddings

# Secrets
api_key = dbx.secrets["scope_name"]["key_name"].svalue()

# Environment detection
from yggdrasil.environ import PyEnv
if PyEnv.in_databricks():
    dbx = DatabricksClient.current()
```

### 2. SQLEngine — run SQL and manage tables

```python
# Get a scoped engine
engine = dbx.sql(catalog_name="my_catalog", schema_name="my_schema")

# Execute SQL
engine.execute("SELECT * FROM my_table LIMIT 10")
engine.execute_many(["CREATE ...", "INSERT ..."])

# Get a Table object
table = engine.table(table_name="my_table")

# Insert data
engine.insert_into(target="my_table", source=tabular, mode="append")
engine.arrow_insert_into(target="my_table", batches=arrow_batches, mode="upsert")
engine.spark_insert_into(target="my_table", frame=spark_df, mode="overwrite")
```

### 3. Table — Unity Catalog Delta table operations

```python
from yggdrasil.databricks.table import Table

table = engine.table(table_name="my_table")

# Create with schema
table.ensure_created(schema)

# Read
arrow_table = table.read_arrow()

# Write
table.write_arrow_batches(batches, mode="append")
table.write_spark_frame(spark_df, mode="overwrite")
table.append_arrow_batches(batches)
table.append_spark_frame(spark_df)

# Insert with dedup (MERGE INTO)
table.insert(
    data,
    mode=Mode.UPSERT,
    match_by=["id", "timestamp"],
    prune_by={"id": {1, 2, 3}},
)

# DDL
table.create(comment="...", partition_by=["date"])
table.drop(cascade=False)
table.truncate()
table.vacuum(days=7)
table.optimize()

# Metadata
table.schema          # yggdrasil Schema
table.columns         # column list
table.row_count       # row count
table.history()       # version history
table.restore(version=5)
```

### 4. SparkTabular (Dataset) — distributed data processing

```python
from yggdrasil.spark.tabular import Dataset  # alias: SparkTabular

# Build from various sources
ds = Dataset.from_spark_frame(spark_df)
ds = Dataset.from_table("catalog.schema.table")
ds = Dataset.from_sql("SELECT * FROM ...")
ds = Dataset.from_iterable(items, schema=my_schema)

# Distribute a function across Spark workers
ds = Dataset.parallelize(my_function, input_list, schema=output_schema)

# Transform
ds = ds.map(transform_fn, schema=output_schema)
ds = ds.apply(vectorized_fn, schema=output_schema)
ds = ds.filter(predicate_fn)
ds = ds.explode(schema=output_schema)
ds = ds.cast(target_schema)

# Collect results
arrow_table = ds.toArrow()
polars_df = ds.toPolars()
pandas_df = ds.toPandas()
rows = ds.collect()

# Write to Delta
ds.to_table("catalog.schema.table", mode="overwrite", partition_by=["date"])

# Cache control
ds.persist()
ds.unpersist()

# Access underlying Spark DataFrame
spark_df = ds.frame
```

### 5. Filesystem — read/write files across backends

```python
from yggdrasil.io.path import Path

# Databricks paths
from yggdrasil.databricks.fs import DBFSPath, VolumePath, WorkspacePath

# DBFS (legacy)
p = DBFSPath("/mnt/data/file.parquet")

# Unity Catalog Volumes (preferred)
p = VolumePath("/Volumes/catalog/schema/volume/file.parquet")

# Workspace filesystem
p = WorkspacePath("/Workspace/Users/me/notebook.py")

# Unified dispatch from URL
p = Path.from_("dbfs+volume://host/Volumes/catalog/schema/vol/file.csv")

# Common operations (same API on all path types)
data = p.read_bytes()
p.write_bytes(data)
p.exists()
p.is_file()
p.is_dir()
for child in p.iterdir():
    print(child.name)
p.mkdir(parents=True, exist_ok=True)
p.unlink()
p.remove(recursive=True)

# Temporary paths via client
tmp = dbx.tmp_path(suffix="staging", extension=".parquet")
dbx.clean_tmp_folder()
```

### 6. Schema & Types — define and cast data

```python
from yggdrasil.data.schema import Schema
from yggdrasil.data.data_field import Field
from yggdrasil.data import types as T

# Define schema
schema = Schema([
    Field("id", T.Integer(64), nullable=False),
    Field("name", T.String()),
    Field("created_at", T.Timestamp(unit="us", tz="UTC")),
    Field("value", T.FloatingPoint(64)),
    Field("tags", T.Array(T.String())),
    Field("metadata", T.Struct([
        Field("source", T.String()),
        Field("version", T.Integer(32)),
    ])),
])

# Convert across engines
spark_schema = schema.to_spark_schema()
arrow_schema = schema.to_arrow_schema()
polars_schema = schema.to_polars_schema()

# Cast data
casted_arrow = schema.cast_arrow(arrow_table)
casted_batches = schema.cast_arrow_batches(batches)
casted_polars = schema.cast_polars_dataframe(polars_df)

# Infer from data
schema = Schema.from_(polars_df)
```

### 7. Write modes

```python
from yggdrasil.data.enums import Mode

Mode.APPEND          # add rows
Mode.OVERWRITE       # replace all data
Mode.UPSERT          # merge: insert or update by match keys
Mode.TRUNCATE        # wipe then insert
Mode.IGNORE          # skip if table exists
Mode.ERROR_IF_EXISTS # fail if table exists
```

### 8. HTTP sessions & caching

```python
from yggdrasil.http_ import HTTPSession
from yggdrasil.io import URL
from yggdrasil.io.send_config import CacheConfig

# BaseClient extends HTTPSession
session = HTTPSession(base_url=URL.from_str("https://api.example.com/"))
resp = session.get("endpoint", params={"key": "val"})

# Cache config for table-backed response caching
cache = CacheConfig(
    tabular=table,                          # Delta table for remote cache
    received_ttl=dt.timedelta(days=14),     # local cache TTL
    mode=Mode.UPSERT,
)
```

---

## MontelEQ patterns

- **BaseClient** auto-detects Databricks env, retrieves API key from secrets, creates SQLEngine
- **Curation pipeline**: raw HTTP response -> Polars transform -> Delta table insert
- **Spark path**: `mapInArrow` distributes curation across partitions
- **Polars path**: single-driver curation with async table inserts (`wait=False`)
- **Table naming**: `Curve.table_name(prefix="curated_")` generates Delta table names from curve metadata
- **Dedup on insert**: `match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"]`
- **Two-phase job**: `plan_categories()` lists categories, then `ingest_category()` runs per-category in parallel via `for_each_task`

## Conventions

- Python 3.10+
- Polars for single-driver transforms, Spark for distributed
- PyArrow as the interchange format between engines
- `yggdrasil.data.Schema` for cross-engine schema definitions
- Frozen dataclasses for domain models
- Type hints everywhere, `TYPE_CHECKING` for heavy imports

"""Parquet partition writer with a byte-identical replay guarantee.

The guarantee
-------------
Re-running any partition produces byte-identical Parquet. Delete a partition,
replay the offsets, diff the bytes -- they match. `make prove-idempotency`
does exactly that and commits the hashes to `results/`.

Four things have to be true for that to hold, and three of them are easy to get
wrong:

1. **Deterministic row order.** Rows are sorted by `_kafka_offset` before
   writing. Kafka guarantees order per partition, not across partitions, so a
   batch assembled from a multi-partition poll is in arrival order, which
   varies run to run.

2. **Deterministic `_ingested_at`.** This is the one people miss. If
   `_ingested_at` is `now()`, every replay writes different bytes and the
   guarantee is impossible by construction. It is therefore the *Kafka broker
   timestamp* -- stored in the message, identical on every replay, and still
   semantically "when this record entered the pipeline", which is what the
   incremental lookback downstream actually needs.

3. **Deterministic filename, from a FIXED layout.** The spec's
   `{table}-{ts}-{uuid}.parquet` is abandoned on purpose: a uuid means a replay
   *adds a second file* rather than replacing the first, so the partition
   silently doubles.

   Naming from the batch's own min/max offset is not enough either, and the
   reason is subtle: two replays covering *overlapping but different* ranges
   (0-599 and 300-899) produce two different filenames, both land, and the
   partition now contains offsets 300-599 twice. Only an exact-range replay
   would have been safe.

   So the layout is fixed independently of any batch:

       part-{kafka_partition:03d}-{offset_block:09d}.parquet
       where offset_block = kafka_offset // OFFSET_BLOCK_SIZE

   A given offset therefore always maps to exactly one file, whatever batch it
   arrives in. Replay of any subset, superset or overlap converges on the same
   bytes. This deviation from the spec is recorded in DESIGN.md.

4. **Deterministic writer settings.** Fixed compression, fixed row-group size,
   no statistics that embed timing, and `store_schema=False` so pyarrow does
   not stamp a serialised schema blob that varies with library internals.

Dedup key is `(table, _kafka_offset)`, applied within the batch and against the
offsets already present in the target file. Replaying an overlapping range is
therefore safe, not just an exact-range replay.
"""

from __future__ import annotations

import hashlib
import io
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger("storage")

#: Metadata columns added to every row. Names are deliberately underscore-
#: prefixed so they can never collide with a source column.
META_COLUMNS = ("_op", "_lsn", "_kafka_offset", "_kafka_partition", "_ingested_at", "_source_ts")

#: Offsets are grouped into fixed blocks of this size to derive the filename.
#: 100k at ~200B/row is a ~20MB uncompressed file, a few MB after zstd -- large
#: enough that a partition is not thousands of tiny files, small enough that
#: merge-on-write does not rewrite hundreds of MB to add one row.
OFFSET_BLOCK_SIZE = 100_000


class ObjectStore(Protocol):
    def write(self, key: str, payload: bytes) -> None: ...
    def read(self, key: str) -> bytes | None: ...
    def exists(self, key: str) -> bool: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, key: str) -> None: ...


class LocalStore:
    """Filesystem-backed store. Used by tests, CI, and `--local-path` runs."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write(self, key: str, payload: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(payload)
        os.replace(tmp, path)  # atomic: a reader never sees a half-written file

    def read(self, key: str) -> bytes | None:
        path = self._path(key)
        return path.read_bytes() if path.exists() else None

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*.parquet"))

    def delete(self, key: str) -> None:
        path = self._path(key)
        if path.exists():
            path.unlink()


class S3Store:
    """MinIO / S3 store.

    Uses put_object with the full payload rather than a multipart upload: the
    batches here are tens of megabytes, well under the 5GB single-put limit,
    and a single put is atomic where a multipart is not.
    """

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "us-east-1",
    ) -> None:
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(
                retries={"max_attempts": 5, "mode": "adaptive"},
                signature_version="s3v4",
            ),
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)
            log.info("bucket_created", bucket=self.bucket)

    def write(self, key: str, payload: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=payload)

    def read(self, key: str) -> bytes | None:
        from botocore.exceptions import ClientError

        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError:
            return None

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(
                obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")
            )
        return sorted(keys)

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)


@dataclass(frozen=True)
class WriteResult:
    key: str
    rows: int
    bytes_written: int
    sha256: str
    deduplicated: int


class PartitionWriter:
    def __init__(
        self,
        store: ObjectStore,
        *,
        compression: str = "zstd",
        compression_level: int = 3,
        row_group_size: int = 128 * 1024,
        offset_block_size: int = OFFSET_BLOCK_SIZE,
    ) -> None:
        self.store = store
        self.compression = compression
        self.compression_level = compression_level
        self.row_group_size = row_group_size
        self.offset_block_size = offset_block_size

    # -- keys --------------------------------------------------------------- #

    @staticmethod
    def partition_key(
        table: str, ingested_date: date, kafka_partition: int, offset_block: int
    ) -> str:
        # The bulk export uses partition -1 (see bulk_export.BULK_PARTITION) so
        # its synthetic offsets cannot collide with real Kafka ones. Name it
        # `bulk` rather than letting `%03d` render it as `-01`, which sorts
        # oddly and reads like a typo.
        part = "bulk" if kafka_partition < 0 else f"{kafka_partition:03d}"
        return (
            f"{table}/_ingested_date={ingested_date.isoformat()}/"
            f"part-{part}-{offset_block:09d}.parquet"
        )

    @staticmethod
    def partition_prefix(table: str, ingested_date: date) -> str:
        return f"{table}/_ingested_date={ingested_date.isoformat()}/"

    # -- writing ------------------------------------------------------------ #

    def write_batch(self, table_name: str, batch: pa.Table) -> list[WriteResult]:
        """Write a batch, split into (ingested_date, kafka_partition, offset_block) files."""
        if batch.num_rows == 0:
            return []
        for col in ("_kafka_offset", "_ingested_at"):
            if col not in batch.column_names:
                raise ValueError(f"batch for {table_name} is missing required column {col}")

        dates = _ingested_dates(batch)
        partitions = (
            batch.column("_kafka_partition").to_pylist()
            if "_kafka_partition" in batch.column_names
            else [0] * batch.num_rows
        )
        offsets = batch.column("_kafka_offset").to_pylist()

        groups: dict[tuple[date, int, int], list[int]] = {}
        for i, (day, part, off) in enumerate(zip(dates, partitions, offsets, strict=False)):
            groups.setdefault((day, int(part or 0), int(off) // self.offset_block_size), []).append(
                i
            )

        results: list[WriteResult] = []
        for day, part, block in sorted(groups):
            indices = groups[(day, part, block)]
            slice_ = batch.take(pa.array(indices, type=pa.int64()))
            results.append(self._write_file(table_name, day, part, block, slice_))
        return results

    def _write_file(
        self,
        table_name: str,
        day: date,
        kafka_partition: int,
        offset_block: int,
        batch: pa.Table,
    ) -> WriteResult:
        batch, deduped_in_batch = _dedupe_by_offset(batch)
        key = self.partition_key(table_name, day, kafka_partition, offset_block)

        # Merge with whatever is already at this key. Because the key is derived
        # from a fixed layout rather than from this batch, an overlapping replay
        # lands here too -- which is what makes overlap safe.
        existing = self.store.read(key)
        deduped_against_existing = 0
        if existing is not None:
            prior = pq.read_table(io.BytesIO(existing))
            before = prior.num_rows + batch.num_rows
            batch = _union_by_offset(prior, batch)
            deduped_against_existing = before - batch.num_rows

        batch = batch.sort_by([("_kafka_offset", "ascending")])
        payload = self._serialise(batch)
        self.store.write(key, payload)
        digest = hashlib.sha256(payload).hexdigest()

        log.info(
            "partition_written",
            table=table_name,
            key=key,
            rows=batch.num_rows,
            bytes=len(payload),
            sha256=digest[:16],
            deduplicated=deduped_in_batch + deduped_against_existing,
        )
        return WriteResult(
            key=key,
            rows=batch.num_rows,
            bytes_written=len(payload),
            sha256=digest,
            deduplicated=deduped_in_batch + deduped_against_existing,
        )

    def _serialise(self, batch: pa.Table) -> bytes:
        """Serialise with settings chosen for byte-stability.

        `write_statistics=False` is not an optimisation -- min/max statistics
        are stable, but the *page* layout that carries them shifts with
        dictionary state, and that is enough to break a byte comparison.
        `store_schema=False` suppresses the serialised-schema key/value blob,
        whose contents vary with pyarrow internals across patch releases.
        """
        sink = io.BytesIO()
        # `none`/`snappy` reject an explicit level; only pass it where it means
        # something. Passing it unconditionally is an ArrowInvalid at write time.
        level = self.compression_level if self.compression in ("zstd", "gzip", "brotli") else None
        pq.write_table(
            batch,
            sink,
            compression=self.compression,
            compression_level=level,
            row_group_size=self.row_group_size,
            use_dictionary=True,
            write_statistics=False,
            store_schema=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            version="2.6",
        )
        return sink.getvalue()

    # -- reading / verification --------------------------------------------- #

    def read_partition(self, table_name: str, day: date) -> pa.Table | None:
        keys = self.store.list(self.partition_prefix(table_name, day))
        tables = []
        for key in keys:
            raw = self.store.read(key)
            if raw:
                tables.append(pq.read_table(io.BytesIO(raw)))
        if not tables:
            return None
        return pa.concat_tables(tables, promote_options="permissive")

    def partition_checksums(self, table_name: str, day: date) -> dict[str, str]:
        """sha256 per file in a partition. The unit of the idempotency proof."""
        out: dict[str, str] = {}
        for key in self.store.list(self.partition_prefix(table_name, day)):
            raw = self.store.read(key)
            if raw is not None:
                out[key] = hashlib.sha256(raw).hexdigest()
        return out

    def drop_partition(self, table_name: str, day: date) -> int:
        keys = self.store.list(self.partition_prefix(table_name, day))
        for key in keys:
            self.store.delete(key)
        log.warning("partition_dropped", table=table_name, date=day.isoformat(), files=len(keys))
        return len(keys)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _ingested_dates(batch: pa.Table) -> list[date]:
    values = batch.column("_ingested_at").to_pylist()
    out: list[date] = []
    for v in values:
        if v is None:
            out.append(datetime.now(UTC).date())
        elif isinstance(v, datetime):
            out.append((v if v.tzinfo else v.replace(tzinfo=UTC)).astimezone(UTC).date())
        else:
            out.append(v)
    return out


def _dedupe_by_offset(batch: pa.Table) -> tuple[pa.Table, int]:
    """Keep the LAST row for each (partition, offset).

    Last, not first: on a rebalance the same offset can be redelivered, and the
    later delivery is the one whose surrounding state we committed against.
    """
    partitions = (
        batch.column("_kafka_partition").to_pylist()
        if "_kafka_partition" in batch.column_names
        else [0] * batch.num_rows
    )
    offsets = batch.column("_kafka_offset").to_pylist()
    last_index: dict[tuple, int] = {}
    for i, pair in enumerate(zip(partitions, offsets, strict=False)):
        last_index[pair] = i
    keep = sorted(last_index.values())
    if len(keep) == batch.num_rows:
        return batch, 0
    return batch.take(pa.array(keep, type=pa.int64())), batch.num_rows - len(keep)


def _union_by_offset(prior: pa.Table, incoming: pa.Table) -> pa.Table:
    """Union two batches, incoming winning on offset collision."""
    combined = pa.concat_tables([prior, incoming], promote_options="permissive")
    deduped, _ = _dedupe_by_offset(combined)
    return deduped

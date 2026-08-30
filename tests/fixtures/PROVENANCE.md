# Fixture provenance

Written by `tools/gen_fixtures.py` with pyarrow 25.0.1 (`pixi run fixtures`). Each file is a few rows; the point is the metadata, not the data.

- **`primitives.parquet`** (3780 bytes) — every Parquet physical type plus the int8..uint64 logical widths; nulls in every column; statistics on.
- **`logical.parquet`** (4596 bytes) — decimal (int32/int64/fixed-len backing), date, time ms/us/ns, timestamp ms/us/ns with and without UTC, string, large string.
- **`extension.parquet`** (660 bytes) — the logical types pyarrow can write natively: uuid, json, f16.
- **`nested.parquet`** (2279 bytes) — list, list of list, map and struct — repeated/optional repetition types and multi-level schema trees.
- **`encodings.parquet`** (2087 bytes) — dictionary, plain, delta-binary-packed, delta-byte-array and byte-stream-split in one file (per-column encodings).
- **`codecs.parquet`** (2611 bytes) — one column per compression codec: none, snappy, gzip, zstd, lz4, brotli.
- **`pageindex.parquet`** (5337 bytes) — three row groups, page index (OffsetIndex + ColumnIndex) written, small pages so several page headers per chunk.
- **`nostats.parquet`** (2435 bytes) — statistics disabled and no page index — the optional-field-absent path.
- **`v2pages.parquet`** (3058 bytes) — data page v2 headers (DataPageHeaderV2) over two row groups, zstd, and a bloom filter on `k` (BloomFilterHeader on disk).

`<name>.oracle.txt` next to each file is the canonical fact dump written by `tools/oracle_pyarrow.py`, which decodes the footer with the Apache Thrift compiler's own generated Python bindings and cross-checks every fact against pyarrow before writing it. `<name>.oracle.json` is the same metadata for a human to read. The Mojo tests reproduce the `.txt` from our own decode, line for line.

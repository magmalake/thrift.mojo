# Fixture provenance

Written by `tools/gen_fixtures.py` with pyarrow 25.0.1 (`pixi run fixtures`). Each file is a few rows; the point is the metadata, not the data.

- **`primitives.parquet`** (3780 bytes) — every Parquet physical type plus the int8..uint64 logical widths; nulls in every column; statistics on.
- **`logical.parquet`** (4596 bytes) — decimal (int32/int64/fixed-len backing), date, time ms/us/ns, timestamp ms/us/ns with and without UTC, string, large string.
- **`extension.parquet`** (492 bytes) — the logical types pyarrow can write natively: uuid, json.
- **`nested.parquet`** (2279 bytes) — list, list of list, map and struct — repeated/optional repetition types and multi-level schema trees.
- **`encodings.parquet`** (2087 bytes) — dictionary, plain, delta-binary-packed, delta-byte-array and byte-stream-split in one file (per-column encodings).
- **`codecs.parquet`** (2611 bytes) — one column per compression codec: none, snappy, gzip, zstd, lz4, brotli.
- **`pageindex.parquet`** (5337 bytes) — three row groups, page index (OffsetIndex + ColumnIndex) written, small pages so several page headers per chunk.
- **`nostats.parquet`** (2435 bytes) — statistics disabled and no page index — the optional-field-absent path.
- **`v2pages.parquet`** (2886 bytes) — data page v2 headers (DataPageHeaderV2) over two row groups, zstd (this pyarrow cannot write bloom filters).

`<name>.oracle.json` next to each file is `pyarrow.parquet.ParquetFile(...).metadata` dumped by `tools/oracle_pyarrow.py`; the Mojo tests assert our decoded `FileMetaData` matches it field by field.

#!/usr/bin/env python3
"""Write the tests/fixtures/*.parquet files with pyarrow.

Each fixture is deliberately tiny (a handful of rows) but aims at one corner
of the metadata schema: physical types, logical types, nesting, encodings,
codecs, statistics, the page index, data page v1 vs v2.

    python3 tools/gen_fixtures.py tests/fixtures
"""

import datetime
import decimal
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

PROVENANCE = []


def note(name, what):
    PROVENANCE.append((name, what))


def write(out_dir, name, table, what, **kw):
    path = os.path.join(out_dir, name)
    pq.write_table(table, path, **kw)
    note(name, what)
    return path


def primitives_table():
    n = 6
    return pa.table(
        {
            "b": pa.array([True, False, True, None, False, True], pa.bool_()),
            "i8": pa.array([-128, -1, 0, 1, 127, None], pa.int8()),
            "i16": pa.array([-32768, -1, 0, 1, 32767, None], pa.int16()),
            "i32": pa.array([-2147483648, -1, 0, 1, 2147483647, None], pa.int32()),
            "i64": pa.array(
                [-(2**63), -1, 0, 1, 2**63 - 1, None], pa.int64()
            ),
            "u8": pa.array([0, 1, 2, 3, 255, None], pa.uint8()),
            "u16": pa.array([0, 1, 2, 3, 65535, None], pa.uint16()),
            "u32": pa.array([0, 1, 2, 3, 4294967295, None], pa.uint32()),
            "u64": pa.array([0, 1, 2, 3, 2**64 - 1, None], pa.uint64()),
            "f32": pa.array([-1.5, 0.0, 1.5, float("inf"), -0.0, None], pa.float32()),
            "f64": pa.array(
                [-1.5, 0.0, 1.5, float("-inf"), 3.25, None], pa.float64()
            ),
            "s": pa.array(["", "a", "bb", "ccc", "é世界", None], pa.string()),
            "bin": pa.array([b"", b"\x00", b"\xff\xfe", b"abc", b"z", None], pa.binary()),
            "flba": pa.array(
                [b"0123456789abcdef"] * 5 + [None], pa.binary(16)
            ),
        }
    )


def logical_table():
    return pa.table(
        {
            "dec32": pa.array(
                [decimal.Decimal("1.23"), decimal.Decimal("-4.56"), None],
                pa.decimal128(5, 2),
            ),
            "dec64": pa.array(
                [decimal.Decimal("1.234567890"), decimal.Decimal("-9.87"), None],
                pa.decimal128(18, 9),
            ),
            "dec_flba": pa.array(
                [decimal.Decimal("1e10"), decimal.Decimal("-2e10"), None],
                pa.decimal128(30, 4),
            ),
            "date": pa.array(
                [datetime.date(1970, 1, 1), datetime.date(2026, 8, 29), None],
                pa.date32(),
            ),
            "time_ms": pa.array([0, 3661000, None], pa.time32("ms")),
            "time_us": pa.array([0, 3661000000, None], pa.time64("us")),
            "time_ns": pa.array([0, 3661000000000, None], pa.time64("ns")),
            "ts_ms": pa.array([0, 1700000000000, None], pa.timestamp("ms")),
            "ts_us": pa.array([0, 1700000000000000, None], pa.timestamp("us")),
            "ts_ns": pa.array([0, 1700000000000000000, None], pa.timestamp("ns")),
            "ts_ms_utc": pa.array(
                [0, 1700000000000, None], pa.timestamp("ms", tz="UTC")
            ),
            "ts_us_utc": pa.array(
                [0, 1700000000000000, None], pa.timestamp("us", tz="UTC")
            ),
            "ts_ns_utc": pa.array(
                [0, 1700000000000000000, None], pa.timestamp("ns", tz="UTC")
            ),
            "str": pa.array(["x", "y", None], pa.string()),
            "large_str": pa.array(["x", "y", None], pa.large_string()),
        }
    )


def extension_table():
    """Types pyarrow only writes with a logical annotation on newer versions."""
    cols = {}
    try:
        cols["uuid"] = pa.array(
            [b"0123456789abcdef", b"fedcba9876543210", None], pa.uuid()
        )
    except Exception:
        pass
    try:
        cols["json"] = pa.array(['{"a":1}', "[]", None], pa.json_())
    except Exception:
        pass
    try:
        import numpy as np

        cols["f16"] = pa.array(
            np.array([1.5, -2.25, np.nan], dtype=np.float16), pa.float16()
        )
    except Exception:
        pass
    if not cols:
        return None
    return pa.table(cols)


def nested_table():
    return pa.table(
        {
            "l": pa.array([[1, 2], [], None, [3]], pa.list_(pa.int32())),
            "ll": pa.array(
                [[[1], [2, 3]], [], None, [[]]],
                pa.list_(pa.list_(pa.int64())),
            ),
            "m": pa.array(
                [[("a", 1)], [], None, [("b", 2), ("c", 3)]],
                pa.map_(pa.string(), pa.int32()),
            ),
            "st": pa.array(
                [
                    {"x": 1, "y": "a"},
                    {"x": 2, "y": None},
                    None,
                    {"x": 4, "y": "d"},
                ],
                pa.struct([("x", pa.int32()), ("y", pa.string())]),
            ),
        }
    )


def encodings_table():
    return pa.table(
        {
            "dict_col": pa.array(["a", "b", "a", "b", "c"] * 8, pa.string()),
            "plain_col": pa.array([f"s{i}" for i in range(40)], pa.string()),
            "delta_int": pa.array(list(range(40)), pa.int64()),
            "delta_str": pa.array([f"pre{i:04d}" for i in range(40)], pa.string()),
            "bss": pa.array([float(i) * 1.5 for i in range(40)], pa.float64()),
        }
    )


def wide_table(rows=200):
    return pa.table(
        {
            "k": pa.array(list(range(rows)), pa.int64()),
            "v": pa.array([f"row-{i}" for i in range(rows)], pa.string()),
            "d": pa.array([float(i) / 3.0 for i in range(rows)], pa.float64()),
        }
    )


def main():
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)

    write(
        out_dir,
        "primitives.parquet",
        primitives_table(),
        "every Parquet physical type plus the int8..uint64 logical widths;"
        " nulls in every column; statistics on",
        compression="none",
        write_statistics=True,
    )

    write(
        out_dir,
        "logical.parquet",
        logical_table(),
        "decimal (int32/int64/fixed-len backing), date, time ms/us/ns,"
        " timestamp ms/us/ns with and without UTC, string, large string",
        compression="none",
    )

    ext = extension_table()
    if ext is not None:
        write(
            out_dir,
            "extension.parquet",
            ext,
            "the logical types pyarrow can write natively: "
            + ", ".join(ext.schema.names),
            compression="none",
            store_schema=False,
        )

    write(
        out_dir,
        "nested.parquet",
        nested_table(),
        "list, list of list, map and struct — repeated/optional repetition"
        " types and multi-level schema trees",
        compression="none",
    )

    write(
        out_dir,
        "encodings.parquet",
        encodings_table(),
        "dictionary, plain, delta-binary-packed, delta-byte-array and"
        " byte-stream-split in one file (per-column encodings)",
        compression="none",
        use_dictionary=["dict_col"],
        column_encoding={
            "plain_col": "PLAIN",
            "delta_int": "DELTA_BINARY_PACKED",
            "delta_str": "DELTA_BYTE_ARRAY",
            "bss": "BYTE_STREAM_SPLIT",
        },
        write_statistics=True,
    )

    codecs = ["none", "snappy", "gzip", "zstd", "lz4", "brotli"]
    cols = {}
    per_col = {}
    for c in codecs:
        cols[c] = pa.array([f"{c}-{i}" for i in range(30)], pa.string())
        per_col[c] = c
    write(
        out_dir,
        "codecs.parquet",
        pa.table(cols),
        "one column per compression codec: " + ", ".join(codecs),
        compression=per_col,
    )

    write(
        out_dir,
        "pageindex.parquet",
        wide_table(),
        "three row groups, page index (OffsetIndex + ColumnIndex) written,"
        " small pages so several page headers per chunk",
        compression="snappy",
        row_group_size=80,
        data_page_size=512,
        write_page_index=True,
        write_statistics=True,
    )

    write(
        out_dir,
        "nostats.parquet",
        wide_table(60),
        "statistics disabled and no page index — the optional-field-absent"
        " path",
        compression="none",
        write_statistics=False,
        write_page_index=False,
    )

    v2_kw = dict(
        compression="zstd",
        data_page_version="2.0",
        row_group_size=70,
        data_page_size=512,
        write_statistics=True,
    )
    # `bloom_filter_options` is a {column: {ndv, fpp}} dict on recent pyarrow;
    # older versions do not have the parameter at all.
    try:
        write(
            out_dir,
            "v2pages.parquet",
            wide_table(140),
            "data page v2 headers (DataPageHeaderV2) over two row groups,"
            " zstd, and a bloom filter on `k` (BloomFilterHeader on disk)",
            bloom_filter_options={"k": {"ndv": 140, "fpp": 0.05}},
            **v2_kw,
        )
    except TypeError:
        write(
            out_dir,
            "v2pages.parquet",
            wide_table(140),
            "data page v2 headers (DataPageHeaderV2) over two row groups,"
            " zstd (this pyarrow cannot write bloom filters)",
            **v2_kw,
        )

    with open(os.path.join(out_dir, "PROVENANCE.md"), "w") as fh:
        fh.write("# Fixture provenance\n\n")
        fh.write(
            "Written by `tools/gen_fixtures.py` with pyarrow %s"
            " (`pixi run fixtures`). Each file is a few rows; the point is"
            " the metadata, not the data.\n\n" % pa.__version__
        )
        for name, what in PROVENANCE:
            size = os.path.getsize(os.path.join(out_dir, name))
            fh.write("- **`%s`** (%d bytes) — %s.\n" % (name, size, what))
        fh.write(
            "\n`<name>.oracle.json` next to each file is"
            " `pyarrow.parquet.ParquetFile(...).metadata` dumped by"
            " `tools/oracle_pyarrow.py`; the Mojo tests assert our decoded"
            " `FileMetaData` matches it field by field.\n"
        )
    print("wrote %d fixtures to %s" % (len(PROVENANCE), out_dir))


if __name__ == "__main__":
    main()

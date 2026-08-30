#!/usr/bin/env python3
"""Build the oracle the Mojo tests check each fixture against.

Two independent references are used and they must agree:

1. **Apache Thrift itself.** `thrift --gen py spec/parquet.thrift` produces
   the official generated bindings; decoding a footer with them and Apache's
   own `TCompactProtocol` gives the ground truth at the Thrift level,
   including the raw statistics bytes. This is what the oracle records.
2. **pyarrow.** `ParquetFile.metadata` is cross-checked against every fact
   the oracle records. pyarrow's Python surface is lossy in two documented
   places (see `PYARROW_BLIND_SPOTS`); everything else must match exactly,
   or this script fails rather than writing an oracle.

For each `tests/fixtures/*.parquet` it writes `<name>.oracle.txt` — one
canonical `key=value` line per fact, which `tests/parquet_expect.mojo`
reproduces from our own decode — and `<name>.oracle.json` for humans.

    pixi global install thrift-compiler   # or conda install thrift-compiler
    python3 tools/oracle_pyarrow.py tests/fixtures

Requires: thrift (compiler), thrift (Python runtime), pyarrow.
"""

import datetime
import decimal
import glob
import json
import os
import struct
import subprocess
import sys
import tempfile

PYARROW_BLIND_SPOTS = """\
pyarrow renders two Thrift fields lossily, so the cross-check tolerates
exactly these differences (both verified against the generated Thrift
bindings, which see the real values):

* `CompressionCodec.LZ4_RAW` (7) is reported as "LZ4" — pyarrow maps the
  Parquet codec onto Arrow's codec enum, which has one LZ4.
* `ConvertedType.TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS` are reported as NONE
  on a column whose LogicalType says `isAdjustedToUTC=false`, because the
  converted type implies UTC and pyarrow suppresses the contradiction. The
  bytes on disk do carry the converted type.
"""

EPOCH_DATE = datetime.date(1970, 1, 1)
UNIT_PER_US = {"MILLIS": ("div", 1000), "MICROS": ("mul", 1), "NANOS": ("mul", 1000)}


def gen_bindings(idl, workdir):
    """Run the Thrift compiler over parquet.thrift and import the result."""
    out = os.path.join(workdir, "gen")
    os.makedirs(out, exist_ok=True)
    subprocess.run(
        ["thrift", "--gen", "py", "-out", out, idl], check=True
    )
    sys.path.insert(0, out)
    import parquet.ttypes as tt

    return tt


def read_footer(tt, path):
    from thrift.protocol.TCompactProtocol import TCompactProtocol
    from thrift.transport.TTransport import TMemoryBuffer

    data = open(path, "rb").read()
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise ValueError("%s is not a PAR1 file" % path)
    n = struct.unpack("<i", data[-8:-4])[0]
    fm = tt.FileMetaData()
    fm.read(TCompactProtocol(TMemoryBuffer(data[-8 - n : -8])))
    return fm


def enum_name(cls, value):
    if value is None:
        return "NONE"
    return cls._VALUES_TO_NAMES.get(value, "%s(%d)" % (cls.__name__, value))


def unit_name(unit):
    if unit is None:
        raise ValueError("TimeUnit with no member set")
    if unit.MILLIS is not None:
        return "MILLIS"
    if unit.MICROS is not None:
        return "MICROS"
    if unit.NANOS is not None:
        return "NANOS"
    raise ValueError("TimeUnit with no member set")


def logical_string(lt):
    if lt is None:
        return "NONE"
    if lt.STRING is not None:
        return "String"
    if lt.MAP is not None:
        return "Map"
    if lt.LIST is not None:
        return "List"
    if lt.ENUM is not None:
        return "Enum"
    if lt.DECIMAL is not None:
        return "Decimal(%d,%d)" % (lt.DECIMAL.precision, lt.DECIMAL.scale)
    if lt.DATE is not None:
        return "Date"
    if lt.TIME is not None:
        return "Time(%s,%s)" % (
            "true" if lt.TIME.isAdjustedToUTC else "false",
            unit_name(lt.TIME.unit),
        )
    if lt.TIMESTAMP is not None:
        return "Timestamp(%s,%s)" % (
            "true" if lt.TIMESTAMP.isAdjustedToUTC else "false",
            unit_name(lt.TIMESTAMP.unit),
        )
    if lt.INTEGER is not None:
        return "Int(%d,%s)" % (
            lt.INTEGER.bitWidth,
            "true" if lt.INTEGER.isSigned else "false",
        )
    for member, spelling in (
        ("UNKNOWN", "Null"),
        ("JSON", "Json"),
        ("BSON", "Bson"),
        ("UUID", "UUID"),
        ("FLOAT16", "Float16"),
        ("VARIANT", "Variant"),
        ("GEOMETRY", "Geometry"),
        ("GEOGRAPHY", "Geography"),
        ("FILE", "File"),
    ):
        if getattr(lt, member, None) is not None:
            return spelling
    raise ValueError("LogicalType with no member set")


class Leaf:
    def __init__(self, index, path, max_def, max_rep):
        self.index = index
        self.path = path
        self.max_def = max_def
        self.max_rep = max_rep


def collect_leaves(tt, schema):
    """Depth-first walk of the flat SchemaElement list; root has no name."""
    leaves = []
    pos = [1]

    def walk(prefix, d, r):
        idx = pos[0]
        e = schema[idx]
        pos[0] += 1
        path = e.name if not prefix else prefix + "." + e.name
        if e.repetition_type == tt.FieldRepetitionType.OPTIONAL:
            d += 1
        elif e.repetition_type == tt.FieldRepetitionType.REPEATED:
            d += 1
            r += 1
        nc = e.num_children or 0
        if nc == 0:
            leaves.append(Leaf(idx, path, d, r))
        else:
            for _ in range(nc):
                walk(path, d, r)

    for _ in range(schema[0].num_children or 0):
        walk("", 0, 0)
    return leaves


def opt(v):
    return "-" if v is None else str(v)


def canonical(tt, fm):
    lines = []
    a = lines.append
    a("version=%d" % fm.version)
    a("created_by=%s" % (fm.created_by or ""))
    a("num_rows=%d" % fm.num_rows)
    a("num_row_groups=%d" % len(fm.row_groups))
    leaves = collect_leaves(tt, fm.schema)
    a("num_leaves=%d" % len(leaves))
    a("num_schema_elements=%d" % len(fm.schema))
    kv = fm.key_value_metadata or []
    a("kv_count=%d" % len(kv))
    for e in sorted(kv, key=lambda e: e.key):
        a("kv=%s|%d" % (e.key, len(e.value.encode("utf-8")) if e.value else 0))
    for i, lf in enumerate(leaves):
        e = fm.schema[lf.index]
        a(
            "leaf=%d|%s|%s|%s|%s|%d|%d|%d|%d|%d"
            % (
                i,
                lf.path,
                enum_name(tt.Type, e.type),
                enum_name(tt.ConvertedType, e.converted_type),
                logical_string(e.logicalType),
                lf.max_def,
                lf.max_rep,
                e.type_length or 0,
                -1 if e.precision is None else e.precision,
                -1 if e.scale is None else e.scale,
            )
        )
    for r, rg in enumerate(fm.row_groups):
        a(
            "rg=%d|%d|%d|%d|%d"
            % (
                r,
                rg.num_rows,
                rg.total_byte_size,
                len(rg.columns),
                len(rg.sorting_columns or ()),
            )
        )
        for j, cc in enumerate(rg.columns):
            cm = cc.meta_data
            a(
                "col=%d|%d|%s|%s|%s|%s|%d|%d|%s|%d|%d|%d|%s|%s"
                % (
                    r,
                    j,
                    ".".join(cm.path_in_schema),
                    enum_name(tt.Type, cm.type),
                    enum_name(tt.CompressionCodec, cm.codec),
                    ",".join(
                        sorted(enum_name(tt.Encoding, e) for e in cm.encodings)
                    ),
                    cm.num_values,
                    cm.data_page_offset,
                    opt(cm.dictionary_page_offset),
                    cm.total_compressed_size,
                    cm.total_uncompressed_size,
                    cc.file_offset,
                    opt(cm.bloom_filter_offset),
                    opt(cm.bloom_filter_length),
                )
            )
            st = cm.statistics
            if st is None:
                a("stats=%d|%d|-" % (r, j))
                continue
            mn = st.min_value if st.min_value is not None else st.min
            mx = st.max_value if st.max_value is not None else st.max
            if mn is None or mx is None:
                mn_hex = mx_hex = "-"
            else:
                mn_hex = to_bytes(mn).hex()
                mx_hex = to_bytes(mx).hex()
            a(
                "stats=%d|%d|set|%s|%s|%s|%s"
                % (r, j, mn_hex, mx_hex, opt(st.null_count), opt(st.distinct_count))
            )
    return lines


def to_bytes(v):
    return v if isinstance(v, bytes) else v.encode("utf-8")


# ── the pyarrow cross-check ────────────────────────────────────────────────


def as_int(value, scale, unit):
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, decimal.Decimal):
        return int(value.scaleb(scale))
    if isinstance(value, datetime.datetime):
        base = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        d = value - base
        return convert_us(
            d.days * 86400000000 + d.seconds * 1000000 + d.microseconds, unit
        )
    if isinstance(value, datetime.date):
        return (value - EPOCH_DATE).days
    if isinstance(value, datetime.time):
        us = (
            value.hour * 3600000000
            + value.minute * 60000000
            + value.second * 1000000
            + value.microsecond
        )
        return convert_us(us, unit)
    raise TypeError("cannot encode %r as an integer" % (value,))


def convert_us(us, unit):
    op, k = UNIT_PER_US[unit or "MICROS"]
    return us // k if op == "div" else us * k


def raw_bytes(value, physical, logical, length, precision, scale):
    if physical == "BOOLEAN":
        return b"\x01" if value else b"\x00"
    if physical in ("INT32", "INT64"):
        unit = None
        if logical.startswith("Time(") or logical.startswith("Timestamp("):
            unit = logical.rstrip(")").split(",")[-1]
        n = as_int(value, scale, unit)
        signed = not (logical.startswith("Int(") and logical.endswith("false)"))
        fmt = ("<i" if signed else "<I") if physical == "INT32" else (
            "<q" if signed else "<Q"
        )
        return struct.pack(fmt, n)
    if physical == "FLOAT":
        return struct.pack("<f", value)
    if physical == "DOUBLE":
        return struct.pack("<d", value)
    if physical == "BYTE_ARRAY":
        return value.encode("utf-8") if isinstance(value, str) else bytes(value)
    if physical == "FIXED_LEN_BYTE_ARRAY":
        if isinstance(value, decimal.Decimal):
            return int(value.scaleb(scale)).to_bytes(length, "big", signed=True)
        return bytes(value)
    raise TypeError("unhandled physical type %s" % physical)


def cross_check(path, tt, fm, lines):
    """Every fact in `lines` must also be what pyarrow reports."""
    import pyarrow.parquet as pq

    m = pq.ParquetFile(path).metadata
    schema = m.schema
    problems = []

    def eq(what, got, want):
        if got != want:
            problems.append("%s: thrift=%r pyarrow=%r" % (what, want, got))

    eq("version", int(str(m.format_version).split(".")[0]), fm.version)
    eq("created_by", m.created_by or "", fm.created_by or "")
    eq("num_rows", m.num_rows, fm.num_rows)
    eq("num_row_groups", m.num_row_groups, len(fm.row_groups))
    leaves = collect_leaves(tt, fm.schema)
    eq("num_leaves", len(schema), len(leaves))
    kv = m.metadata or {}
    eq("kv_count", len(kv), len(fm.key_value_metadata or []))

    for i, lf in enumerate(leaves):
        e = fm.schema[lf.index]
        c = schema.column(i)
        eq("leaf%d.path" % i, c.path, lf.path)
        eq("leaf%d.physical" % i, c.physical_type, enum_name(tt.Type, e.type))
        eq("leaf%d.max_def" % i, c.max_definition_level, lf.max_def)
        eq("leaf%d.max_rep" % i, c.max_repetition_level, lf.max_rep)
        eq("leaf%d.length" % i, c.length, e.type_length or 0)
        eq(
            "leaf%d.precision" % i,
            c.precision,
            -1 if e.precision is None else e.precision,
        )
        eq("leaf%d.scale" % i, c.scale, -1 if e.scale is None else e.scale)
        want_conv = enum_name(tt.ConvertedType, e.converted_type)
        got_conv = c.converted_type
        if not (
            got_conv == "NONE"
            and want_conv in ("TIMESTAMP_MILLIS", "TIMESTAMP_MICROS")
        ):
            eq("leaf%d.converted" % i, got_conv, want_conv)
        eq(
            "leaf%d.logical" % i,
            pyarrow_logical(c),
            logical_string(e.logicalType),
        )

    for r, rg in enumerate(fm.row_groups):
        prg = m.row_group(r)
        eq("rg%d.num_rows" % r, prg.num_rows, rg.num_rows)
        eq("rg%d.total_byte_size" % r, prg.total_byte_size, rg.total_byte_size)
        eq("rg%d.num_columns" % r, prg.num_columns, len(rg.columns))
        for j, cc in enumerate(rg.columns):
            cm = cc.meta_data
            pc = prg.column(j)
            tag = "rg%d.col%d" % (r, j)
            eq(tag + ".path", pc.path_in_schema, ".".join(cm.path_in_schema))
            eq(tag + ".physical", pc.physical_type, enum_name(tt.Type, cm.type))
            want_codec = enum_name(tt.CompressionCodec, cm.codec)
            got_codec = pc.compression
            if not (got_codec == "LZ4" and want_codec == "LZ4_RAW"):
                eq(tag + ".codec", got_codec, want_codec)
            eq(
                tag + ".encodings",
                ",".join(sorted(pc.encodings)),
                ",".join(sorted(enum_name(tt.Encoding, e) for e in cm.encodings)),
            )
            eq(tag + ".num_values", pc.num_values, cm.num_values)
            eq(tag + ".data_page_offset", pc.data_page_offset, cm.data_page_offset)
            eq(
                tag + ".dictionary_page_offset",
                pc.dictionary_page_offset if pc.has_dictionary_page else None,
                cm.dictionary_page_offset,
            )
            eq(
                tag + ".total_compressed_size",
                pc.total_compressed_size,
                cm.total_compressed_size,
            )
            eq(
                tag + ".total_uncompressed_size",
                pc.total_uncompressed_size,
                cm.total_uncompressed_size,
            )
            eq(tag + ".file_offset", pc.file_offset, cc.file_offset)
            eq(
                tag + ".bloom_filter_offset",
                pc.bloom_filter_offset,
                cm.bloom_filter_offset,
            )
            st = cm.statistics
            pst = pc.statistics
            if st is None:
                eq(tag + ".stats", pst is not None and pc.is_stats_set, False)
                continue
            if pst is None:
                problems.append(tag + ".stats: pyarrow has none, thrift does")
                continue
            eq(tag + ".null_count", pst.null_count, st.null_count)
            eq(tag + ".distinct_count", pst.distinct_count, st.distinct_count)
            mn = st.min_value if st.min_value is not None else st.min
            mx = st.max_value if st.max_value is not None else st.max
            if pst.has_min_max and mn is not None:
                c = schema.column(j) if len(schema) > j else None
                lf_i = [x.path for x in leaves].index(".".join(cm.path_in_schema))
                c = schema.column(lf_i)
                e = fm.schema[leaves[lf_i].index]
                args = (
                    enum_name(tt.Type, cm.type),
                    logical_string(e.logicalType),
                    e.type_length or 0,
                    e.precision or 0,
                    e.scale or 0,
                )
                eq(tag + ".min", raw_bytes(pst.min, *args), to_bytes(mn))
                eq(tag + ".max", raw_bytes(pst.max, *args), to_bytes(mx))
    return problems


def pyarrow_logical(c):
    j = json.loads(c.logical_type.to_json())
    t = j.get("Type")
    if t in (None, "None", "Undefined"):
        return "NONE"
    if t == "Decimal":
        return "Decimal(%d,%d)" % (j["precision"], j["scale"])
    short = {
        "milliseconds": "MILLIS",
        "microseconds": "MICROS",
        "nanoseconds": "NANOS",
    }
    if t in ("Time", "Timestamp"):
        return "%s(%s,%s)" % (
            t,
            "true" if j["isAdjustedToUTC"] else "false",
            short[j["timeUnit"]],
        )
    if t == "Int":
        return "Int(%d,%s)" % (j["bitWidth"], "true" if j["isSigned"] else "false")
    return {
        "String": "String",
        "Map": "Map",
        "List": "List",
        "Enum": "Enum",
        "Date": "Date",
        "JSON": "Json",
        "BSON": "Bson",
        "UUID": "UUID",
        "Float16": "Float16",
        "Null": "Null",
        "Variant": "Variant",
        "Geometry": "Geometry",
        "Geography": "Geography",
    }[t]


def main():
    out_dir = sys.argv[1]
    idl = os.path.join(os.path.dirname(out_dir), "..", "spec", "parquet.thrift")
    idl = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spec", "parquet.thrift")
    )
    failures = 0
    with tempfile.TemporaryDirectory() as workdir:
        tt = gen_bindings(idl, workdir)
        for path in sorted(glob.glob(os.path.join(out_dir, "*.parquet"))):
            fm = read_footer(tt, path)
            lines = canonical(tt, fm)
            problems = cross_check(path, tt, fm, lines)
            if problems:
                failures += 1
                print("!! %s" % os.path.basename(path))
                for p in problems:
                    print("     %s" % p)
                continue
            with open(path + ".oracle.txt", "w") as fh:
                fh.write("\n".join(lines) + "\n")
            with open(path + ".oracle.json", "w") as fh:
                json.dump(json.loads(json.dumps(fm, default=str)), fh, indent=1)
            print(
                "%-24s %3d facts, pyarrow agrees"
                % (os.path.basename(path), len(lines))
            )
    if failures:
        print("\n%d fixture(s) disagreed; no oracle written for them" % failures)
        sys.exit(1)
    print("\n" + PYARROW_BLIND_SPOTS)


if __name__ == "__main__":
    main()

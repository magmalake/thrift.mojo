"""Benchmarks — `pixi run -e bench bench`.

The headline number is footer parsing: a Parquet file with 1,000 columns and
50 row groups has 50,000 `ColumnChunk`s in its footer, which is where every
reader spends its first few milliseconds. The footer here is synthesised in
Mojo with the same shape and field population pyarrow writes for such a file
(measured against a real one — see the README), so the benchmark needs no
multi-megabyte fixture in the repository.

Synthesising that footer is not cheap, and the harness re-enters a benchmark
body once per phase. Only what is inside `b.iter` is timed, so the cost is
wall-clock only -- but it is why the large-footer benchmarks run with fewer
repetitions than the default.
"""

from bench import Benchmark, BenchSuite, Metric, keep

from thrift.parquet_footer import read_footer_bytes, write_footer
from thrift.parquet_types import (
    ColumnChunk,
    ColumnMetaData,
    CompressionCodec,
    Encoding,
    FieldRepetitionType,
    FileMetaData,
    KeyValue,
    RowGroup,
    SchemaElement,
    Statistics,
    Type,
)
from thrift.protocol import (
    T_I32,
    T_STOP,
    T_STRING,
    T_STRUCT,
    TCompactProtocolReader,
    TCompactProtocolWriter,
)


def _synth_footer(columns: Int, row_groups: Int) raises -> FileMetaData:
    var meta = FileMetaData()
    meta.version = 2
    meta.created_by = String("thrift.mojo bench")
    meta.num_rows = Int64(row_groups) * 100000

    var root = SchemaElement()
    root.name = String("schema")
    root.num_children = Int32(columns)
    root.repetition_type = FieldRepetitionType.REQUIRED
    meta.schema.append(root^)
    for c in range(columns):
        var se = SchemaElement()
        se.name = String("column_", c)
        se.type_ = Type.INT64 if c % 2 == 0 else Type.BYTE_ARRAY
        se.repetition_type = FieldRepetitionType.OPTIONAL
        se.field_id = Int32(c + 1)
        meta.schema.append(se^)

    var offset = Int64(4)
    for r in range(row_groups):
        var rg = RowGroup()
        rg.num_rows = 100000
        rg.total_byte_size = 0
        for c in range(columns):
            var cm = ColumnMetaData()
            cm.type_ = Type.INT64 if c % 2 == 0 else Type.BYTE_ARRAY
            cm.encodings.append(Encoding.PLAIN)
            cm.encodings.append(Encoding.RLE)
            cm.encodings.append(Encoding.RLE_DICTIONARY)
            cm.path_in_schema.append(String("column_", c))
            cm.codec = CompressionCodec.SNAPPY
            cm.num_values = 100000
            cm.total_uncompressed_size = 800000
            cm.total_compressed_size = 400000
            cm.data_page_offset = offset + 128
            cm.dictionary_page_offset = offset
            var st = Statistics()
            var lo = List[UInt8]()
            var hi = List[UInt8]()
            for k in range(8):
                lo.append(UInt8(k))
                hi.append(UInt8(255 - k))
            st.min_value = lo^
            st.max_value = hi^
            st.null_count = Int64(c)
            cm.statistics = st^
            rg.total_byte_size += cm.total_uncompressed_size
            offset += cm.total_compressed_size
            var cc = ColumnChunk()
            cc.file_offset = cm.data_page_offset
            cc.meta_data = cm^
            rg.columns.append(cc^)
        rg.total_compressed_size = Int64(columns) * 400000
        rg.ordinal = Int16(r)
        meta.row_groups.append(rg^)

    var kv = KeyValue()
    kv.key = String("ARROW:schema")
    kv.value = String("(elided)")
    var kvs = List[KeyValue]()
    kvs.append(kv^)
    meta.key_value_metadata = kvs^
    return meta^


comptime LARGE_COLUMNS = 1000
comptime LARGE_ROW_GROUPS = 50
comptime SMALL_COLUMNS = 10
comptime SMALL_ROW_GROUPS = 1
comptime VARINTS = 10_000


# ── footer round trip ───────────────────────────────────────────────────────
#
# The count declared for throughput is column chunks, so the rate column reads
# as chunks/s -- the same figure the old bench printed by hand, and the one
# that says how a reader will scale with footer size.


def bench_read_footer_large(mut b: Benchmark) raises:
    var meta = _synth_footer(LARGE_COLUMNS, LARGE_ROW_GROUPS)
    var body = write_footer(meta)
    b.throughput(Metric.elements(), LARGE_COLUMNS * LARGE_ROW_GROUPS)

    @parameter
    def call() raises:
        var again = read_footer_bytes(Span(body))
        keep(len(again.row_groups))

    b.iter[call]()
    keep(body)


def bench_write_footer_large(mut b: Benchmark) raises:
    var meta = _synth_footer(LARGE_COLUMNS, LARGE_ROW_GROUPS)
    b.throughput(Metric.elements(), LARGE_COLUMNS * LARGE_ROW_GROUPS)

    @parameter
    def call() raises:
        var out = write_footer(meta)
        keep(len(out))

    b.iter[call]()
    keep(meta.num_rows)


def bench_read_footer_small(mut b: Benchmark) raises:
    var meta = _synth_footer(SMALL_COLUMNS, SMALL_ROW_GROUPS)
    var body = write_footer(meta)
    b.throughput(Metric.elements(), SMALL_COLUMNS * SMALL_ROW_GROUPS)

    @parameter
    def call() raises:
        var again = read_footer_bytes(Span(body))
        keep(len(again.row_groups))

    b.iter[call]()
    keep(body)


def bench_write_footer_small(mut b: Benchmark) raises:
    var meta = _synth_footer(SMALL_COLUMNS, SMALL_ROW_GROUPS)
    b.throughput(Metric.elements(), SMALL_COLUMNS * SMALL_ROW_GROUPS)

    @parameter
    def call() raises:
        var out = write_footer(meta)
        keep(len(out))

    b.iter[call]()
    keep(meta.num_rows)


# ── skipping and primitives ─────────────────────────────────────────────────


def bench_skip_footer(mut b: Benchmark) raises:
    """How fast the recursive skipper steps over a footer it ignores."""
    var meta = _synth_footer(100, 10)
    var body = write_footer(meta)
    b.throughput(Metric.bytes(), len(body))

    @parameter
    def call() raises:
        var r = TCompactProtocolReader(Span(body))
        r.skip(T_STRUCT)
        keep(r.remaining())

    b.iter[call]()
    keep(body)


def bench_read_i64_varints(mut b: Benchmark) raises:
    var w = TCompactProtocolWriter()
    for i in range(VARINTS):
        w.write_i64(Int64(i) * Int64(-7919))
    var buf = w^.take()
    b.throughput(Metric.elements(), VARINTS)

    @parameter
    def call() raises:
        var r = TCompactProtocolReader(Span(buf))
        var acc = Int64(0)
        for _ in range(VARINTS):
            acc += r.read_i64()
        keep(acc)

    b.iter[call]()
    keep(buf)


def _print_shape() raises:
    """Footer sizes and the correctness checks the old bench folded into its
    timing loops. Neither belongs inside a timed region."""
    var large = _synth_footer(LARGE_COLUMNS, LARGE_ROW_GROUPS)
    var large_body = write_footer(large)
    var small_body = write_footer(_synth_footer(SMALL_COLUMNS, SMALL_ROW_GROUPS))
    var skip_body = write_footer(_synth_footer(100, 10))

    var again = read_footer_bytes(Span(large_body))
    if len(again.row_groups) != LARGE_ROW_GROUPS:
        raise Error("large footer did not round trip")
    if len(write_footer(large)) != len(large_body):
        raise Error("write_footer is not deterministic")

    var r = TCompactProtocolReader(Span(skip_body))
    r.skip(T_STRUCT)
    if r.remaining() != 0:
        raise Error("skip did not consume the footer")

    print(
        "footers: large", LARGE_COLUMNS, "x", LARGE_ROW_GROUPS, "=",
        LARGE_COLUMNS * LARGE_ROW_GROUPS, "chunks,", len(large_body) // 1024,
        "KiB | small", len(small_body), "B | skip target",
        len(skip_body) // 1024, "KiB",
    )


def main() raises:
    _print_shape()
    # Three repetitions, not five: synthesising the 50,000-chunk footer runs
    # once per phase and dominates wall-clock time.
    BenchSuite.run[__functions_in_module()](num_repetitions=3)

"""Benchmarks — `pixi run bench`.

The headline number is footer parsing: a Parquet file with 1,000 columns and
50 row groups has 50,000 `ColumnChunk`s in its footer, which is where every
reader spends its first few milliseconds. The footer here is synthesised in
Mojo with the same shape and field population pyarrow writes for such a file
(measured against a real one — see the README), so the benchmark needs no
multi-megabyte fixture in the repository.
"""

from std.time import perf_counter_ns

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


def synth_footer(columns: Int, row_groups: Int) raises -> FileMetaData:
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


def ms(ns: Int) -> String:
    var whole = ns // 1000000
    var frac = (ns % 1000000) // 1000
    var pad = String()
    if frac < 100:
        pad += "0"
    if frac < 10:
        pad += "0"
    return String(whole, ".", pad, frac, " ms")


def bench_footer(columns: Int, row_groups: Int, rounds: Int) raises:
    var meta = synth_footer(columns, row_groups)
    var body = write_footer(meta)
    var chunks = columns * row_groups

    var t0 = perf_counter_ns()
    for _ in range(rounds):
        var again = read_footer_bytes(Span(body))
        if len(again.row_groups) != row_groups:
            raise Error(String("bad decode"))
    var t1 = perf_counter_ns()
    var per_read = (t1 - t0) // rounds

    var t2 = perf_counter_ns()
    for _ in range(rounds):
        var out = write_footer(meta)
        if len(out) != len(body):
            raise Error(String("bad encode"))
        _ = out^
    var t3 = perf_counter_ns()
    var per_write = (t3 - t2) // rounds

    print(
        String(
            columns, " columns x ", row_groups, " row groups = ", chunks,
            " column chunks, footer ", len(body) // 1024, " KiB",
        )
    )
    print(String("  read_footer  ", ms(per_read), "  (", chunks * 1000000000 // (per_read if per_read > 0 else 1), " chunks/s)"))
    print(String("  write_footer ", ms(per_write)))
    _ = body^


def bench_skip(rounds: Int) raises:
    """How fast the recursive skipper steps over a footer it ignores."""
    var meta = synth_footer(100, 10)
    var body = write_footer(meta)
    var t0 = perf_counter_ns()
    for _ in range(rounds):
        var r = TCompactProtocolReader(Span(body))
        r.skip(T_STRUCT)
        if r.remaining() != 0:
            raise Error(String("skip did not consume the footer"))
    var t1 = perf_counter_ns()
    var per = (t1 - t0) // rounds
    print(
        String(
            "skip over a 100 x 10 footer (", len(body) // 1024, " KiB): ",
            ms(per),
            "  (",
            len(body) * 1000 // (per if per > 0 else 1),
            " MB/s)",
        )
    )
    _ = body^


def bench_primitives(rounds: Int) raises:
    var w = TCompactProtocolWriter()
    for i in range(10000):
        w.write_i64(Int64(i) * Int64(-7919))
    var buf = w^.take()
    var t0 = perf_counter_ns()
    for _ in range(rounds):
        var r = TCompactProtocolReader(Span(buf))
        var acc = Int64(0)
        for _ in range(10000):
            acc += r.read_i64()
        if acc == Int64(1):
            raise Error(String("impossible"))
    var t1 = perf_counter_ns()
    var per = (t1 - t0) // rounds
    print(
        String(
            "10,000 zigzag i64 varints: ", ms(per), "  (",
            10000 * 1000 // (per if per > 0 else 1), " M values/s)",
        )
    )
    _ = buf^


def main() raises:
    print("thrift.mojo benchmarks")
    print("")
    bench_footer(1000, 50, 3)
    print("")
    bench_footer(10, 1, 200)
    print("")
    bench_skip(20)
    bench_primitives(200)

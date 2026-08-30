"""The thrift.mojo test suite — run with `pixi run test`."""

from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)

from thrift.protocol import (
    MAX_SKIP_DEPTH,
    MESSAGE_CALL,
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_I16,
    T_I32,
    T_I64,
    T_LIST,
    T_MAP,
    T_SET,
    T_STOP,
    T_STRING,
    T_STRUCT,
    T_UUID,
    TBinaryProtocolReader,
    TBinaryProtocolWriter,
    TCompactProtocolReader,
    TCompactProtocolWriter,
    type_name,
    unzigzag_i64,
    zigzag_i32,
    zigzag_i64,
)

from thrift.parquet_footer import (
    FOOTER_TRAILER_SIZE,
    footer_length,
    read_footer,
    read_footer_bytes,
    read_page_header,
    read_parquet_file,
    write_footer,
    write_footer_trailer,
)
from thrift.parquet_types import (
    BloomFilterHeader,
    ColumnIndex,
    CompressionCodec,
    ConvertedType,
    Encoding,
    FileMetaData,
    LogicalType,
    OffsetIndex,
    PageType,
    SchemaElement,
    Type,
)
from parquet_expect import oracle_text

from vectors import (
    VECTOR_COUNT,
    binary_hex,
    compact_hex,
    hex_of,
    inf_f64,
    is_nan_f64,
    nan_f64,
    replay,
    unhex,
    vector_doc,
    vector_name,
    verify,
)


# ── vectors: byte-for-byte against Apache Thrift's Python runtime ──────────


def test_compact_vectors_encode() raises:
    for i in range(VECTOR_COUNT):
        var w = TCompactProtocolWriter()
        replay(w, i)
        var got = hex_of(Span(w.out))
        var want = compact_hex(i)
        if got != want:
            raise Error(
                String(
                    "compact vector '",
                    vector_name(i),
                    "' (",
                    vector_doc(i),
                    ")\n  got  ",
                    got,
                    "\n  want ",
                    want,
                )
            )


def test_binary_vectors_encode() raises:
    for i in range(VECTOR_COUNT):
        var w = TBinaryProtocolWriter()
        replay(w, i)
        var got = hex_of(Span(w.out))
        var want = binary_hex(i)
        if got != want:
            raise Error(
                String(
                    "binary vector '",
                    vector_name(i),
                    "' (",
                    vector_doc(i),
                    ")\n  got  ",
                    got,
                    "\n  want ",
                    want,
                )
            )


def test_compact_vectors_decode() raises:
    for i in range(VECTOR_COUNT):
        var buf = unhex(compact_hex(i))
        var r = TCompactProtocolReader(Span(buf))
        verify(r, i)
        assert_equal(r.remaining(), 0, String("leftover in ", vector_name(i)))
        _ = buf^


def test_binary_vectors_decode() raises:
    for i in range(VECTOR_COUNT):
        var buf = unhex(binary_hex(i))
        var r = TBinaryProtocolReader(Span(buf))
        verify(r, i)
        assert_equal(r.remaining(), 0, String("leftover in ", vector_name(i)))
        _ = buf^


def test_compact_vectors_skip() raises:
    """`skip(T_STRUCT)` must step over every vector exactly."""
    for i in range(VECTOR_COUNT):
        var buf = unhex(compact_hex(i))
        var r = TCompactProtocolReader(Span(buf))
        r.skip(T_STRUCT)
        assert_equal(r.remaining(), 0, String("skip left bytes in ", vector_name(i)))
        _ = buf^


def test_binary_vectors_skip() raises:
    for i in range(VECTOR_COUNT):
        var buf = unhex(binary_hex(i))
        var r = TBinaryProtocolReader(Span(buf))
        r.skip(T_STRUCT)
        assert_equal(r.remaining(), 0, String("skip left bytes in ", vector_name(i)))
        _ = buf^


def test_round_trip_through_our_own_writers() raises:
    """Write with ours, read with ours, for both protocols."""
    for i in range(VECTOR_COUNT):
        var cw = TCompactProtocolWriter()
        replay(cw, i)
        var cbuf = cw^.take()
        var cr = TCompactProtocolReader(Span(cbuf))
        verify(cr, i)
        _ = cbuf^

        var bw = TBinaryProtocolWriter()
        replay(bw, i)
        var bbuf = bw^.take()
        var br = TBinaryProtocolReader(Span(bbuf))
        verify(br, i)
        _ = bbuf^


# ── zigzag ─────────────────────────────────────────────────────────────────


def test_zigzag() raises:
    assert_equal(zigzag_i32(Int32(0)), UInt64(0))
    assert_equal(zigzag_i32(Int32(-1)), UInt64(1))
    assert_equal(zigzag_i32(Int32(1)), UInt64(2))
    assert_equal(zigzag_i32(Int32(-2)), UInt64(3))
    assert_equal(zigzag_i32(Int32(2147483647)), UInt64(4294967294))
    assert_equal(zigzag_i32(Int32(-2147483648)), UInt64(4294967295))
    assert_equal(zigzag_i64(Int64(0)), UInt64(0))
    assert_equal(zigzag_i64(Int64(-1)), UInt64(1))
    assert_equal(
        zigzag_i64(Int64(9223372036854775807)), UInt64(18446744073709551614)
    )
    assert_equal(
        zigzag_i64(Int64(-9223372036854775807) - Int64(1)),
        UInt64(18446744073709551615),
    )
    # Round trip over a wide sweep.
    var v = Int64(1)
    for _ in range(63):
        assert_equal(unzigzag_i64(zigzag_i64(v)), v)
        assert_equal(unzigzag_i64(zigzag_i64(-v)), -v)
        v *= 2


def test_type_name() raises:
    assert_equal(type_name(T_STOP), String("STOP"))
    assert_equal(type_name(T_LIST), String("LIST"))
    assert_equal(type_name(T_UUID), String("UUID"))
    assert_equal(type_name(Int8(99)), String("type(99)"))


# ── messages (protocol id / version framing) ───────────────────────────────


def test_compact_message_round_trip() raises:
    var w = TCompactProtocolWriter()
    w.write_message_begin("ping", MESSAGE_CALL, Int32(7))
    w.write_struct_begin()
    w.write_field_stop()
    w.write_struct_end()
    w.write_message_end()
    var buf = w^.take()
    assert_equal(Int(buf[0]), 0x82)
    var r = TCompactProtocolReader(Span(buf))
    var head = r.read_message_begin()
    assert_equal(head[0], String("ping"))
    assert_equal(Int(head[1]), Int(MESSAGE_CALL))
    assert_equal(Int(head[2]), 7)
    _ = buf^


def test_compact_bad_protocol_id() raises:
    var buf: List[UInt8] = [0x81, 0x21, 0x00, 0x00]
    var r = TCompactProtocolReader(Span(buf))
    with assert_raises(contains="bad protocol id"):
        _ = r.read_message_begin()
    _ = buf^


def test_compact_bad_version() raises:
    var buf: List[UInt8] = [0x82, 0x22, 0x00, 0x00]
    var r = TCompactProtocolReader(Span(buf))
    with assert_raises(contains="unsupported version"):
        _ = r.read_message_begin()
    _ = buf^


def test_binary_message_round_trip() raises:
    var w = TBinaryProtocolWriter()
    w.write_message_begin("ping", MESSAGE_CALL, Int32(-3))
    w.write_message_end()
    var buf = w^.take()
    assert_equal(Int(buf[0]), 0x80)
    assert_equal(Int(buf[1]), 0x01)
    var r = TBinaryProtocolReader(Span(buf))
    var head = r.read_message_begin()
    assert_equal(head[0], String("ping"))
    assert_equal(Int(head[1]), Int(MESSAGE_CALL))
    assert_equal(Int(head[2]), -3)
    _ = buf^


def test_binary_non_strict_message() raises:
    var w = TBinaryProtocolWriter(strict_write=False)
    w.write_message_begin("ping", MESSAGE_CALL, Int32(1))
    var buf = w^.take()
    # A strict reader must refuse it...
    var strict = TBinaryProtocolReader(Span(buf))
    with assert_raises(contains="missing version prefix"):
        _ = strict.read_message_begin()
    # ...and a lenient one must accept it.
    var lenient = TBinaryProtocolReader(Span(buf), strict_read=False)
    var head = lenient.read_message_begin()
    assert_equal(head[0], String("ping"))
    assert_equal(Int(head[2]), 1)
    _ = buf^


def test_binary_bad_version() raises:
    var buf: List[UInt8] = [0x80, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]
    var r = TBinaryProtocolReader(Span(buf))
    with assert_raises(contains="bad version"):
        _ = r.read_message_begin()
    _ = buf^


# ── hostile inputs ─────────────────────────────────────────────────────────


def test_truncated_compact_struct() raises:
    """Every prefix of a valid struct must raise rather than read past the end."""
    var full = unhex(compact_hex(VECTOR_COUNT - 1))
    for cut in range(1, len(full)):
        var part = List[UInt8](capacity=cut)
        part.extend(Span(full)[0:cut])
        var r = TCompactProtocolReader(Span(part))
        var raised = False
        try:
            r.skip(T_STRUCT)
            # A prefix may also simply run out of bytes mid-struct without
            # a raise only if it happened to be complete, which it is not.
            raised = r.remaining() != 0
        except:
            raised = True
        assert_true(raised, String("prefix of length ", cut, " did not raise"))
        _ = part^
    _ = full^


def test_truncated_binary_struct() raises:
    var full = unhex(binary_hex(VECTOR_COUNT - 1))
    for cut in range(1, len(full)):
        var part = List[UInt8](capacity=cut)
        part.extend(Span(full)[0:cut])
        var r = TBinaryProtocolReader(Span(part))
        var raised = False
        try:
            r.skip(T_STRUCT)
            raised = r.remaining() != 0
        except:
            raised = True
        assert_true(raised, String("prefix of length ", cut, " did not raise"))
        _ = part^
    _ = full^


def test_compact_huge_list_size_rejected() raises:
    """A list header claiming 2^31 elements must not make us allocate."""
    # field 1, type LIST(9), long size form 0xF? then varint 0xFFFFFFFF07.
    var buf: List[UInt8] = [0x19, 0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0x07]
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    var head = r.read_field_begin()
    assert_equal(Int(head[0]), Int(T_LIST))
    with assert_raises(contains="exceeds"):
        _ = r.read_list_begin()
    _ = buf^


def test_compact_huge_binary_length_rejected() raises:
    var buf: List[UInt8] = [0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0x07]
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    _ = r.read_field_begin()
    with assert_raises(contains="exceeds"):
        _ = r.read_binary()
    _ = buf^


def test_compact_huge_map_size_rejected() raises:
    var buf: List[UInt8] = [0x1B, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x88]
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    _ = r.read_field_begin()
    with assert_raises(contains="exceeds"):
        _ = r.read_map_begin()
    _ = buf^


def test_binary_huge_list_size_rejected() raises:
    var buf: List[UInt8] = [
        0x0F, 0x00, 0x01, 0x08, 0x7F, 0xFF, 0xFF, 0xFF,
    ]
    var r = TBinaryProtocolReader(Span(buf))
    r.read_struct_begin()
    _ = r.read_field_begin()
    with assert_raises(contains="exceeds"):
        _ = r.read_list_begin()
    _ = buf^


def test_binary_negative_length_rejected() raises:
    var buf: List[UInt8] = [
        0x0B, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF,
    ]
    var r = TBinaryProtocolReader(Span(buf))
    r.read_struct_begin()
    _ = r.read_field_begin()
    with assert_raises(contains="negative length"):
        _ = r.read_string()
    _ = buf^


def test_compact_unknown_type_id_rejected() raises:
    var buf: List[UInt8] = [0x1E, 0x00]
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    with assert_raises(contains="unknown type id"):
        _ = r.read_field_begin()
    _ = buf^


def test_compact_deep_nesting_rejected() raises:
    """A tower of struct headers must be refused, not overflow the stack."""
    var buf = List[UInt8]()
    for _ in range(MAX_SKIP_DEPTH + 10):
        buf.append(0x1C)  # field 1, type STRUCT
    for _ in range(MAX_SKIP_DEPTH + 10):
        buf.append(0x00)
    var r = TCompactProtocolReader(Span(buf))
    with assert_raises():
        r.skip(T_STRUCT)
    _ = buf^


def test_varint_too_long_rejected() raises:
    var buf = List[UInt8]()
    buf.append(0x15)  # field 1, type I32
    for _ in range(12):
        buf.append(0xFF)
    buf.append(0x00)
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    _ = r.read_field_begin()
    with assert_raises(contains="varint too long"):
        _ = r.read_i32()
    _ = buf^


def test_struct_end_without_begin() raises:
    var buf = List[UInt8]()
    var r = TCompactProtocolReader(Span(buf))
    with assert_raises(contains="struct end without begin"):
        r.read_struct_end()
    _ = buf^


# ── skip over unknown fields ───────────────────────────────────────────────


def test_skip_unknown_fields_in_a_struct() raises:
    """Forward compatibility: an old reader steps over new fields."""
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_I32, 1)
    w.write_i32(Int32(11))
    w.write_field_end()
    # A field the "old" reader does not know: a map of lists of structs.
    w.write_field_begin(T_MAP, 2)
    w.write_map_begin(T_STRING, T_LIST, 1)
    w.write_string("k")
    w.write_list_begin(T_STRUCT, 2)
    for i in range(2):
        w.write_struct_begin()
        w.write_field_begin(T_DOUBLE, 1)
        w.write_double(Float64(i))
        w.write_field_end()
        w.write_field_begin(T_SET, 2)
        w.write_set_begin(T_BOOL, 3)
        w.write_bool(True)
        w.write_bool(False)
        w.write_bool(True)
        w.write_set_end()
        w.write_field_end()
        w.write_field_stop()
        w.write_struct_end()
    w.write_list_end()
    w.write_map_end()
    w.write_field_end()
    w.write_field_begin(T_I64, 3)
    w.write_i64(Int64(33))
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()

    var r = TCompactProtocolReader(Span(buf))
    var saw_1 = Int32(0)
    var saw_3 = Int64(0)
    r.read_struct_begin()
    while True:
        var head = r.read_field_begin()
        if head[0] == T_STOP:
            break
        if head[1] == 1:
            saw_1 = r.read_i32()
        elif head[1] == 3:
            saw_3 = r.read_i64()
        else:
            r.skip(head[0])
        r.read_field_end()
    r.read_struct_end()
    assert_equal(saw_1, Int32(11))
    assert_equal(saw_3, Int64(33))
    assert_equal(r.remaining(), 0)
    _ = buf^


def test_field_id_delta_boundaries() raises:
    """A delta of exactly 15 uses the short form; 16 falls back to the long one."""
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_BYTE, 15)
    w.write_byte(Int8(1))
    w.write_field_end()
    w.write_field_begin(T_BYTE, 30)
    w.write_byte(Int8(2))
    w.write_field_end()
    w.write_field_begin(T_BYTE, 46)
    w.write_byte(Int8(3))
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()
    # 15 - 0 = 15 → short; 30 - 15 = 15 → short; 46 - 30 = 16 → long.
    assert_equal(hex_of(Span(buf)), String("f301f302035c0300"))
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    var ids = List[Int]()
    while True:
        var head = r.read_field_begin()
        if head[0] == T_STOP:
            break
        ids.append(Int(head[1]))
        _ = r.read_byte()
        r.read_field_end()
    r.read_struct_end()
    assert_equal(len(ids), 3)
    assert_equal(ids[0], 15)
    assert_equal(ids[1], 30)
    assert_equal(ids[2], 46)
    _ = buf^


def test_negative_and_decreasing_field_ids() raises:
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_I32, 100)
    w.write_i32(Int32(1))
    w.write_field_end()
    w.write_field_begin(T_I32, 5)
    w.write_i32(Int32(2))
    w.write_field_end()
    w.write_field_begin(T_I32, -7)
    w.write_i32(Int32(3))
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()
    var r = TCompactProtocolReader(Span(buf))
    r.read_struct_begin()
    var ids = List[Int]()
    while True:
        var head = r.read_field_begin()
        if head[0] == T_STOP:
            break
        ids.append(Int(head[1]))
        _ = r.read_i32()
        r.read_field_end()
    r.read_struct_end()
    assert_equal(ids[0], 100)
    assert_equal(ids[1], 5)
    assert_equal(ids[2], -7)
    _ = buf^


def test_double_bit_patterns() raises:
    """-0.0 and NaN must survive the round trip bit-for-bit."""
    var w = TCompactProtocolWriter()
    w.write_double(-0.0)
    w.write_double(nan_f64())
    w.write_double(inf_f64())
    var buf = w^.take()
    # Compact writes doubles little-endian.
    assert_equal(hex_of(Span(buf)[0:8]), String("0000000000000080"))
    var r = TCompactProtocolReader(Span(buf))
    var neg_zero = r.read_double()
    assert_equal(neg_zero, Float64(0.0))
    assert_true(Float64(1.0) / neg_zero < Float64(0.0), "sign of -0.0 lost")
    assert_true(is_nan_f64(r.read_double()))
    assert_equal(r.read_double(), inf_f64())
    _ = buf^


def test_binary_double_is_big_endian() raises:
    var w = TBinaryProtocolWriter()
    w.write_double(-0.0)
    var buf = w^.take()
    assert_equal(hex_of(Span(buf)), String("8000000000000000"))
    _ = buf^


def test_empty_collections() raises:
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_LIST, 1)
    w.write_list_begin(T_I32, 0)
    w.write_list_end()
    w.write_field_end()
    w.write_field_begin(T_MAP, 2)
    w.write_map_begin(T_I32, T_I32, 0)
    w.write_map_end()
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()
    # An empty map is a single 0x00 byte, with no key/value type byte.
    assert_equal(hex_of(Span(buf)), String("19051b0000"))
    var r = TCompactProtocolReader(Span(buf))
    r.skip(T_STRUCT)
    assert_equal(r.remaining(), 0)
    _ = buf^


# ── Parquet: real files, checked against Apache Thrift + pyarrow ───────────


def fixture_names() -> List[String]:
    return [
        String("primitives"),
        String("logical"),
        String("extension"),
        String("nested"),
        String("encodings"),
        String("codecs"),
        String("pageindex"),
        String("nostats"),
        String("v2pages"),
    ]


def fixture_path(name: StringSlice) -> String:
    return String("tests/fixtures/", name, ".parquet")


def read_text(path: StringSlice) raises -> String:
    with open(String(path), "r") as f:
        return f.read()


def diff_report(name: StringSlice, got: String, want: String) raises -> String:
    var gl = got.split("\n")
    var wl = want.split("\n")
    var out = String("fixture '", name, "' does not match its oracle\n")
    var n = len(gl) if len(gl) < len(wl) else len(wl)
    var shown = 0
    for i in range(n):
        if gl[i] != wl[i]:
            out += String("  line ", i + 1, "\n    got  ", gl[i], "\n    want ", wl[i], "\n")
            shown += 1
            if shown >= 8:
                break
    if len(gl) != len(wl):
        out += String("  line counts differ: ", len(gl), " vs ", len(wl), "\n")
    return out^


def test_fixtures_match_the_oracle() raises:
    """Our decode must reproduce the Apache-Thrift-derived oracle exactly."""
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        var got = oracle_text(meta)
        var want = read_text(String(path, ".oracle.txt"))
        if got != want:
            raise Error(diff_report(name, got, want))
        _ = data^


def test_footer_round_trip_is_semantically_identical() raises:
    """write_footer(read_footer(x)) must decode back to the same metadata."""
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        var before = oracle_text(meta)
        var body = write_footer(meta)
        var again = read_footer_bytes(Span(body))
        var after = oracle_text(again)
        if before != after:
            raise Error(diff_report(name, after, before))
        # And once more, to prove the re-serialisation is a fixed point.
        var body2 = write_footer(again)
        assert_equal(
            hex_of(Span(body)),
            hex_of(Span(body2)),
            String("re-serialising ", name, " is not idempotent"),
        )
        _ = data^
        _ = body^
        _ = body2^


def test_footer_round_trip_through_a_whole_file() raises:
    """Rebuild the 8-byte trailer too and re-read the file end to end."""
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        var body = write_footer(meta)
        var rebuilt = List[UInt8]()
        var keep = len(data) - FOOTER_TRAILER_SIZE - footer_length(Span(data))
        rebuilt.extend(Span(data)[0:keep])
        rebuilt.extend(Span(body))
        write_footer_trailer(rebuilt, len(body))
        var again = read_footer(Span(rebuilt))
        assert_equal(oracle_text(again), oracle_text(meta))
        _ = data^
        _ = body^
        _ = rebuilt^


def test_page_headers_of_every_page_in_every_fixture() raises:
    """Walk every page of every chunk and reconcile with the chunk metadata."""
    var total_pages = 0
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        for r in range(len(meta.row_groups)):
            ref rg = meta.row_groups[r]
            for j in range(len(rg.columns)):
                ref cm = rg.columns[j].meta_data.value()
                var start = cm.data_page_offset
                if cm.dictionary_page_offset:
                    var d = cm.dictionary_page_offset.value()
                    if d < start:
                        start = d
                var pos = Int(start)
                var stop = Int(start + cm.total_compressed_size)
                var data_values = Int64(0)
                var n_data = 0
                var n_dict = 0
                while pos < stop:
                    var page = read_page_header(Span(data), pos)
                    total_pages += 1
                    var header_len = page[1]
                    ref h = page[0]
                    if h.type_ == PageType.DICTIONARY_PAGE:
                        n_dict += 1
                        assert_true(
                            Bool(h.dictionary_page_header),
                            String("dictionary page without its header in ", name),
                        )
                    elif h.type_ == PageType.DATA_PAGE:
                        n_data += 1
                        ref dh = h.data_page_header.value()
                        data_values += Int64(dh.num_values)
                    elif h.type_ == PageType.DATA_PAGE_V2:
                        n_data += 1
                        ref dh2 = h.data_page_header_v2.value()
                        data_values += Int64(dh2.num_values)
                    assert_true(
                        h.compressed_page_size >= 0
                        and h.uncompressed_page_size >= 0,
                        String("negative page size in ", name),
                    )
                    pos += header_len + Int(h.compressed_page_size)
                assert_equal(
                    pos,
                    stop,
                    String(
                        "pages of ", name, " rg", r, " col", j,
                        " do not tile total_compressed_size",
                    ),
                )
                assert_equal(
                    data_values,
                    cm.num_values,
                    String(
                        "data page num_values in ", name, " rg", r, " col", j,
                        " do not sum to the chunk's num_values",
                    ),
                )
                assert_equal(
                    n_dict,
                    1 if cm.dictionary_page_offset else 0,
                    String("dictionary page count in ", name),
                )
                assert_true(n_data > 0, String("no data pages in ", name))
        _ = data^
    assert_true(total_pages >= 100, String("only saw ", total_pages, " pages"))


def page_kind(t: PageType) raises -> String:
    """DATA_PAGE and DATA_PAGE_V2 count as one kind here.

    parquet-cpp records `page_type = DATA_PAGE` in `encoding_stats` even for
    a chunk it wrote as v2 pages, so comparing the raw enum would fail on a
    writer quirk rather than on anything we decoded wrong.
    """
    if t == PageType.DATA_PAGE or t == PageType.DATA_PAGE_V2:
        return String("DATA")
    return t.name()


def test_page_counts_match_encoding_stats() raises:
    """`ColumnMetaData.encoding_stats` must agree with the pages on disk."""
    var checked = 0
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        for r in range(len(meta.row_groups)):
            ref rg = meta.row_groups[r]
            for j in range(len(rg.columns)):
                ref cm = rg.columns[j].meta_data.value()
                if not cm.encoding_stats:
                    continue
                ref stats = cm.encoding_stats.value()
                var start = cm.data_page_offset
                if cm.dictionary_page_offset:
                    var d = cm.dictionary_page_offset.value()
                    if d < start:
                        start = d
                var pos = Int(start)
                var stop = Int(start + cm.total_compressed_size)
                var keys = List[String]()
                var counts = List[Int]()
                while pos < stop:
                    var page = read_page_header(Span(data), pos)
                    ref h = page[0]
                    var enc = Encoding(0)
                    if h.data_page_header:
                        enc = h.data_page_header.value().encoding
                    elif h.data_page_header_v2:
                        enc = h.data_page_header_v2.value().encoding
                    elif h.dictionary_page_header:
                        enc = h.dictionary_page_header.value().encoding
                    var key = String(page_kind(h.type_), "/", enc.name())
                    var found = False
                    for k in range(len(keys)):
                        if keys[k] == key:
                            counts[k] += 1
                            found = True
                            break
                    if not found:
                        keys.append(key^)
                        counts.append(1)
                    pos += page[1] + Int(h.compressed_page_size)
                assert_equal(
                    len(stats),
                    len(keys),
                    String("encoding_stats arity in ", name),
                )
                for s in range(len(stats)):
                    var key = String(
                        page_kind(stats[s].page_type), "/",
                        stats[s].encoding.name(),
                    )
                    var got = -1
                    for k in range(len(keys)):
                        if keys[k] == key:
                            got = counts[k]
                            break
                    assert_equal(
                        got,
                        Int(stats[s].count),
                        String("encoding_stats ", key, " in ", name),
                    )
                    checked += 1
        _ = data^
    assert_true(checked > 0, "no encoding_stats were checked")


def test_page_index_structures() raises:
    """OffsetIndex and ColumnIndex decode and describe the same pages."""
    var path = fixture_path("pageindex")
    var data = read_parquet_file(path)
    var meta = read_footer(Span(data))
    var chunks_with_index = 0
    for r in range(len(meta.row_groups)):
        ref rg = meta.row_groups[r]
        for j in range(len(rg.columns)):
            ref cc = rg.columns[j]
            if not cc.offset_index_offset or not cc.column_index_offset:
                continue
            chunks_with_index += 1
            var oi_r = TCompactProtocolReader(
                Span(data), Int(cc.offset_index_offset.value())
            )
            var oi = OffsetIndex()
            oi.read(oi_r)
            assert_equal(
                oi_r.pos - Int(cc.offset_index_offset.value()),
                Int(cc.offset_index_length.value()),
                "OffsetIndex length disagrees with the ColumnChunk",
            )
            var ci_r = TCompactProtocolReader(
                Span(data), Int(cc.column_index_offset.value())
            )
            var ci = ColumnIndex()
            ci.read(ci_r)
            assert_equal(
                ci_r.pos - Int(cc.column_index_offset.value()),
                Int(cc.column_index_length.value()),
                "ColumnIndex length disagrees with the ColumnChunk",
            )
            assert_equal(
                len(ci.null_pages),
                len(oi.page_locations),
                "ColumnIndex and OffsetIndex disagree on the page count",
            )
            assert_equal(len(ci.min_values), len(ci.null_pages))
            assert_equal(len(ci.max_values), len(ci.null_pages))
            # Every page location must point at a real page header whose
            # size matches.
            ref cm = cc.meta_data.value()
            var n_data = 0
            var pos = Int(cm.data_page_offset)
            var stop = Int(cm.data_page_offset + cm.total_compressed_size)
            if cm.dictionary_page_offset:
                pos = Int(cm.dictionary_page_offset.value())
                stop = pos + Int(cm.total_compressed_size)
            var rows = Int64(0)
            while pos < stop:
                var page = read_page_header(Span(data), pos)
                ref h = page[0]
                if h.type_ != PageType.DICTIONARY_PAGE:
                    assert_equal(
                        Int64(oi.page_locations[n_data].offset),
                        Int64(pos),
                        "page location offset",
                    )
                    assert_equal(
                        Int64(oi.page_locations[n_data].compressed_page_size),
                        Int64(page[1] + Int(h.compressed_page_size)),
                        "page location size",
                    )
                    assert_equal(
                        oi.page_locations[n_data].first_row_index,
                        rows,
                        "page location first_row_index",
                    )
                    rows += Int64(h.data_page_header.value().num_values)
                    n_data += 1
                pos += page[1] + Int(h.compressed_page_size)
            assert_equal(n_data, len(oi.page_locations))
    assert_true(chunks_with_index > 0, "no page index found in the fixture")
    _ = data^


def test_bloom_filter_header_if_present() raises:
    var found = 0
    for name in fixture_names():
        var path = fixture_path(name)
        var data = read_parquet_file(path)
        var meta = read_footer(Span(data))
        for r in range(len(meta.row_groups)):
            ref rg = meta.row_groups[r]
            for j in range(len(rg.columns)):
                ref cm = rg.columns[j].meta_data.value()
                if not cm.bloom_filter_offset:
                    continue
                found += 1
                var r2 = TCompactProtocolReader(
                    Span(data), Int(cm.bloom_filter_offset.value())
                )
                var bh = BloomFilterHeader()
                bh.read(r2)
                assert_true(bh.numBytes > 0, "empty bloom filter")
                assert_true(Bool(bh.algorithm.BLOCK), "unexpected bloom algorithm")
                assert_true(Bool(bh.hash.XXHASH), "unexpected bloom hash")
                assert_true(
                    Bool(bh.compression.UNCOMPRESSED),
                    "unexpected bloom compression",
                )
        _ = data^
    assert_true(
        found > 0,
        "no fixture carries a bloom filter — regenerate them with a pyarrow"
        " that supports bloom_filter_options",
    )


# ── footer framing errors ──────────────────────────────────────────────────


def test_footer_rejects_short_file() raises:
    var buf: List[UInt8] = [80, 65, 82, 49]
    with assert_raises(contains="too small"):
        _ = read_footer(Span(buf))
    _ = buf^


def test_footer_rejects_bad_magic() raises:
    var data = read_parquet_file(fixture_path("primitives"))
    var bad = data.copy()
    bad[len(bad) - 1] = UInt8(88)
    with assert_raises(contains="bad trailing magic"):
        _ = read_footer(Span(bad))
    var bad2 = data.copy()
    bad2[0] = UInt8(88)
    with assert_raises(contains="bad leading magic"):
        _ = read_footer(Span(bad2))
    _ = data^
    _ = bad^
    _ = bad2^


def test_footer_rejects_encrypted() raises:
    var data = read_parquet_file(fixture_path("primitives"))
    var enc = data.copy()
    # PARE, both ends.
    enc[len(enc) - 1] = UInt8(69)
    with assert_raises(contains="encrypted footer unsupported"):
        _ = read_footer(Span(enc))
    _ = data^
    _ = enc^


def test_footer_rejects_impossible_length() raises:
    var data = read_parquet_file(fixture_path("primitives"))
    var bad = data.copy()
    var base = len(bad) - FOOTER_TRAILER_SIZE
    bad[base] = UInt8(0xFF)
    bad[base + 1] = UInt8(0xFF)
    bad[base + 2] = UInt8(0xFF)
    bad[base + 3] = UInt8(0x7F)
    with assert_raises(contains="does not fit"):
        _ = read_footer(Span(bad))
    _ = data^
    _ = bad^


def test_truncated_footer_body_raises() raises:
    var data = read_parquet_file(fixture_path("extension"))
    var n = footer_length(Span(data))
    var start = len(data) - FOOTER_TRAILER_SIZE - n
    for cut in range(1, n):
        var part = List[UInt8]()
        part.extend(Span(data)[start : start + cut])
        var raised = False
        try:
            _ = read_footer_bytes(Span(part))
        except:
            raised = True
        assert_true(raised, String("footer prefix of ", cut, " bytes decoded"))
        _ = part^
    _ = data^


# ── generated struct behaviour ─────────────────────────────────────────────


def test_required_field_missing_raises() raises:
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_I32, 1)
    w.write_i32(Int32(2))
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()
    var r = TCompactProtocolReader(Span(buf))
    var meta = FileMetaData()
    with assert_raises(contains="missing required field"):
        meta.read(r)
    _ = buf^


def test_unknown_fields_are_skipped() raises:
    """A FileMetaData with a field id from the future still decodes."""
    var meta = FileMetaData()
    meta.version = 2
    meta.num_rows = 7
    var se = SchemaElement()
    se.name = String("schema")
    meta.schema.append(se^)
    var body = write_footer(meta)
    # Splice an unknown struct-typed field 99 in before the stop byte.
    var spliced = List[UInt8]()
    spliced.extend(Span(body)[0 : len(body) - 1])
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_begin(T_STRUCT, 99)
    w.write_struct_begin()
    w.write_field_begin(T_LIST, 1)
    w.write_list_begin(T_DOUBLE, 2)
    w.write_double(1.0)
    w.write_double(2.0)
    w.write_list_end()
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    w.write_field_end()
    w.write_field_stop()
    w.write_struct_end()
    var tail = w^.take()
    spliced.extend(Span(tail))
    var back = read_footer_bytes(Span(spliced))
    assert_equal(back.num_rows, Int64(7))
    assert_equal(back.schema[0].name, String("schema"))
    _ = body^
    _ = tail^
    _ = spliced^


def test_union_requires_exactly_one_member() raises:
    var w = TCompactProtocolWriter()
    w.write_struct_begin()
    w.write_field_stop()
    w.write_struct_end()
    var buf = w^.take()
    var r = TCompactProtocolReader(Span(buf))
    var lt = LogicalType()
    with assert_raises(contains="exactly one member"):
        lt.read(r)
    _ = buf^


def test_enum_names_and_open_values() raises:
    assert_equal(Type.BOOLEAN.name(), String("BOOLEAN"))
    assert_equal(Type.FIXED_LEN_BYTE_ARRAY.name(), String("FIXED_LEN_BYTE_ARRAY"))
    assert_equal(CompressionCodec.LZ4_RAW.name(), String("LZ4_RAW"))
    assert_equal(Encoding.BYTE_STREAM_SPLIT.name(), String("BYTE_STREAM_SPLIT"))
    assert_equal(PageType.DATA_PAGE_V2.name(), String("DATA_PAGE_V2"))
    assert_equal(ConvertedType.UTF8.name(), String("UTF8"))
    # An enum value the IDL does not define round-trips rather than raising.
    assert_equal(CompressionCodec(99).name(), String("CompressionCodec(99)"))


def test_struct_round_trip_over_binary_protocol_too() raises:
    """The generated code is protocol-agnostic; prove it on TBinaryProtocol."""
    var data = read_parquet_file(fixture_path("nested"))
    var meta = read_footer(Span(data))
    var w = TBinaryProtocolWriter()
    meta.write(w)
    var buf = w^.take()
    var r = TBinaryProtocolReader(Span(buf))
    var back = FileMetaData()
    back.read(r)
    assert_equal(oracle_text(back), oracle_text(meta))
    _ = data^
    _ = buf^


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()

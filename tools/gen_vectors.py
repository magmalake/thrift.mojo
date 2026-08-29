#!/usr/bin/env python3
"""Generate tests/vectors.mojo — Thrift wire vectors produced by Apache's own
Python runtime.

Each vector is a little *program* of protocol calls. The program is run
through `thrift.protocol.TCompactProtocol` and `TBinaryProtocol` over a
`TMemoryBuffer` (the generated-code-free `TProtocolBase` API, so no IDL
compiler is needed) to get the reference bytes, and the very same program is
emitted as Mojo so the test can replay it through our writers and compare,
then read it back through our readers and compare the values.

Run:  pip install thrift && python3 tools/gen_vectors.py > tests/vectors.mojo
"""

import sys
import uuid as _uuid

from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TProtocol import TType
from thrift.transport.TTransport import TMemoryBuffer

I8_MIN, I8_MAX = -128, 127
I16_MIN, I16_MAX = -32768, 32767
I32_MIN, I32_MAX = -2147483648, 2147483647
I64_MIN, I64_MAX = -(2**63), 2**63 - 1

UUID_A = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
UUID_B = "00000000-0000-0000-0000-000000000000"

# ── the vector programs ────────────────────────────────────────────────────
#
# ops are (kind, *args). `struct`/`field`/`list`/`set`/`map` are the framing
# calls; the rest are values.

VECTORS = []


def vec(name, doc, ops):
    VECTORS.append((name, doc, ops))


def field(fid, ttype, *value_ops):
    return [("field_begin", ttype, fid), *value_ops, ("field_end",)]


vec(
    "i32_boundaries",
    "i32 at both ends of the range plus the zigzag-sensitive small values",
    [("struct_begin",)]
    + field(1, TType.I32, ("i32", 0))
    + field(2, TType.I32, ("i32", -1))
    + field(3, TType.I32, ("i32", 1))
    + field(4, TType.I32, ("i32", I32_MIN))
    + field(5, TType.I32, ("i32", I32_MAX))
    + field(6, TType.I32, ("i32", -12345))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "i64_boundaries",
    "i64 min/max and the 10-byte varint they force",
    [("struct_begin",)]
    + field(1, TType.I64, ("i64", 0))
    + field(2, TType.I64, ("i64", -1))
    + field(3, TType.I64, ("i64", I64_MIN))
    + field(4, TType.I64, ("i64", I64_MAX))
    + field(5, TType.I64, ("i64", 1 << 40))
    + field(6, TType.I64, ("i64", -(1 << 40)))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "small_ints",
    "i8 and i16 at their boundaries",
    [("struct_begin",)]
    + field(1, TType.BYTE, ("byte", I8_MIN))
    + field(2, TType.BYTE, ("byte", I8_MAX))
    + field(3, TType.BYTE, ("byte", -1))
    + field(4, TType.I16, ("i16", I16_MIN))
    + field(5, TType.I16, ("i16", I16_MAX))
    + field(6, TType.I16, ("i16", -300))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "doubles",
    "doubles including -0.0, infinities and a NaN",
    [("struct_begin",)]
    + field(1, TType.DOUBLE, ("double", 0.0))
    + field(2, TType.DOUBLE, ("double", -0.0))
    + field(3, TType.DOUBLE, ("double", 3.141592653589793))
    + field(4, TType.DOUBLE, ("double", -2.2250738585072014e-308))
    + field(5, TType.DOUBLE, ("double", float("inf")))
    + field(6, TType.DOUBLE, ("double", float("-inf")))
    + field(7, TType.DOUBLE, ("double", float("nan")))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "strings_and_binary",
    "empty, ASCII, UTF-8 and raw binary payloads",
    [("struct_begin",)]
    + field(1, TType.STRING, ("string", ""))
    + field(2, TType.STRING, ("string", "parquet"))
    + field(3, TType.STRING, ("string", "é世界\U0001f525"))
    + field(4, TType.STRING, ("binary", "00ff7f8001"))
    + field(5, TType.STRING, ("string", "x" * 300))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "bools_as_fields",
    "bool field values, which the compact protocol folds into the type nibble",
    [("struct_begin",)]
    + field(1, TType.BOOL, ("bool", True))
    + field(2, TType.BOOL, ("bool", False))
    + field(3, TType.I32, ("i32", 7))
    + field(4, TType.BOOL, ("bool", True))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "bools_in_list",
    "bool list elements, which are a whole byte (1 true, 2 false)",
    [("struct_begin",)]
    + field(
        1,
        TType.LIST,
        ("list_begin", TType.BOOL, 5),
        ("bool", True),
        ("bool", False),
        ("bool", False),
        ("bool", True),
        ("bool", True),
        ("list_end",),
    )
    + [("field_stop",), ("struct_end",)],
)

vec(
    "field_id_deltas",
    "field ids that fit the 4-bit delta and ids that force the long form",
    [("struct_begin",)]
    + field(1, TType.I32, ("i32", 1))
    + field(2, TType.I32, ("i32", 2))
    + field(16, TType.I32, ("i32", 3))
    + field(17, TType.I32, ("i32", 4))
    + field(100, TType.I32, ("i32", 5))
    + field(4, TType.I32, ("i32", 6))
    + field(32767, TType.I32, ("i32", 7))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "nested_structs",
    "a struct inside a struct inside a struct, exercising the field-id stack",
    [("struct_begin",)]
    + field(1, TType.I32, ("i32", 11))
    + field(
        9,
        TType.STRUCT,
        ("struct_begin",),
        *field(3, TType.I32, ("i32", 22)),
        *field(
            20,
            TType.STRUCT,
            ("struct_begin",),
            *field(1, TType.STRING, ("string", "deep")),
            ("field_stop",),
            ("struct_end",),
        ),
        *field(4, TType.I32, ("i32", 33)),
        ("field_stop",),
        ("struct_end",),
    )
    + field(10, TType.I32, ("i32", 44))
    + [("field_stop",), ("struct_end",)],
)

vec(
    "list_of_structs",
    "a list whose elements are structs, plus an empty list and a long one",
    [("struct_begin",)]
    + field(
        1,
        TType.LIST,
        ("list_begin", TType.STRUCT, 3),
        *[
            op
            for i in (1, 2, 3)
            for op in (
                ("struct_begin",),
                *field(1, TType.I32, ("i32", i * 10)),
                *field(2, TType.STRING, ("string", "elem")),
                ("field_stop",),
                ("struct_end",),
            )
        ],
        ("list_end",),
    )
    + field(2, TType.LIST, ("list_begin", TType.I32, 0), ("list_end",))
    + field(
        3,
        TType.LIST,
        ("list_begin", TType.I32, 20),
        *[("i32", i * 1000) for i in range(20)],
        ("list_end",),
    )
    + [("field_stop",), ("struct_end",)],
)

vec(
    "maps_and_sets",
    "map headers (including the empty-map short circuit) and sets",
    [("struct_begin",)]
    + field(
        1,
        TType.MAP,
        ("map_begin", TType.STRING, TType.I64, 2),
        ("string", "alpha"),
        ("i64", 1),
        ("string", "beta"),
        ("i64", -2),
        ("map_end",),
    )
    + field(
        2,
        TType.MAP,
        ("map_begin", TType.I32, TType.I32, 0),
        ("map_end",),
    )
    + field(
        3,
        TType.SET,
        ("set_begin", TType.I32, 3),
        ("i32", 5),
        ("i32", 6),
        ("i32", 7),
        ("set_end",),
    )
    + field(
        4,
        TType.MAP,
        ("map_begin", TType.I32, TType.LIST, 1),
        ("i32", 9),
        ("list_begin", TType.STRING, 2),
        ("string", "a"),
        ("string", "b"),
        ("list_end",),
        ("map_end",),
    )
    + [("field_stop",), ("struct_end",)],
)

vec(
    "uuids",
    "the 16-byte UUID type",
    [("struct_begin",)]
    + field(1, TType.UUID, ("uuid", UUID_A))
    + field(2, TType.UUID, ("uuid", UUID_B))
    + field(
        3,
        TType.LIST,
        ("list_begin", TType.UUID, 2),
        ("uuid", UUID_A),
        ("uuid", UUID_B),
        ("list_end",),
    )
    + [("field_stop",), ("struct_end",)],
)

vec(
    "everything",
    "one struct touching every primitive and container at once",
    [("struct_begin",)]
    + field(1, TType.BOOL, ("bool", False))
    + field(2, TType.BYTE, ("byte", 42))
    + field(3, TType.I16, ("i16", -1234))
    + field(4, TType.I32, ("i32", 70000))
    + field(5, TType.I64, ("i64", -9007199254740993))
    + field(6, TType.DOUBLE, ("double", 1.5))
    + field(7, TType.STRING, ("string", "magmalake"))
    + field(8, TType.UUID, ("uuid", UUID_A))
    + field(
        9,
        TType.LIST,
        ("list_begin", TType.I64, 3),
        ("i64", -1),
        ("i64", 0),
        ("i64", I64_MAX),
        ("list_end",),
    )
    + field(
        10,
        TType.MAP,
        ("map_begin", TType.STRING, TType.STRUCT, 1),
        ("string", "k"),
        ("struct_begin",),
        *field(1, TType.BOOL, ("bool", True)),
        ("field_stop",),
        ("struct_end",),
        ("map_end",),
    )
    + [("field_stop",), ("struct_end",)],
)

# ── run the programs through Apache Thrift ─────────────────────────────────


def play(proto, ops):
    for op in ops:
        k = op[0]
        if k == "struct_begin":
            proto.writeStructBegin("S")
        elif k == "struct_end":
            proto.writeStructEnd()
        elif k == "field_begin":
            proto.writeFieldBegin("f", op[1], op[2])
        elif k == "field_end":
            proto.writeFieldEnd()
        elif k == "field_stop":
            proto.writeFieldStop()
        elif k == "list_begin":
            proto.writeListBegin(op[1], op[2])
        elif k == "list_end":
            proto.writeListEnd()
        elif k == "set_begin":
            proto.writeSetBegin(op[1], op[2])
        elif k == "set_end":
            proto.writeSetEnd()
        elif k == "map_begin":
            proto.writeMapBegin(op[1], op[2], op[3])
        elif k == "map_end":
            proto.writeMapEnd()
        elif k == "bool":
            proto.writeBool(op[1])
        elif k == "byte":
            proto.writeByte(op[1])
        elif k == "i16":
            proto.writeI16(op[1])
        elif k == "i32":
            proto.writeI32(op[1])
        elif k == "i64":
            proto.writeI64(op[1])
        elif k == "double":
            proto.writeDouble(op[1])
        elif k == "string":
            proto.writeBinary(op[1].encode("utf-8"))
        elif k == "binary":
            proto.writeBinary(bytes.fromhex(op[1]))
        elif k == "uuid":
            proto.writeUuid(_uuid.UUID(op[1]))
        else:
            raise ValueError(k)


def encode(factory, ops):
    buf = TMemoryBuffer()
    play(factory(buf), ops)
    return buf.getvalue().hex()


# ── emit Mojo ──────────────────────────────────────────────────────────────

MOJO_ESCAPE = {'"': '\\"', "\\": "\\\\", "\n": "\\n"}


def mojo_str(s):
    out = ['"']
    for ch in s:
        if ch in MOJO_ESCAPE:
            out.append(MOJO_ESCAPE[ch])
        else:
            # Mojo source is UTF-8; `\\xNN` escapes are code points, not
            # bytes, so non-ASCII has to go in verbatim.
            out.append(ch)
    out.append('"')
    return "".join(out)


def mojo_i64(v):
    if v == I64_MIN:
        return "(Int64(-9223372036854775807) - Int64(1))"
    return "Int64(%d)" % v


def mojo_f64(v):
    import math

    if math.isnan(v):
        return "nan_f64()"
    if math.isinf(v):
        return "inf_f64()" if v > 0 else "-inf_f64()"
    return "Float64(%r)" % v


def write_ops(ops, indent):
    pad = " " * indent
    out = []
    for op in ops:
        k = op[0]
        if k == "struct_begin":
            out.append(f"{pad}w.write_struct_begin()")
        elif k == "struct_end":
            out.append(f"{pad}w.write_struct_end()")
        elif k == "field_begin":
            out.append(f"{pad}w.write_field_begin({TTYPE_MOJO[op[1]]}, {op[2]})")
        elif k == "field_end":
            out.append(f"{pad}w.write_field_end()")
        elif k == "field_stop":
            out.append(f"{pad}w.write_field_stop()")
        elif k == "list_begin":
            out.append(f"{pad}w.write_list_begin({TTYPE_MOJO[op[1]]}, {op[2]})")
        elif k == "list_end":
            out.append(f"{pad}w.write_list_end()")
        elif k == "set_begin":
            out.append(f"{pad}w.write_set_begin({TTYPE_MOJO[op[1]]}, {op[2]})")
        elif k == "set_end":
            out.append(f"{pad}w.write_set_end()")
        elif k == "map_begin":
            out.append(
                f"{pad}w.write_map_begin({TTYPE_MOJO[op[1]]},"
                f" {TTYPE_MOJO[op[2]]}, {op[3]})"
            )
        elif k == "map_end":
            out.append(f"{pad}w.write_map_end()")
        elif k == "bool":
            out.append(f"{pad}w.write_bool({'True' if op[1] else 'False'})")
        elif k == "byte":
            out.append(f"{pad}w.write_byte(Int8({op[1]}))")
        elif k == "i16":
            out.append(f"{pad}w.write_i16(Int16({op[1]}))")
        elif k == "i32":
            out.append(f"{pad}w.write_i32(Int32({op[1]}))")
        elif k == "i64":
            out.append(f"{pad}w.write_i64({mojo_i64(op[1])})")
        elif k == "double":
            out.append(f"{pad}w.write_double({mojo_f64(op[1])})")
        elif k == "string":
            out.append(f"{pad}w.write_string({mojo_str(op[1])})")
        elif k == "binary":
            out.append(f"{pad}w.write_binary(Span(unhex({mojo_str(op[1])})))")
        elif k == "uuid":
            out.append(
                f"{pad}w.write_uuid(Span(unhex("
                f"{mojo_str(op[1].replace('-', ''))})))"
            )
    return out


def read_ops(ops, indent):
    """The mirror image: read the bytes back and assert every value."""
    pad = " " * indent
    out = []
    for op in ops:
        k = op[0]
        if k == "struct_begin":
            out.append(f"{pad}r.read_struct_begin()")
        elif k == "struct_end":
            out.append(f"{pad}r.read_struct_end()")
        elif k == "field_begin":
            out.append(f"{pad}head = r.read_field_begin()")
            out.append(f"{pad}assert_equal(Int(head[0]), {int(op[1])})")
            out.append(f"{pad}assert_equal(Int(head[1]), {int(op[2])})")
        elif k == "field_end":
            out.append(f"{pad}r.read_field_end()")
        elif k == "field_stop":
            out.append(f"{pad}head = r.read_field_begin()")
            out.append(f"{pad}assert_equal(Int(head[0]), 0)")
        elif k in ("list_begin", "set_begin"):
            verb = "list" if k == "list_begin" else "set"
            out.append(f"{pad}chead = r.read_{verb}_begin()")
            out.append(f"{pad}assert_equal(Int(chead[0]), {int(op[1])})")
            out.append(f"{pad}assert_equal(chead[1], {op[2]})")
        elif k == "list_end":
            out.append(f"{pad}r.read_list_end()")
        elif k == "set_end":
            out.append(f"{pad}r.read_set_end()")
        elif k == "map_begin":
            out.append(f"{pad}mhead = r.read_map_begin()")
            if op[3] != 0:
                out.append(f"{pad}assert_equal(Int(mhead[0]), {int(op[1])})")
                out.append(f"{pad}assert_equal(Int(mhead[1]), {int(op[2])})")
            out.append(f"{pad}assert_equal(mhead[2], {op[3]})")
        elif k == "map_end":
            out.append(f"{pad}r.read_map_end()")
        elif k == "bool":
            out.append(
                f"{pad}assert_equal(r.read_bool(),"
                f" {'True' if op[1] else 'False'})"
            )
        elif k == "byte":
            out.append(f"{pad}assert_equal(Int(r.read_byte()), {op[1]})")
        elif k == "i16":
            out.append(f"{pad}assert_equal(Int(r.read_i16()), {op[1]})")
        elif k == "i32":
            out.append(f"{pad}assert_equal(Int(r.read_i32()), {op[1]})")
        elif k == "i64":
            out.append(f"{pad}assert_equal(r.read_i64(), {mojo_i64(op[1])})")
        elif k == "double":
            import math

            if math.isnan(op[1]):
                out.append(f"{pad}assert_true(is_nan_f64(r.read_double()))")
            else:
                out.append(
                    f"{pad}assert_equal(r.read_double(), {mojo_f64(op[1])})"
                )
        elif k == "string":
            out.append(f"{pad}assert_equal(r.read_string(), {mojo_str(op[1])})")
        elif k == "binary":
            out.append(
                f"{pad}assert_equal(hex_of(Span(r.read_binary())),"
                f" {mojo_str(op[1])})"
            )
        elif k == "uuid":
            out.append(
                f"{pad}assert_equal(hex_of(Span(r.read_uuid())),"
                f" {mojo_str(op[1].replace('-', ''))})"
            )
    return out


TTYPE_MOJO = {
    TType.BOOL: "T_BOOL",
    TType.BYTE: "T_BYTE",
    TType.DOUBLE: "T_DOUBLE",
    TType.I16: "T_I16",
    TType.I32: "T_I32",
    TType.I64: "T_I64",
    TType.STRING: "T_STRING",
    TType.STRUCT: "T_STRUCT",
    TType.MAP: "T_MAP",
    TType.SET: "T_SET",
    TType.LIST: "T_LIST",
    TType.UUID: "T_UUID",
}


def main():
    lines = []
    a = lines.append
    a('"""Thrift wire vectors — GENERATED by tools/gen_vectors.py, do not edit.')
    a("")
    a("The expected bytes come from Apache Thrift's own Python runtime")
    a("(`thrift.protocol.TCompactProtocol` / `TBinaryProtocol` over a")
    a("`TMemoryBuffer`). Each vector is replayed through our writers and the")
    a("bytes compared, then read back through our readers and every value")
    a('checked.')
    a('"""')
    a("")
    a("from std.testing import assert_equal, assert_true")
    a("")
    a("from thrift.protocol import (")
    for n in sorted(set(TTYPE_MOJO.values())):
        a(f"    {n},")
    a("    TProtocolReader,")
    a("    TProtocolWriter,")
    a(")")
    a("")
    a("")
    a("def unhex(text: StringSlice) raises -> List[UInt8]:")
    a("    var out = List[UInt8]()")
    a("    var raw = text.as_bytes()")
    a("    if len(raw) % 2 != 0:")
    a('        raise Error(String("odd hex length"))')
    a("    for i in range(0, len(raw), 2):")
    a("        var hi = _nib(raw[i])")
    a("        var lo = _nib(raw[i + 1])")
    a("        out.append((hi << 4) | lo)")
    a("    return out^")
    a("")
    a("")
    a("def _nib(c: UInt8) raises -> UInt8:")
    a("    if c >= UInt8(48) and c <= UInt8(57):")
    a("        return c - 48")
    a("    if c >= UInt8(97) and c <= UInt8(102):")
    a("        return c - 87")
    a("    if c >= UInt8(65) and c <= UInt8(70):")
    a("        return c - 55")
    a('    raise Error(String("bad hex digit ", Int(c)))')
    a("")
    a("")
    a("def hex_of(data: Span[UInt8, _]) -> String:")
    a('    comptime H = "0123456789abcdef"')
    a("    var out = String()")
    a("    for b in data:")
    a("        out += H[byte= Int(b >> 4)]")
    a("        out += H[byte= Int(b & 0xF)]")
    a("    return out^")
    a("")
    a("")
    a("def nan_f64() -> Float64:")
    a("    var zero = Float64(0.0)")
    a("    return zero / zero")
    a("")
    a("")
    a("def inf_f64() -> Float64:")
    a("    return Float64(1.0) / Float64(0.0)")
    a("")
    a("")
    a("def is_nan_f64(v: Float64) -> Bool:")
    a("    return v != v")
    a("")
    a("")
    a(f"comptime VECTOR_COUNT = {len(VECTORS)}")
    a("")
    a("")
    a("def vector_name(idx: Int) raises -> String:")
    for i, (name, doc, _) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        a(f"        return String({mojo_str(name)})")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    a("")
    a("def vector_doc(idx: Int) raises -> String:")
    for i, (name, doc, _) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        a(f"        return String({mojo_str(doc)})")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    a("")
    a("def compact_hex(idx: Int) raises -> String:")
    for i, (name, doc, ops) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        a(f"        return String({mojo_str(encode(TCompactProtocol, ops))})")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    a("")
    a("def binary_hex(idx: Int) raises -> String:")
    for i, (name, doc, ops) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        a(f"        return String({mojo_str(encode(TBinaryProtocol, ops))})")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    a("")
    a("def replay[W: TProtocolWriter, //](mut w: W, idx: Int) raises:")
    a('    """Write vector `idx` through any protocol writer."""')
    for i, (name, doc, ops) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        lines.extend(write_ops(ops, 8))
        a("        return")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    a("")
    a("def verify[R: TProtocolReader, //](mut r: R, idx: Int) raises:")
    a('    """Read vector `idx` back and assert every value."""')
    a("    var head = Tuple[Int8, Int16](Int8(0), Int16(0))")
    a("    var chead = Tuple[Int8, Int](Int8(0), 0)")
    a("    var mhead = Tuple[Int8, Int8, Int](Int8(0), Int8(0), 0)")
    a("    _ = head")
    a("    _ = chead")
    a("    _ = mhead")
    for i, (name, doc, ops) in enumerate(VECTORS):
        a(f"    if idx == {i}:")
        lines.extend(read_ops(ops, 8))
        a("        return")
    a('    raise Error(String("no such vector ", idx))')
    a("")
    sys.stdout.write("\n".join(lines))


if __name__ == "__main__":
    main()

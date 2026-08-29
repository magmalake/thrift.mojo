"""Apache Thrift wire protocols in pure Mojo — serialization only, no RPC.

Two protocols are implemented, each as a *reader* over a borrowed
`Span[UInt8]` and a *writer* that appends to an owned `List[UInt8]`:

* `TCompactProtocolReader` / `TCompactProtocolWriter` — the variable-length
  encoding described in `doc/specs/thrift-compact-protocol.md`. This is what
  Apache Parquet uses for its footer and page headers.
* `TBinaryProtocolReader` / `TBinaryProtocolWriter` — the fixed-width
  big-endian encoding from `doc/specs/thrift-binary-protocol.md`, in both
  strict (versioned) and non-strict message framing.

Both conform to the `TProtocolReader` / `TProtocolWriter` traits, so
generated struct code can be written once and used with either.

Everything is bounds checked. A truncated buffer, a bad protocol id or
version, a negative length, a container whose declared element count cannot
possibly fit in the bytes that remain, or a nesting depth beyond
`MAX_SKIP_DEPTH` raises rather than reading out of bounds or allocating on a
size an attacker chose.
"""

from std.memory import bitcast


# ── Thrift type ids (the "TType" enum) ─────────────────────────────────────
#
# These are the *protocol independent* type ids used by the generic API and
# by the binary protocol on the wire. The compact protocol has its own,
# denser set of ids and converts at the boundary.

comptime T_STOP: Int8 = 0
comptime T_VOID: Int8 = 1
comptime T_BOOL: Int8 = 2
comptime T_BYTE: Int8 = 3
comptime T_DOUBLE: Int8 = 4
comptime T_I16: Int8 = 6
comptime T_I32: Int8 = 8
comptime T_I64: Int8 = 10
comptime T_STRING: Int8 = 11
comptime T_STRUCT: Int8 = 12
comptime T_MAP: Int8 = 13
comptime T_SET: Int8 = 14
comptime T_LIST: Int8 = 15
comptime T_UUID: Int8 = 16

# `T_BYTE` and `T_STRING` are also spelled I08 and BINARY in the spec.
comptime T_I08: Int8 = T_BYTE
comptime T_BINARY: Int8 = T_STRING

# ── compact protocol constants ─────────────────────────────────────────────

comptime COMPACT_PROTOCOL_ID: UInt8 = 0x82
comptime COMPACT_VERSION: UInt8 = 1
comptime COMPACT_VERSION_MASK: UInt8 = 0x1F
comptime COMPACT_TYPE_MASK: UInt8 = 0xE0
comptime COMPACT_TYPE_SHIFT_AMOUNT = 5

comptime C_STOP: UInt8 = 0x00
comptime C_BOOLEAN_TRUE: UInt8 = 0x01
comptime C_BOOLEAN_FALSE: UInt8 = 0x02
comptime C_BYTE: UInt8 = 0x03
comptime C_I16: UInt8 = 0x04
comptime C_I32: UInt8 = 0x05
comptime C_I64: UInt8 = 0x06
comptime C_DOUBLE: UInt8 = 0x07
comptime C_BINARY: UInt8 = 0x08
comptime C_LIST: UInt8 = 0x09
comptime C_SET: UInt8 = 0x0A
comptime C_MAP: UInt8 = 0x0B
comptime C_STRUCT: UInt8 = 0x0C
comptime C_UUID: UInt8 = 0x0D

# ── binary protocol constants ──────────────────────────────────────────────

comptime BINARY_VERSION_1: UInt32 = 0x80010000
comptime BINARY_VERSION_MASK: UInt32 = 0xFFFF0000

# ── message types ──────────────────────────────────────────────────────────

comptime MESSAGE_CALL: Int8 = 1
comptime MESSAGE_REPLY: Int8 = 2
comptime MESSAGE_EXCEPTION: Int8 = 3
comptime MESSAGE_ONEWAY: Int8 = 4

# How deep `skip` will recurse before giving up. Thrift's own runtimes use
# the same guard; without it a handful of bytes can blow the stack.
comptime MAX_SKIP_DEPTH = 64

# Longest varint that can encode a 64-bit value.
comptime MAX_VARINT_BYTES = 10


def type_name(t: Int8) -> String:
    """A human name for a Thrift type id, for error messages."""
    if t == T_STOP:
        return String("STOP")
    if t == T_VOID:
        return String("VOID")
    if t == T_BOOL:
        return String("BOOL")
    if t == T_BYTE:
        return String("BYTE")
    if t == T_DOUBLE:
        return String("DOUBLE")
    if t == T_I16:
        return String("I16")
    if t == T_I32:
        return String("I32")
    if t == T_I64:
        return String("I64")
    if t == T_STRING:
        return String("STRING")
    if t == T_STRUCT:
        return String("STRUCT")
    if t == T_MAP:
        return String("MAP")
    if t == T_SET:
        return String("SET")
    if t == T_LIST:
        return String("LIST")
    if t == T_UUID:
        return String("UUID")
    return String("type(", Int(t), ")")


# ── zigzag helpers ─────────────────────────────────────────────────────────


def zigzag_i32(n: Int32) -> UInt64:
    """`(n << 1) ^ (n >> 31)`, computed wide so it cannot overflow."""
    var v = Int64(n)
    return bitcast[DType.uint64]((v << Int64(1)) ^ (v >> Int64(63)))


def zigzag_i64(n: Int64) -> UInt64:
    """`(n << 1) ^ (n >> 63)` in the unsigned domain (Int64.MIN is fine)."""
    var u = bitcast[DType.uint64](n)
    var s = bitcast[DType.uint64](n >> Int64(63))
    return (u << UInt64(1)) ^ s


def unzigzag_i64(u: UInt64) -> Int64:
    """The inverse of `zigzag_i64`."""
    var half = bitcast[DType.int64](u >> UInt64(1))
    var sign = bitcast[DType.int64](u & UInt64(1))
    return half ^ (-sign)


def unzigzag_i32(u: UInt64) raises -> Int32:
    """The inverse of `zigzag_i32`, range-checked."""
    var v = unzigzag_i64(u)
    if v < Int64(-2147483648) or v > Int64(2147483647):
        raise Error(String("thrift: zigzag i32 out of range: ", v))
    return Int32(v)


# ── traits ─────────────────────────────────────────────────────────────────


trait TProtocolReader:
    """The read half of the Thrift protocol surface.

    Generated struct readers are written against this trait, so the same
    code decodes compact and binary streams.
    """

    def read_message_begin(mut self) raises -> Tuple[String, Int8, Int32]: ...
    def read_message_end(mut self) raises: ...
    def read_struct_begin(mut self) raises: ...
    def read_struct_end(mut self) raises: ...
    def read_field_begin(mut self) raises -> Tuple[Int8, Int16]: ...
    def read_field_end(mut self) raises: ...
    def read_map_begin(mut self) raises -> Tuple[Int8, Int8, Int]: ...
    def read_map_end(mut self) raises: ...
    def read_list_begin(mut self) raises -> Tuple[Int8, Int]: ...
    def read_list_end(mut self) raises: ...
    def read_set_begin(mut self) raises -> Tuple[Int8, Int]: ...
    def read_set_end(mut self) raises: ...
    def read_bool(mut self) raises -> Bool: ...
    def read_byte(mut self) raises -> Int8: ...
    def read_i16(mut self) raises -> Int16: ...
    def read_i32(mut self) raises -> Int32: ...
    def read_i64(mut self) raises -> Int64: ...
    def read_double(mut self) raises -> Float64: ...
    def read_binary(mut self) raises -> List[UInt8]: ...
    def read_string(mut self) raises -> String: ...
    def read_uuid(mut self) raises -> List[UInt8]: ...
    def skip(mut self, ttype: Int8) raises: ...
    def offset(self) -> Int: ...
    def remaining(self) -> Int: ...


trait TProtocolWriter:
    """The write half of the Thrift protocol surface."""

    def write_message_begin(
        mut self, name: StringSlice, mtype: Int8, seqid: Int32
    ) raises: ...
    def write_message_end(mut self) raises: ...
    def write_struct_begin(mut self) raises: ...
    def write_struct_end(mut self) raises: ...
    def write_field_begin(mut self, ftype: Int8, fid: Int16) raises: ...
    def write_field_end(mut self) raises: ...
    def write_field_stop(mut self) raises: ...
    def write_map_begin(
        mut self, ktype: Int8, vtype: Int8, size: Int
    ) raises: ...
    def write_map_end(mut self) raises: ...
    def write_list_begin(mut self, etype: Int8, size: Int) raises: ...
    def write_list_end(mut self) raises: ...
    def write_set_begin(mut self, etype: Int8, size: Int) raises: ...
    def write_set_end(mut self) raises: ...
    def write_bool(mut self, value: Bool) raises: ...
    def write_byte(mut self, value: Int8) raises: ...
    def write_i16(mut self, value: Int16) raises: ...
    def write_i32(mut self, value: Int32) raises: ...
    def write_i64(mut self, value: Int64) raises: ...
    def write_double(mut self, value: Float64) raises: ...
    def write_binary(mut self, value: Span[UInt8, _]) raises: ...
    def write_string(mut self, value: StringSlice) raises: ...
    def write_uuid(mut self, value: Span[UInt8, _]) raises: ...


def skip_value[P: TProtocolReader, //](mut p: P, ttype: Int8, depth: Int) raises:
    """Consume one value of `ttype` without materialising it.

    Forward compatibility depends on this: a reader that meets a field id it
    does not know must still be able to step over the value, whatever shape
    it has.
    """
    if depth > MAX_SKIP_DEPTH:
        raise Error(
            String("thrift: skip nested deeper than ", MAX_SKIP_DEPTH)
        )
    if ttype == T_BOOL:
        _ = p.read_bool()
    elif ttype == T_BYTE:
        _ = p.read_byte()
    elif ttype == T_I16:
        _ = p.read_i16()
    elif ttype == T_I32:
        _ = p.read_i32()
    elif ttype == T_I64:
        _ = p.read_i64()
    elif ttype == T_DOUBLE:
        _ = p.read_double()
    elif ttype == T_STRING:
        _ = p.read_binary()
    elif ttype == T_UUID:
        _ = p.read_uuid()
    elif ttype == T_STRUCT:
        p.read_struct_begin()
        while True:
            var head = p.read_field_begin()
            if head[0] == T_STOP:
                break
            skip_value(p, head[0], depth + 1)
            p.read_field_end()
        p.read_struct_end()
    elif ttype == T_MAP:
        var head = p.read_map_begin()
        for _ in range(head[2]):
            skip_value(p, head[0], depth + 1)
            skip_value(p, head[1], depth + 1)
        p.read_map_end()
    elif ttype == T_SET:
        var head = p.read_set_begin()
        for _ in range(head[1]):
            skip_value(p, head[0], depth + 1)
        p.read_set_end()
    elif ttype == T_LIST:
        var head = p.read_list_begin()
        for _ in range(head[1]):
            skip_value(p, head[0], depth + 1)
        p.read_list_end()
    elif ttype == T_VOID:
        pass
    else:
        raise Error(String("thrift: cannot skip ", type_name(ttype)))


# ── compact protocol ───────────────────────────────────────────────────────


def _compact_to_ttype(c: UInt8) raises -> Int8:
    var v = c & 0x0F
    if v == C_STOP:
        return T_STOP
    if v == C_BOOLEAN_TRUE or v == C_BOOLEAN_FALSE:
        return T_BOOL
    if v == C_BYTE:
        return T_BYTE
    if v == C_I16:
        return T_I16
    if v == C_I32:
        return T_I32
    if v == C_I64:
        return T_I64
    if v == C_DOUBLE:
        return T_DOUBLE
    if v == C_BINARY:
        return T_STRING
    if v == C_LIST:
        return T_LIST
    if v == C_SET:
        return T_SET
    if v == C_MAP:
        return T_MAP
    if v == C_STRUCT:
        return T_STRUCT
    if v == C_UUID:
        return T_UUID
    raise Error(String("thrift.compact: unknown type id ", Int(v)))


def _ttype_to_compact(t: Int8) raises -> UInt8:
    if t == T_STOP:
        return C_STOP
    if t == T_BOOL:
        return C_BOOLEAN_TRUE
    if t == T_BYTE:
        return C_BYTE
    if t == T_I16:
        return C_I16
    if t == T_I32:
        return C_I32
    if t == T_I64:
        return C_I64
    if t == T_DOUBLE:
        return C_DOUBLE
    if t == T_STRING:
        return C_BINARY
    if t == T_LIST:
        return C_LIST
    if t == T_SET:
        return C_SET
    if t == T_MAP:
        return C_MAP
    if t == T_STRUCT:
        return C_STRUCT
    if t == T_UUID:
        return C_UUID
    raise Error(String("thrift.compact: no compact id for ", type_name(t)))


struct TCompactProtocolReader[origin: ImmOrigin](
    Copyable, Movable, TProtocolReader
):
    """Decodes TCompactProtocol from a borrowed byte span."""

    var data: Span[UInt8, Self.origin]
    var pos: Int
    var _last_field_id: Int16
    var _field_id_stack: List[Int16]
    # -1 = no deferred bool, 0 = false, 1 = true. The compact protocol packs
    # a boolean *field*'s value into its type nibble, so `read_field_begin`
    # has already seen the value by the time `read_bool` is called.
    var _pending_bool: Int8

    def __init__(out self, data: Span[UInt8, Self.origin]):
        self.data = data
        self.pos = 0
        self._last_field_id = 0
        self._field_id_stack = List[Int16]()
        self._pending_bool = -1

    def __init__(out self, data: Span[UInt8, Self.origin], pos: Int):
        self.data = data
        self.pos = pos
        self._last_field_id = 0
        self._field_id_stack = List[Int16]()
        self._pending_bool = -1

    def __init__(out self, *, copy: Self):
        self.data = copy.data
        self.pos = copy.pos
        self._last_field_id = copy._last_field_id
        self._field_id_stack = copy._field_id_stack.copy()
        self._pending_bool = copy._pending_bool

    def __init__(out self, *, deinit move: Self):
        self.data = move.data
        self.pos = move.pos
        self._last_field_id = move._last_field_id
        self._field_id_stack = move._field_id_stack^
        self._pending_bool = move._pending_bool

    def offset(self) -> Int:
        return self.pos

    def remaining(self) -> Int:
        return len(self.data) - self.pos

    def _need(self, n: Int) raises:
        if n < 0 or self.pos + n > len(self.data):
            raise Error(
                String(
                    "thrift.compact: truncated input, wanted ",
                    n,
                    " byte(s) at offset ",
                    self.pos,
                    " of ",
                    len(self.data),
                )
            )

    def _raw_byte(mut self) raises -> UInt8:
        self._need(1)
        var b = self.data[self.pos]
        self.pos += 1
        return b

    def _varint(mut self) raises -> UInt64:
        var result = UInt64(0)
        var shift = 0
        var count = 0
        while True:
            var b = self._raw_byte()
            count += 1
            if count > MAX_VARINT_BYTES:
                raise Error(String("thrift.compact: varint too long"))
            result |= UInt64(b & 0x7F) << UInt64(shift)
            if (b & 0x80) == 0:
                break
            shift += 7
        return result

    def _check_container(self, size: Int, what: StringSlice) raises:
        if size < 0:
            raise Error(
                String("thrift.compact: negative ", what, " size ", size)
            )
        # Every element costs at least one byte on the wire, so a size
        # larger than what is left cannot be honest. Refuse before
        # reserving anything.
        if size > self.remaining():
            raise Error(
                String(
                    "thrift.compact: ",
                    what,
                    " of ",
                    size,
                    " element(s) exceeds the ",
                    self.remaining(),
                    " byte(s) remaining",
                )
            )

    # ── messages ───────────────────────────────────────────────────────────

    def read_message_begin(mut self) raises -> Tuple[String, Int8, Int32]:
        var pid = self._raw_byte()
        if pid != COMPACT_PROTOCOL_ID:
            raise Error(
                String(
                    "thrift.compact: bad protocol id 0x",
                    hex(Int(pid)),
                    ", expected 0x82",
                )
            )
        var vt = self._raw_byte()
        var version = vt & COMPACT_VERSION_MASK
        if version != COMPACT_VERSION:
            raise Error(
                String(
                    "thrift.compact: unsupported version ",
                    Int(version),
                    ", expected 1",
                )
            )
        var mtype = Int8((vt >> COMPACT_TYPE_SHIFT_AMOUNT) & 0x07)
        var seqid = Int32(self._varint())
        var name = self.read_string()
        return (name^, mtype, seqid)

    def read_message_end(mut self) raises:
        pass

    # ── structs and fields ─────────────────────────────────────────────────

    def read_struct_begin(mut self) raises:
        if len(self._field_id_stack) > MAX_SKIP_DEPTH:
            raise Error(
                String("thrift.compact: struct nested deeper than ", MAX_SKIP_DEPTH)
            )
        self._field_id_stack.append(self._last_field_id)
        self._last_field_id = 0

    def read_struct_end(mut self) raises:
        if len(self._field_id_stack) == 0:
            raise Error(String("thrift.compact: struct end without begin"))
        self._last_field_id = self._field_id_stack.pop()

    def read_field_begin(mut self) raises -> Tuple[Int8, Int16]:
        var b = self._raw_byte()
        if (b & 0x0F) == C_STOP:
            return (T_STOP, Int16(0))
        var delta = Int16((b >> 4) & 0x0F)
        var fid: Int16
        if delta == 0:
            # Long form: an explicit zigzag i16 field id follows.
            var raw = unzigzag_i64(self._varint())
            if raw < Int64(-32768) or raw > Int64(32767):
                raise Error(
                    String("thrift.compact: field id out of range: ", raw)
                )
            fid = Int16(raw)
        else:
            fid = self._last_field_id + delta
        self._last_field_id = fid
        var ct = b & 0x0F
        if ct == C_BOOLEAN_TRUE:
            self._pending_bool = 1
        elif ct == C_BOOLEAN_FALSE:
            self._pending_bool = 0
        return (_compact_to_ttype(ct), fid)

    def read_field_end(mut self) raises:
        pass

    # ── containers ─────────────────────────────────────────────────────────

    def read_map_begin(mut self) raises -> Tuple[Int8, Int8, Int]:
        var size = Int(self._varint())
        if size == 0:
            return (T_STOP, T_STOP, 0)
        # A non-empty map needs at least one byte per key and one per value.
        self._check_container(size * 2, "map")
        var kv = self._raw_byte()
        return (
            _compact_to_ttype((kv >> 4) & 0x0F),
            _compact_to_ttype(kv & 0x0F),
            size,
        )

    def read_map_end(mut self) raises:
        pass

    def _read_collection_begin(mut self) raises -> Tuple[Int8, Int]:
        var b = self._raw_byte()
        var size = Int((b >> 4) & 0x0F)
        if size == 15:
            size = Int(self._varint())
        self._check_container(size, "collection")
        return (_compact_to_ttype(b & 0x0F), size)

    def read_list_begin(mut self) raises -> Tuple[Int8, Int]:
        return self._read_collection_begin()

    def read_list_end(mut self) raises:
        pass

    def read_set_begin(mut self) raises -> Tuple[Int8, Int]:
        return self._read_collection_begin()

    def read_set_end(mut self) raises:
        pass

    # ── primitives ─────────────────────────────────────────────────────────

    def read_bool(mut self) raises -> Bool:
        if self._pending_bool >= 0:
            var v = self._pending_bool == 1
            self._pending_bool = -1
            return v
        # A bool outside a field header (a list/set/map element) is an i8:
        # 1 is true, 2 is false. Apache's own runtimes test for 1, so a
        # writer that emitted 0 for false still round-trips.
        var b = self._raw_byte()
        return b == C_BOOLEAN_TRUE

    def read_byte(mut self) raises -> Int8:
        return bitcast[DType.int8](self._raw_byte())

    def read_i16(mut self) raises -> Int16:
        var v = unzigzag_i64(self._varint())
        if v < Int64(-32768) or v > Int64(32767):
            raise Error(String("thrift.compact: i16 out of range: ", v))
        return Int16(v)

    def read_i32(mut self) raises -> Int32:
        return unzigzag_i32(self._varint())

    def read_i64(mut self) raises -> Int64:
        return unzigzag_i64(self._varint())

    def read_double(mut self) raises -> Float64:
        self._need(8)
        var bits = UInt64(0)
        for i in range(8):
            bits |= UInt64(self.data[self.pos + i]) << UInt64(8 * i)
        self.pos += 8
        return bitcast[DType.float64](bits)

    def _read_size(mut self) raises -> Int:
        var n = Int(self._varint())
        if n < 0:
            raise Error(String("thrift.compact: negative length ", n))
        if n > self.remaining():
            raise Error(
                String(
                    "thrift.compact: length ",
                    n,
                    " exceeds the ",
                    self.remaining(),
                    " byte(s) remaining",
                )
            )
        return n

    def read_binary(mut self) raises -> List[UInt8]:
        var n = self._read_size()
        var out = List[UInt8](capacity=n)
        out.extend(self.data[self.pos : self.pos + n])
        self.pos += n
        return out^

    def read_binary_view(mut self) raises -> Span[UInt8, Self.origin]:
        """Zero-copy variant of `read_binary`, valid as long as the input is."""
        var n = self._read_size()
        var view = self.data[self.pos : self.pos + n]
        self.pos += n
        return view

    def read_string(mut self) raises -> String:
        var n = self._read_size()
        var s = String(from_utf8_lossy=self.data[self.pos : self.pos + n])
        self.pos += n
        return s^

    def read_uuid(mut self) raises -> List[UInt8]:
        self._need(16)
        var out = List[UInt8](capacity=16)
        out.extend(self.data[self.pos : self.pos + 16])
        self.pos += 16
        return out^

    def skip(mut self, ttype: Int8) raises:
        skip_value(self, ttype, 0)


struct TCompactProtocolWriter(Copyable, Movable, Defaultable, Sized, TProtocolWriter):
    """Encodes TCompactProtocol into an owned byte list."""

    var out: List[UInt8]
    var _last_field_id: Int16
    var _field_id_stack: List[Int16]
    # Field id of a boolean field whose header has been deferred until its
    # value is known, or -1 when there is none.
    var _pending_bool_field: Int32

    def __init__(out self):
        self.out = List[UInt8]()
        self._last_field_id = 0
        self._field_id_stack = List[Int16]()
        self._pending_bool_field = -1

    def __init__(out self, capacity: Int):
        self.out = List[UInt8](capacity=capacity)
        self._last_field_id = 0
        self._field_id_stack = List[Int16]()
        self._pending_bool_field = -1

    def __init__(out self, *, copy: Self):
        self.out = copy.out.copy()
        self._last_field_id = copy._last_field_id
        self._field_id_stack = copy._field_id_stack.copy()
        self._pending_bool_field = copy._pending_bool_field

    def __init__(out self, *, deinit move: Self):
        self.out = move.out^
        self._last_field_id = move._last_field_id
        self._field_id_stack = move._field_id_stack^
        self._pending_bool_field = move._pending_bool_field

    def __len__(self) -> Int:
        return len(self.out)

    def take(deinit self) -> List[UInt8]:
        return self.out^

    def bytes(self) -> List[UInt8]:
        return self.out.copy()

    def _varint(mut self, value: UInt64):
        var v = value
        while True:
            if (v & ~UInt64(0x7F)) == 0:
                self.out.append(UInt8(v))
                return
            self.out.append(UInt8((v & UInt64(0x7F)) | UInt64(0x80)))
            v >>= UInt64(7)

    def write_message_begin(
        mut self, name: StringSlice, mtype: Int8, seqid: Int32
    ) raises:
        self.out.append(COMPACT_PROTOCOL_ID)
        self.out.append(
            COMPACT_VERSION
            | ((UInt8(bitcast[DType.uint8](mtype)) << COMPACT_TYPE_SHIFT_AMOUNT))
        )
        self._varint(UInt64(Int(seqid)))
        self.write_string(name)

    def write_message_end(mut self) raises:
        pass

    def write_struct_begin(mut self) raises:
        self._field_id_stack.append(self._last_field_id)
        self._last_field_id = 0

    def write_struct_end(mut self) raises:
        if len(self._field_id_stack) == 0:
            raise Error(String("thrift.compact: struct end without begin"))
        self._last_field_id = self._field_id_stack.pop()

    def _field_header(mut self, ctype: UInt8, fid: Int16):
        var delta = Int(fid) - Int(self._last_field_id)
        if delta > 0 and delta <= 15:
            self.out.append(UInt8(delta << 4) | ctype)
        else:
            self.out.append(ctype)
            self._varint(zigzag_i32(Int32(fid)))
        self._last_field_id = fid

    def write_field_begin(mut self, ftype: Int8, fid: Int16) raises:
        if ftype == T_BOOL:
            # Defer: the compact encoding puts the value in the type nibble.
            self._pending_bool_field = Int32(fid)
            return
        self._field_header(_ttype_to_compact(ftype), fid)

    def write_field_end(mut self) raises:
        pass

    def write_field_stop(mut self) raises:
        self.out.append(C_STOP)

    def write_map_begin(mut self, ktype: Int8, vtype: Int8, size: Int) raises:
        if size < 0:
            raise Error(String("thrift.compact: negative map size ", size))
        if size == 0:
            self.out.append(0)
            return
        self._varint(UInt64(size))
        self.out.append(
            (_ttype_to_compact(ktype) << 4) | _ttype_to_compact(vtype)
        )

    def write_map_end(mut self) raises:
        pass

    def _collection_begin(mut self, etype: Int8, size: Int) raises:
        if size < 0:
            raise Error(String("thrift.compact: negative collection size ", size))
        var ct = _ttype_to_compact(etype)
        if size <= 14:
            self.out.append(UInt8(size << 4) | ct)
        else:
            self.out.append(0xF0 | ct)
            self._varint(UInt64(size))

    def write_list_begin(mut self, etype: Int8, size: Int) raises:
        self._collection_begin(etype, size)

    def write_list_end(mut self) raises:
        pass

    def write_set_begin(mut self, etype: Int8, size: Int) raises:
        self._collection_begin(etype, size)

    def write_set_end(mut self) raises:
        pass

    def write_bool(mut self, value: Bool) raises:
        if self._pending_bool_field >= 0:
            var fid = Int16(Int(self._pending_bool_field))
            self._pending_bool_field = -1
            self._field_header(
                C_BOOLEAN_TRUE if value else C_BOOLEAN_FALSE, fid
            )
            return
        # A bool that is a list/set/map *element* is an i8 — and the compact
        # spec spells false as 2, not 0 (it reuses the BOOLEAN_FALSE type id).
        self.out.append(C_BOOLEAN_TRUE if value else C_BOOLEAN_FALSE)

    def write_byte(mut self, value: Int8) raises:
        self.out.append(bitcast[DType.uint8](value))

    def write_i16(mut self, value: Int16) raises:
        self._varint(zigzag_i32(Int32(value)))

    def write_i32(mut self, value: Int32) raises:
        self._varint(zigzag_i32(value))

    def write_i64(mut self, value: Int64) raises:
        self._varint(zigzag_i64(value))

    def write_double(mut self, value: Float64) raises:
        var bits = bitcast[DType.uint64](value)
        for i in range(8):
            self.out.append(UInt8((bits >> UInt64(8 * i)) & UInt64(0xFF)))

    def write_binary(mut self, value: Span[UInt8, _]) raises:
        self._varint(UInt64(len(value)))
        self.out.extend(value)

    def write_string(mut self, value: StringSlice) raises:
        self.write_binary(value.as_bytes())

    def write_uuid(mut self, value: Span[UInt8, _]) raises:
        if len(value) != 16:
            raise Error(
                String("thrift.compact: uuid must be 16 bytes, got ", len(value))
            )
        self.out.extend(value)


# ── binary protocol ────────────────────────────────────────────────────────


struct TBinaryProtocolReader[origin: ImmOrigin](
    Copyable, Movable, TProtocolReader
):
    """Decodes TBinaryProtocol (strict or non-strict) from a byte span."""

    var data: Span[UInt8, Self.origin]
    var pos: Int
    var strict_read: Bool
    var _depth: Int

    def __init__(out self, data: Span[UInt8, Self.origin]):
        self.data = data
        self.pos = 0
        self.strict_read = True
        self._depth = 0

    def __init__(out self, data: Span[UInt8, Self.origin], *, strict_read: Bool):
        self.data = data
        self.pos = 0
        self.strict_read = strict_read
        self._depth = 0

    def __init__(out self, *, copy: Self):
        self.data = copy.data
        self.pos = copy.pos
        self.strict_read = copy.strict_read
        self._depth = copy._depth

    def __init__(out self, *, deinit move: Self):
        self.data = move.data
        self.pos = move.pos
        self.strict_read = move.strict_read
        self._depth = move._depth

    def offset(self) -> Int:
        return self.pos

    def remaining(self) -> Int:
        return len(self.data) - self.pos

    def _need(self, n: Int) raises:
        if n < 0 or self.pos + n > len(self.data):
            raise Error(
                String(
                    "thrift.binary: truncated input, wanted ",
                    n,
                    " byte(s) at offset ",
                    self.pos,
                    " of ",
                    len(self.data),
                )
            )

    def _raw_byte(mut self) raises -> UInt8:
        self._need(1)
        var b = self.data[self.pos]
        self.pos += 1
        return b

    def _check_container(self, size: Int, what: StringSlice) raises:
        if size < 0:
            raise Error(
                String("thrift.binary: negative ", what, " size ", size)
            )
        if size > self.remaining():
            raise Error(
                String(
                    "thrift.binary: ",
                    what,
                    " of ",
                    size,
                    " element(s) exceeds the ",
                    self.remaining(),
                    " byte(s) remaining",
                )
            )

    def _u32(mut self) raises -> UInt32:
        self._need(4)
        var v = UInt32(0)
        for i in range(4):
            v = (v << UInt32(8)) | UInt32(self.data[self.pos + i])
        self.pos += 4
        return v

    def read_message_begin(mut self) raises -> Tuple[String, Int8, Int32]:
        var head = self._u32()
        if (head & UInt32(0x80000000)) != 0:
            if (head & BINARY_VERSION_MASK) != BINARY_VERSION_1:
                raise Error(
                    String(
                        "thrift.binary: bad version 0x",
                        hex(Int(head & BINARY_VERSION_MASK)),
                        ", expected 0x80010000",
                    )
                )
            var mtype = Int8(Int(head & UInt32(0xFF)))
            var name = self.read_string()
            var seqid = self.read_i32()
            return (name^, mtype, seqid)
        if self.strict_read:
            raise Error(
                String(
                    "thrift.binary: missing version prefix (non-strict"
                    " message); construct the reader with strict_read=False"
                    " to accept it"
                )
            )
        var n = Int(head)
        if n < 0 or n > self.remaining():
            raise Error(String("thrift.binary: bad message name length ", n))
        var name = String(from_utf8_lossy=self.data[self.pos : self.pos + n])
        self.pos += n
        var mtype = self.read_byte()
        var seqid = self.read_i32()
        return (name^, mtype, seqid)

    def read_message_end(mut self) raises:
        pass

    def read_struct_begin(mut self) raises:
        self._depth += 1
        if self._depth > MAX_SKIP_DEPTH:
            raise Error(
                String("thrift.binary: struct nested deeper than ", MAX_SKIP_DEPTH)
            )

    def read_struct_end(mut self) raises:
        if self._depth == 0:
            raise Error(String("thrift.binary: struct end without begin"))
        self._depth -= 1

    def read_field_begin(mut self) raises -> Tuple[Int8, Int16]:
        var t = self.read_byte()
        if t == T_STOP:
            return (T_STOP, Int16(0))
        var fid = self.read_i16()
        return (t, fid)

    def read_field_end(mut self) raises:
        pass

    def read_map_begin(mut self) raises -> Tuple[Int8, Int8, Int]:
        var kt = self.read_byte()
        var vt = self.read_byte()
        var size = Int(self.read_i32())
        if size != 0:
            self._check_container(size * 2, "map")
        return (kt, vt, size)

    def read_map_end(mut self) raises:
        pass

    def _collection_begin(mut self) raises -> Tuple[Int8, Int]:
        var et = self.read_byte()
        var size = Int(self.read_i32())
        self._check_container(size, "collection")
        return (et, size)

    def read_list_begin(mut self) raises -> Tuple[Int8, Int]:
        return self._collection_begin()

    def read_list_end(mut self) raises:
        pass

    def read_set_begin(mut self) raises -> Tuple[Int8, Int]:
        return self._collection_begin()

    def read_set_end(mut self) raises:
        pass

    def read_bool(mut self) raises -> Bool:
        return self._raw_byte() != 0

    def read_byte(mut self) raises -> Int8:
        return bitcast[DType.int8](self._raw_byte())

    def read_i16(mut self) raises -> Int16:
        self._need(2)
        var v = (UInt16(self.data[self.pos]) << UInt16(8)) | UInt16(
            self.data[self.pos + 1]
        )
        self.pos += 2
        return bitcast[DType.int16](v)

    def read_i32(mut self) raises -> Int32:
        return bitcast[DType.int32](self._u32())

    def _u64(mut self) raises -> UInt64:
        self._need(8)
        var v = UInt64(0)
        for i in range(8):
            v = (v << UInt64(8)) | UInt64(self.data[self.pos + i])
        self.pos += 8
        return v

    def read_i64(mut self) raises -> Int64:
        return bitcast[DType.int64](self._u64())

    def read_double(mut self) raises -> Float64:
        return bitcast[DType.float64](self._u64())

    def _read_size(mut self) raises -> Int:
        var n = Int(self.read_i32())
        if n < 0:
            raise Error(String("thrift.binary: negative length ", n))
        if n > self.remaining():
            raise Error(
                String(
                    "thrift.binary: length ",
                    n,
                    " exceeds the ",
                    self.remaining(),
                    " byte(s) remaining",
                )
            )
        return n

    def read_binary(mut self) raises -> List[UInt8]:
        var n = self._read_size()
        var out = List[UInt8](capacity=n)
        out.extend(self.data[self.pos : self.pos + n])
        self.pos += n
        return out^

    def read_binary_view(mut self) raises -> Span[UInt8, Self.origin]:
        var n = self._read_size()
        var view = self.data[self.pos : self.pos + n]
        self.pos += n
        return view

    def read_string(mut self) raises -> String:
        var n = self._read_size()
        var s = String(from_utf8_lossy=self.data[self.pos : self.pos + n])
        self.pos += n
        return s^

    def read_uuid(mut self) raises -> List[UInt8]:
        self._need(16)
        var out = List[UInt8](capacity=16)
        out.extend(self.data[self.pos : self.pos + 16])
        self.pos += 16
        return out^

    def skip(mut self, ttype: Int8) raises:
        skip_value(self, ttype, 0)


struct TBinaryProtocolWriter(Copyable, Movable, Defaultable, Sized, TProtocolWriter):
    """Encodes TBinaryProtocol into an owned byte list."""

    var out: List[UInt8]
    var strict_write: Bool

    def __init__(out self):
        self.out = List[UInt8]()
        self.strict_write = True

    def __init__(out self, *, strict_write: Bool):
        self.out = List[UInt8]()
        self.strict_write = strict_write

    def __init__(out self, *, copy: Self):
        self.out = copy.out.copy()
        self.strict_write = copy.strict_write

    def __init__(out self, *, deinit move: Self):
        self.out = move.out^
        self.strict_write = move.strict_write

    def __len__(self) -> Int:
        return len(self.out)

    def take(deinit self) -> List[UInt8]:
        return self.out^

    def bytes(self) -> List[UInt8]:
        return self.out.copy()

    def _put_u32(mut self, v: UInt32):
        self.out.append(UInt8((v >> UInt32(24)) & UInt32(0xFF)))
        self.out.append(UInt8((v >> UInt32(16)) & UInt32(0xFF)))
        self.out.append(UInt8((v >> UInt32(8)) & UInt32(0xFF)))
        self.out.append(UInt8(v & UInt32(0xFF)))

    def _put_u64(mut self, v: UInt64):
        for i in range(8):
            self.out.append(UInt8((v >> UInt64(56 - 8 * i)) & UInt64(0xFF)))

    def write_message_begin(
        mut self, name: StringSlice, mtype: Int8, seqid: Int32
    ) raises:
        if self.strict_write:
            self._put_u32(
                BINARY_VERSION_1 | UInt32(Int(mtype) & 0xFF)
            )
            self.write_string(name)
            self.write_i32(seqid)
        else:
            self.write_string(name)
            self.write_byte(mtype)
            self.write_i32(seqid)

    def write_message_end(mut self) raises:
        pass

    def write_struct_begin(mut self) raises:
        pass

    def write_struct_end(mut self) raises:
        pass

    def write_field_begin(mut self, ftype: Int8, fid: Int16) raises:
        self.write_byte(ftype)
        self.write_i16(fid)

    def write_field_end(mut self) raises:
        pass

    def write_field_stop(mut self) raises:
        self.out.append(0)

    def write_map_begin(mut self, ktype: Int8, vtype: Int8, size: Int) raises:
        if size < 0:
            raise Error(String("thrift.binary: negative map size ", size))
        self.write_byte(ktype)
        self.write_byte(vtype)
        self.write_i32(Int32(size))

    def write_map_end(mut self) raises:
        pass

    def write_list_begin(mut self, etype: Int8, size: Int) raises:
        if size < 0:
            raise Error(String("thrift.binary: negative list size ", size))
        self.write_byte(etype)
        self.write_i32(Int32(size))

    def write_list_end(mut self) raises:
        pass

    def write_set_begin(mut self, etype: Int8, size: Int) raises:
        self.write_list_begin(etype, size)

    def write_set_end(mut self) raises:
        pass

    def write_bool(mut self, value: Bool) raises:
        self.out.append(UInt8(1) if value else UInt8(0))

    def write_byte(mut self, value: Int8) raises:
        self.out.append(bitcast[DType.uint8](value))

    def write_i16(mut self, value: Int16) raises:
        var u = bitcast[DType.uint16](value)
        self.out.append(UInt8((u >> UInt16(8)) & UInt16(0xFF)))
        self.out.append(UInt8(u & UInt16(0xFF)))

    def write_i32(mut self, value: Int32) raises:
        self._put_u32(bitcast[DType.uint32](value))

    def write_i64(mut self, value: Int64) raises:
        self._put_u64(bitcast[DType.uint64](value))

    def write_double(mut self, value: Float64) raises:
        self._put_u64(bitcast[DType.uint64](value))

    def write_binary(mut self, value: Span[UInt8, _]) raises:
        self.write_i32(Int32(len(value)))
        self.out.extend(value)

    def write_string(mut self, value: StringSlice) raises:
        self.write_binary(value.as_bytes())

    def write_uuid(mut self, value: Span[UInt8, _]) raises:
        if len(value) != 16:
            raise Error(
                String("thrift.binary: uuid must be 16 bytes, got ", len(value))
            )
        self.out.extend(value)

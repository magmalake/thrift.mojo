"""Render a decoded `FileMetaData` in the canonical form the oracle uses.

`tools/oracle_pyarrow.py` writes one `key=value` line per fact for every
fixture; this module produces the same lines from *our* decode, so the test
is a line-by-line diff and a failure names the exact field.
"""

from thrift.parquet_types import (
    CompressionCodec,
    Encoding,
    ConvertedType,
    FieldRepetitionType,
    FileMetaData,
    LogicalType,
    SchemaElement,
    TimeUnit,
    Type,
)


def hex_bytes(data: Span[UInt8, _]) -> String:
    comptime H = "0123456789abcdef"
    var out = String()
    for b in data:
        out += H[byte= Int(b >> 4)]
        out += H[byte= Int(b & 0xF)]
    return out^


@fieldwise_init
struct Leaf(Copyable, Movable):
    """One primitive column, with the levels its position implies."""

    var index: Int
    var path: String
    var max_def: Int
    var max_rep: Int


def _walk(
    meta: FileMetaData,
    mut pos: Int,
    prefix: String,
    d: Int,
    r: Int,
    mut out: List[Leaf],
) raises:
    if pos >= len(meta.schema):
        raise Error(String("parquet: schema tree ran past its last element"))
    var idx = pos
    ref e = meta.schema[pos]
    pos += 1
    var path = e.name.copy() if prefix == "" else prefix + "." + e.name
    var dd = d
    var rr = r
    if e.repetition_type:
        var rt = e.repetition_type.value()
        if rt == FieldRepetitionType.OPTIONAL:
            dd += 1
        elif rt == FieldRepetitionType.REPEATED:
            dd += 1
            rr += 1
    var nc = 0
    if e.num_children:
        nc = Int(e.num_children.value())
    if nc == 0:
        out.append(Leaf(idx, path^, dd, rr))
    else:
        for _ in range(nc):
            _walk(meta, pos, path, dd, rr, out)


def collect_leaves(meta: FileMetaData) raises -> List[Leaf]:
    """Depth-first walk of the flat schema list, root excluded from paths."""
    var out = List[Leaf]()
    if len(meta.schema) == 0:
        return out^
    var nc = 0
    if meta.schema[0].num_children:
        nc = Int(meta.schema[0].num_children.value())
    var pos = 1
    for _ in range(nc):
        _walk(meta, pos, String(""), 0, 0, out)
    return out^


def unit_name(u: TimeUnit) raises -> String:
    if u.MILLIS:
        return String("MILLIS")
    if u.MICROS:
        return String("MICROS")
    if u.NANOS:
        return String("NANOS")
    raise Error(String("parquet: TimeUnit with no member set"))


def logical_string(lt: LogicalType) raises -> String:
    """The canonical spelling shared with `tools/oracle_pyarrow.py`."""
    if lt.STRING:
        return String("String")
    if lt.MAP:
        return String("Map")
    if lt.LIST:
        return String("List")
    if lt.ENUM:
        return String("Enum")
    if lt.DECIMAL:
        ref d = lt.DECIMAL.value()
        return String("Decimal(", d.precision, ",", d.scale, ")")
    if lt.DATE:
        return String("Date")
    if lt.TIME:
        ref t = lt.TIME.value()
        return String(
            "Time(",
            "true" if t.isAdjustedToUTC else "false",
            ",",
            unit_name(t.unit),
            ")",
        )
    if lt.TIMESTAMP:
        ref t = lt.TIMESTAMP.value()
        return String(
            "Timestamp(",
            "true" if t.isAdjustedToUTC else "false",
            ",",
            unit_name(t.unit),
            ")",
        )
    if lt.INTEGER:
        ref i = lt.INTEGER.value()
        return String(
            "Int(",
            Int(i.bitWidth),
            ",",
            "true" if i.isSigned else "false",
            ")",
        )
    if lt.UNKNOWN:
        return String("Null")
    if lt.JSON:
        return String("Json")
    if lt.BSON:
        return String("Bson")
    if lt.UUID:
        return String("UUID")
    if lt.FLOAT16:
        return String("Float16")
    if lt.VARIANT:
        return String("Variant")
    if lt.GEOMETRY:
        return String("Geometry")
    if lt.GEOGRAPHY:
        return String("Geography")
    if lt.FILE:
        return String("File")
    raise Error(String("parquet: LogicalType with no member set"))


def _opt_i64(v: Optional[Int64]) -> String:
    if v:
        return String(v.value())
    return String("-")


def _sorted_encoding_names(meta_encodings: List[Encoding]) raises -> String:
    var names = List[String]()
    for e in meta_encodings:
        names.append(e.name())
    # Insertion sort — these lists have a handful of entries.
    for i in range(1, len(names)):
        var j = i
        while j > 0 and names[j] < names[j - 1]:
            var tmp = names[j].copy()
            names[j] = names[j - 1].copy()
            names[j - 1] = tmp^
            j -= 1
    var out = String()
    for i in range(len(names)):
        if i > 0:
            out += ","
        out += names[i]
    return out^


def oracle_text(meta: FileMetaData) raises -> String:
    """The same facts `tools/oracle_pyarrow.py` emits, in the same order."""
    var out = String()
    out += String("version=", meta.version, "\n")
    var created = String()
    if meta.created_by:
        created = meta.created_by.value().copy()
    out += String("created_by=", created, "\n")
    out += String("num_rows=", meta.num_rows, "\n")
    out += String("num_row_groups=", len(meta.row_groups), "\n")
    var leaves = collect_leaves(meta)
    out += String("num_leaves=", len(leaves), "\n")
    out += String("num_schema_elements=", len(meta.schema), "\n")

    var kv_keys = List[String]()
    var kv_lens = List[Int]()
    if meta.key_value_metadata:
        ref kvs = meta.key_value_metadata.value()
        for i in range(len(kvs)):
            kv_keys.append(kvs[i].key.copy())
            var n = 0
            if kvs[i].value:
                n = kvs[i].value.value().byte_length()
            kv_lens.append(n)
    for i in range(1, len(kv_keys)):
        var j = i
        while j > 0 and kv_keys[j] < kv_keys[j - 1]:
            var tk = kv_keys[j].copy()
            kv_keys[j] = kv_keys[j - 1].copy()
            kv_keys[j - 1] = tk^
            var tl = kv_lens[j]
            kv_lens[j] = kv_lens[j - 1]
            kv_lens[j - 1] = tl
            j -= 1
    out += String("kv_count=", len(kv_keys), "\n")
    for i in range(len(kv_keys)):
        out += String("kv=", kv_keys[i], "|", kv_lens[i], "\n")

    for i in range(len(leaves)):
        ref lf = leaves[i]
        ref e = meta.schema[lf.index]
        var physical = String("NONE")
        if e.type_:
            physical = e.type_.value().name()
        var converted = String("NONE")
        if e.converted_type:
            converted = e.converted_type.value().name()
        var logical = String("NONE")
        if e.logicalType:
            logical = logical_string(e.logicalType.value())
        var length = 0
        if e.type_length:
            length = Int(e.type_length.value())
        var precision = -1
        if e.precision:
            precision = Int(e.precision.value())
        var scale = -1
        if e.scale:
            scale = Int(e.scale.value())
        out += String(
            "leaf=", i, "|", lf.path, "|", physical, "|", converted, "|",
            logical, "|", lf.max_def, "|", lf.max_rep, "|", length, "|",
            precision, "|", scale, "\n",
        )

    for r in range(len(meta.row_groups)):
        ref rg = meta.row_groups[r]
        var nsort = 0
        if rg.sorting_columns:
            nsort = len(rg.sorting_columns.value())
        out += String(
            "rg=", r, "|", rg.num_rows, "|", rg.total_byte_size, "|",
            len(rg.columns), "|", nsort, "\n",
        )
        for j in range(len(rg.columns)):
            ref cc = rg.columns[j]
            if not cc.meta_data:
                raise Error(
                    String("parquet: column chunk ", j, " has no meta_data")
                )
            ref cm = cc.meta_data.value()
            var path = String()
            for k in range(len(cm.path_in_schema)):
                if k > 0:
                    path += "."
                path += cm.path_in_schema[k]
            out += String(
                "col=", r, "|", j, "|", path, "|", cm.type_.name(), "|",
                cm.codec.name(), "|", _sorted_encoding_names(cm.encodings),
                "|", cm.num_values, "|", cm.data_page_offset, "|",
                _opt_i64(cm.dictionary_page_offset), "|",
                cm.total_compressed_size, "|", cm.total_uncompressed_size,
                "|", cc.file_offset, "|",
                _opt_i64(cm.bloom_filter_offset), "|",
                "-" if not cm.bloom_filter_length
                else String(cm.bloom_filter_length.value()),
                "\n",
            )
            if not cm.statistics:
                out += String("stats=", r, "|", j, "|-\n")
                continue
            ref st = cm.statistics.value()
            var mn = String("-")
            var mx = String("-")
            if Bool(st.max_value) and Bool(st.min_value):
                mn = hex_bytes(Span(st.min_value.value()))
                mx = hex_bytes(Span(st.max_value.value()))
            elif Bool(st.max) and Bool(st.min):
                mn = hex_bytes(Span(st.min.value()))
                mx = hex_bytes(Span(st.max.value()))
            out += String(
                "stats=", r, "|", j, "|set|", mn, "|", mx, "|",
                _opt_i64(st.null_count), "|", _opt_i64(st.distinct_count),
                "\n",
            )
    return out^

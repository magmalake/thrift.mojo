"""`thrift-mojo` — look inside a Parquet file's metadata from the shell.

    thrift-mojo parquet-meta <file.parquet> [--json] [--pages]

`parquet-meta` prints the schema tree, the row groups and every column
chunk: codec, encodings, offsets, sizes and statistics. `--json` emits the
same information as JSON; `--pages` additionally walks and prints every page
header in the file.
"""

from std.sys import argv

from thrift.parquet_footer import (
    read_footer,
    read_page_header,
    read_parquet_file,
)
from thrift.parquet_types import (
    ColumnMetaData,
    FieldRepetitionType,
    FileMetaData,
    LogicalType,
    PageType,
    SchemaElement,
    Statistics,
    TimeUnit,
    Type,
)

comptime USAGE = String(
    "thrift-mojo — Apache Thrift serialization for Mojo\n"
    "\n"
    "usage:\n"
    "  thrift-mojo parquet-meta <file.parquet> [--json] [--pages]\n"
    "\n"
    "options:\n"
    "  --json    emit JSON instead of text\n"
    "  --pages   also walk and print every page header\n"
)


# ── small formatting helpers ───────────────────────────────────────────────


def hex_of(data: Span[UInt8, _]) -> String:
    comptime H = "0123456789abcdef"
    var out = String()
    for b in data:
        out += H[byte= Int(b >> 4)]
        out += H[byte= Int(b & 0xF)]
    return out^


def json_string(s: StringSlice) -> String:
    var out = String('"')
    for cp in s.codepoint_slices():
        if cp == '"':
            out += '\\"'
        elif cp == "\\":
            out += "\\\\"
        elif cp == "\n":
            out += "\\n"
        elif cp == "\r":
            out += "\\r"
        elif cp == "\t":
            out += "\\t"
        else:
            out += cp
    out += '"'
    return out^


def printable(data: Span[UInt8, _], as_text: Bool) -> String:
    """Statistics bounds as text for the byte types, as hex for the rest."""
    if not as_text:
        if len(data) > 32:
            return String("0x", hex_of(data[0:32]), "…")
        return String("0x", hex_of(data))
    if len(data) == 0:
        return String('""')
    if len(data) > 32:
        return String("0x", hex_of(data[0:32]), "…")
    for b in data:
        if b < 0x20 or b >= 0x7F:
            return String("0x", hex_of(data))
    return String('"', String(from_utf8_lossy=data), '"')


def unit_name(u: TimeUnit) raises -> String:
    if u.MILLIS:
        return String("MILLIS")
    if u.MICROS:
        return String("MICROS")
    if u.NANOS:
        return String("NANOS")
    return String("?")


def logical_name(lt: LogicalType) raises -> String:
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
            "utc" if t.isAdjustedToUTC else "local",
            ",",
            unit_name(t.unit),
            ")",
        )
    if lt.TIMESTAMP:
        ref t = lt.TIMESTAMP.value()
        return String(
            "Timestamp(",
            "utc" if t.isAdjustedToUTC else "local",
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
            "signed" if i.isSigned else "unsigned",
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
    return String("?")


def repetition_name(e: SchemaElement) raises -> String:
    if not e.repetition_type:
        return String("required")
    var rt = e.repetition_type.value()
    if rt == FieldRepetitionType.REQUIRED:
        return String("required")
    if rt == FieldRepetitionType.OPTIONAL:
        return String("optional")
    if rt == FieldRepetitionType.REPEATED:
        return String("repeated")
    return rt.name()


def describe_element(e: SchemaElement) raises -> String:
    var out = repetition_name(e)
    if e.type_:
        out += " " + e.type_.value().name().lower()
        if e.type_length:
            out += String("(", e.type_length.value(), ")")
    else:
        out += " group"
    out += " " + e.name
    var tags = List[String]()
    if e.logicalType:
        tags.append(logical_name(e.logicalType.value()))
    elif e.converted_type:
        tags.append(e.converted_type.value().name())
    if e.field_id:
        tags.append(String("id=", e.field_id.value()))
    if len(tags) > 0:
        out += " ("
        for i in range(len(tags)):
            if i > 0:
                out += ", "
            out += tags[i]
        out += ")"
    return out^


# ── text output ────────────────────────────────────────────────────────────


def print_schema_tree(meta: FileMetaData, mut pos: Int, depth: Int) raises:
    if pos >= len(meta.schema):
        return
    ref e = meta.schema[pos]
    var idx = pos
    pos += 1
    var pad = String()
    for _ in range(depth):
        pad += "  "
    if idx == 0:
        print(String(pad, "message ", e.name, " {"))
    else:
        var nc = 0
        if e.num_children:
            nc = Int(e.num_children.value())
        print(String(pad, describe_element(e), " {" if nc > 0 else ";"))
    var nc = 0
    if e.num_children:
        nc = Int(e.num_children.value())
    for _ in range(nc):
        print_schema_tree(meta, pos, depth + 1)
    if nc > 0 or idx == 0:
        print(String(pad, "}"))


def stats_text(st: Statistics, physical: Type) raises -> String:
    var as_text = (
        physical == Type.BYTE_ARRAY or physical == Type.FIXED_LEN_BYTE_ARRAY
    )
    var parts = List[String]()
    if st.min_value:
        parts.append(String("min=", printable(Span(st.min_value.value()), as_text)))
    elif st.min:
        parts.append(String("min=", printable(Span(st.min.value()), as_text)))
    if st.max_value:
        parts.append(String("max=", printable(Span(st.max_value.value()), as_text)))
    elif st.max:
        parts.append(String("max=", printable(Span(st.max.value()), as_text)))
    if st.null_count:
        parts.append(String("nulls=", st.null_count.value()))
    if st.distinct_count:
        parts.append(String("distinct=", st.distinct_count.value()))
    if st.nan_count:
        parts.append(String("nans=", st.nan_count.value()))
    var out = String()
    for i in range(len(parts)):
        if i > 0:
            out += " "
        out += parts[i]
    return out^


def encodings_text(cm: ColumnMetaData) raises -> String:
    var out = String()
    for i in range(len(cm.encodings)):
        if i > 0:
            out += ","
        out += cm.encodings[i].name()
    return out^


def path_text(cm: ColumnMetaData) -> String:
    var out = String()
    for i in range(len(cm.path_in_schema)):
        if i > 0:
            out += "."
        out += cm.path_in_schema[i]
    return out^


def print_text(path: StringSlice, meta: FileMetaData, data: Span[UInt8, _], pages: Bool) raises:
    print(String("file:        ", path))
    print(String("version:     ", meta.version))
    if meta.created_by:
        print(String("created by:  ", meta.created_by.value()))
    print(String("rows:        ", meta.num_rows))
    print(String("row groups:  ", len(meta.row_groups)))
    print(String("schema nodes:", len(meta.schema)))
    if meta.encryption_algorithm:
        print("encryption:  yes (footer signed)")
    if meta.key_value_metadata:
        ref kvs = meta.key_value_metadata.value()
        print(String("key/value:   ", len(kvs), " entr(y|ies)"))
        for i in range(len(kvs)):
            var n = 0
            if kvs[i].value:
                n = kvs[i].value.value().byte_length()
            print(String("  ", kvs[i].key, " (", n, " bytes)"))
    print("")
    print("schema:")
    var pos = 0
    print_schema_tree(meta, pos, 0)
    print("")
    for r in range(len(meta.row_groups)):
        ref rg = meta.row_groups[r]
        var extra = String()
        if rg.ordinal:
            extra += String(" ordinal=", rg.ordinal.value())
        if rg.file_offset:
            extra += String(" offset=", rg.file_offset.value())
        print(
            String(
                "row group ", r, ": ", rg.num_rows, " rows, ",
                rg.total_byte_size, " bytes uncompressed, ",
                len(rg.columns), " column(s)", extra,
            )
        )
        for j in range(len(rg.columns)):
            ref cc = rg.columns[j]
            if not cc.meta_data:
                print(String("  [", j, "] <metadata encrypted>"))
                continue
            ref cm = cc.meta_data.value()
            print(
                String(
                    "  [", j, "] ", path_text(cm), ": ", cm.type_.name(),
                    " ", cm.codec.name(), " ", encodings_text(cm),
                )
            )
            var dict_off = String("-")
            if cm.dictionary_page_offset:
                dict_off = String(cm.dictionary_page_offset.value())
            print(
                String(
                    "      values=", cm.num_values,
                    " data@", cm.data_page_offset,
                    " dict@", dict_off,
                    " compressed=", cm.total_compressed_size,
                    " uncompressed=", cm.total_uncompressed_size,
                )
            )
            if cm.statistics:
                print(String("      stats: ", stats_text(cm.statistics.value(), cm.type_)))
            if cm.bloom_filter_offset:
                print(
                    String(
                        "      bloom@", cm.bloom_filter_offset.value(),
                        " length=",
                        cm.bloom_filter_length.value()
                        if cm.bloom_filter_length else Int32(-1),
                    )
                )
            if cc.offset_index_offset:
                print(
                    String(
                        "      offset index@", cc.offset_index_offset.value(),
                        " column index@",
                        cc.column_index_offset.value()
                        if cc.column_index_offset else Int64(-1),
                    )
                )
            if pages:
                print_pages(data, cm)
        print("")


def print_pages(data: Span[UInt8, _], cm: ColumnMetaData) raises:
    var start = cm.data_page_offset
    if cm.dictionary_page_offset:
        var d = cm.dictionary_page_offset.value()
        if d < start:
            start = d
    var pos = Int(start)
    var stop = Int(start + cm.total_compressed_size)
    while pos < stop:
        var page = read_page_header(data, pos)
        ref h = page[0]
        var detail = String()
        if h.data_page_header:
            ref d = h.data_page_header.value()
            detail = String(
                " values=", d.num_values, " encoding=", d.encoding.name()
            )
        elif h.data_page_header_v2:
            ref d2 = h.data_page_header_v2.value()
            detail = String(
                " values=", d2.num_values, " nulls=", d2.num_nulls,
                " rows=", d2.num_rows, " encoding=", d2.encoding.name(),
            )
        elif h.dictionary_page_header:
            ref d3 = h.dictionary_page_header.value()
            detail = String(
                " values=", d3.num_values, " encoding=", d3.encoding.name()
            )
        print(
            String(
                "      page @", pos, " ", h.type_.name(), detail,
                " header=", page[1],
                " compressed=", h.compressed_page_size,
                " uncompressed=", h.uncompressed_page_size,
            )
        )
        pos += page[1] + Int(h.compressed_page_size)


# ── JSON output ────────────────────────────────────────────────────────────


def print_json(path: StringSlice, meta: FileMetaData, data: Span[UInt8, _], pages: Bool) raises:
    var out = String("{\n")
    out += String('  "file": ', json_string(path), ",\n")
    out += String('  "version": ', meta.version, ",\n")
    out += String(
        '  "created_by": ',
        json_string(meta.created_by.value()) if meta.created_by else "null",
        ",\n",
    )
    out += String('  "num_rows": ', meta.num_rows, ",\n")
    out += '  "key_value_metadata": {'
    if meta.key_value_metadata:
        ref kvs = meta.key_value_metadata.value()
        for i in range(len(kvs)):
            if i > 0:
                out += ","
            out += String(
                "\n    ",
                json_string(kvs[i].key),
                ": ",
                json_string(kvs[i].value.value()) if kvs[i].value else "null",
            )
        if len(kvs) > 0:
            out += "\n  "
    out += "},\n"
    out += '  "schema": [\n'
    for i in range(len(meta.schema)):
        ref e = meta.schema[i]
        out += String("    {", '"name": ', json_string(e.name))
        out += String(', "repetition": ', json_string(repetition_name(e)))
        if e.type_:
            out += String(', "type": ', json_string(e.type_.value().name()))
        if e.type_length:
            out += String(', "type_length": ', e.type_length.value())
        if e.num_children:
            out += String(', "num_children": ', e.num_children.value())
        if e.converted_type:
            out += String(
                ', "converted_type": ',
                json_string(e.converted_type.value().name()),
            )
        if e.logicalType:
            out += String(
                ', "logical_type": ',
                json_string(logical_name(e.logicalType.value())),
            )
        if e.field_id:
            out += String(', "field_id": ', e.field_id.value())
        if e.precision:
            out += String(', "precision": ', e.precision.value())
        if e.scale:
            out += String(', "scale": ', e.scale.value())
        out += "}"
        if i + 1 < len(meta.schema):
            out += ","
        out += "\n"
    out += "  ],\n"
    out += '  "row_groups": [\n'
    for r in range(len(meta.row_groups)):
        ref rg = meta.row_groups[r]
        out += String(
            '    {"num_rows": ', rg.num_rows,
            ', "total_byte_size": ', rg.total_byte_size,
        )
        if rg.total_compressed_size:
            out += String(
                ', "total_compressed_size": ', rg.total_compressed_size.value()
            )
        if rg.ordinal:
            out += String(', "ordinal": ', rg.ordinal.value())
        out += ',\n     "columns": [\n'
        for j in range(len(rg.columns)):
            ref cc = rg.columns[j]
            if not cc.meta_data:
                out += '      {"encrypted": true}'
            else:
                ref cm = cc.meta_data.value()
                out += String(
                    '      {"path": ', json_string(path_text(cm)),
                    ', "type": ', json_string(cm.type_.name()),
                    ', "codec": ', json_string(cm.codec.name()),
                    ', "encodings": [',
                )
                for k in range(len(cm.encodings)):
                    if k > 0:
                        out += ", "
                    out += json_string(cm.encodings[k].name())
                out += String(
                    '], "num_values": ', cm.num_values,
                    ', "data_page_offset": ', cm.data_page_offset,
                )
                if cm.dictionary_page_offset:
                    out += String(
                        ', "dictionary_page_offset": ',
                        cm.dictionary_page_offset.value(),
                    )
                if cm.index_page_offset:
                    out += String(
                        ', "index_page_offset": ', cm.index_page_offset.value()
                    )
                out += String(
                    ', "total_compressed_size": ', cm.total_compressed_size,
                    ', "total_uncompressed_size": ', cm.total_uncompressed_size,
                    ', "file_offset": ', cc.file_offset,
                )
                if cm.bloom_filter_offset:
                    out += String(
                        ', "bloom_filter_offset": ',
                        cm.bloom_filter_offset.value(),
                    )
                if cc.offset_index_offset:
                    out += String(
                        ', "offset_index_offset": ',
                        cc.offset_index_offset.value(),
                    )
                if cc.column_index_offset:
                    out += String(
                        ', "column_index_offset": ',
                        cc.column_index_offset.value(),
                    )
                if cm.statistics:
                    ref st = cm.statistics.value()
                    out += ', "statistics": {'
                    var wrote = False
                    if st.min_value:
                        out += String(
                            '"min_hex": ',
                            json_string(hex_of(Span(st.min_value.value()))),
                        )
                        wrote = True
                    if st.max_value:
                        if wrote:
                            out += ", "
                        out += String(
                            '"max_hex": ',
                            json_string(hex_of(Span(st.max_value.value()))),
                        )
                        wrote = True
                    if st.null_count:
                        if wrote:
                            out += ", "
                        out += String('"null_count": ', st.null_count.value())
                        wrote = True
                    if st.distinct_count:
                        if wrote:
                            out += ", "
                        out += String(
                            '"distinct_count": ', st.distinct_count.value()
                        )
                    out += "}"
                if pages:
                    out += ', "pages": ['
                    out += pages_json(data, cm)
                    out += "]"
                out += "}"
            if j + 1 < len(rg.columns):
                out += ","
            out += "\n"
        out += "     ]}"
        if r + 1 < len(meta.row_groups):
            out += ","
        out += "\n"
    out += "  ]\n}"
    print(out)


def pages_json(data: Span[UInt8, _], cm: ColumnMetaData) raises -> String:
    var start = cm.data_page_offset
    if cm.dictionary_page_offset:
        var d = cm.dictionary_page_offset.value()
        if d < start:
            start = d
    var pos = Int(start)
    var stop = Int(start + cm.total_compressed_size)
    var out = String()
    var first = True
    while pos < stop:
        var page = read_page_header(data, pos)
        ref h = page[0]
        if not first:
            out += ", "
        first = False
        var values = Int32(0)
        var enc = String("")
        if h.data_page_header:
            values = h.data_page_header.value().num_values
            enc = h.data_page_header.value().encoding.name()
        elif h.data_page_header_v2:
            values = h.data_page_header_v2.value().num_values
            enc = h.data_page_header_v2.value().encoding.name()
        elif h.dictionary_page_header:
            values = h.dictionary_page_header.value().num_values
            enc = h.dictionary_page_header.value().encoding.name()
        out += String(
            '{"offset": ', pos,
            ', "type": ', json_string(h.type_.name()),
            ', "num_values": ', values,
            ', "encoding": ', json_string(enc),
            ', "header_size": ', page[1],
            ', "compressed_page_size": ', h.compressed_page_size,
            ', "uncompressed_page_size": ', h.uncompressed_page_size,
            "}",
        )
        pos += page[1] + Int(h.compressed_page_size)
    return out^


def main() raises:
    var args = argv()
    if len(args) < 3:
        print(USAGE)
        return
    var command = String(args[1])
    if command != "parquet-meta":
        print(String("thrift-mojo: unknown command '", command, "'\n"))
        print(USAGE)
        return
    var path = String(args[2])
    var as_json = False
    var with_pages = False
    for k in range(3, len(args)):
        var a = String(args[k])
        if a == "--json":
            as_json = True
        elif a == "--pages":
            with_pages = True
        else:
            print(String("thrift-mojo: unknown option '", a, "'\n"))
            print(USAGE)
            return
    var data = read_parquet_file(path)
    var meta = read_footer(Span(data))
    if as_json:
        print_json(path, meta, Span(data), with_pages)
    else:
        print_text(path, meta, Span(data), with_pages)
    _ = data^

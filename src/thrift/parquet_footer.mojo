"""Locating and decoding the Parquet footer and page headers.

A Parquet file is `PAR1 <data...> <thrift FileMetaData> <i32 length> PAR1`,
where the length is little-endian and covers just the serialised metadata.
Page headers are the same compact-protocol encoding, written inline in the
data area with no length prefix — the decoder tells you how many bytes it
consumed so you can find the page body that follows.

```mojo
from thrift import read_footer, read_parquet_file

var bytes = read_parquet_file("part-0.parquet")
var meta = read_footer(Span(bytes))
print(meta.num_rows, len(meta.row_groups))
```
"""

from std.memory import bitcast

from thrift.parquet_types import FileMetaData, PageHeader
from thrift.protocol import TCompactProtocolReader, TCompactProtocolWriter

comptime PARQUET_MAGIC = "PAR1"
comptime PARQUET_MAGIC_ENCRYPTED = "PARE"
comptime FOOTER_TRAILER_SIZE = 8


def read_parquet_file(path: StringSlice) raises -> List[UInt8]:
    """Slurp a whole file. Convenience for the CLI and the tests."""
    with open(String(path), "r") as f:
        return f.read_bytes()


def _magic_at(data: Span[UInt8, _], offset: Int) -> String:
    if offset < 0 or offset + 4 > len(data):
        return String()
    return String(from_utf8_lossy=data[offset : offset + 4])


def footer_length(data: Span[UInt8, _]) raises -> Int:
    """The declared byte length of the serialised `FileMetaData`."""
    if len(data) < FOOTER_TRAILER_SIZE + 4:
        raise Error(
            String(
                "parquet: file is ",
                len(data),
                " bytes, too small to be a Parquet file",
            )
        )
    var tail = _magic_at(data, len(data) - 4)
    if tail == PARQUET_MAGIC_ENCRYPTED:
        raise Error(
            String("parquet: encrypted footer unsupported (trailing PARE)")
        )
    if tail != PARQUET_MAGIC:
        raise Error(
            String(
                "parquet: bad trailing magic '",
                tail,
                "', expected PAR1",
            )
        )
    var head = _magic_at(data, 0)
    if head == PARQUET_MAGIC_ENCRYPTED:
        raise Error(
            String("parquet: encrypted footer unsupported (leading PARE)")
        )
    if head != PARQUET_MAGIC:
        raise Error(
            String("parquet: bad leading magic '", head, "', expected PAR1")
        )
    var base = len(data) - FOOTER_TRAILER_SIZE
    var n = Int64(0)
    for i in range(4):
        n |= Int64(Int(data[base + i])) << Int64(8 * i)
    if n < 0 or n > Int64(base - 4):
        raise Error(
            String(
                "parquet: footer length ",
                n,
                " does not fit between the magic and the trailer (",
                base - 4,
                " bytes available)",
            )
        )
    return Int(n)


def read_footer(file_bytes: Span[UInt8, _]) raises -> FileMetaData:
    """Decode the `FileMetaData` at the end of a whole Parquet file."""
    var n = footer_length(file_bytes)
    var start = len(file_bytes) - FOOTER_TRAILER_SIZE - n
    var r = TCompactProtocolReader(file_bytes[start : start + n])
    var meta = FileMetaData()
    meta.read(r)
    return meta^


def read_footer_bytes(metadata_bytes: Span[UInt8, _]) raises -> FileMetaData:
    """Decode a `FileMetaData` that has already been sliced out of a file."""
    var r = TCompactProtocolReader(metadata_bytes)
    var meta = FileMetaData()
    meta.read(r)
    return meta^


def write_footer(meta: FileMetaData) raises -> List[UInt8]:
    """Serialise a `FileMetaData` with the compact protocol.

    This is the footer body only — no magic, no length trailer. Use
    `write_footer_trailer` to append the 8-byte tail a writer needs.
    """
    var w = TCompactProtocolWriter()
    meta.write(w)
    return w^.take()


def write_footer_trailer(mut out: List[UInt8], metadata_length: Int) raises:
    """Append the little-endian i32 length and the trailing `PAR1`."""
    if metadata_length < 0:
        raise Error(
            String("parquet: negative metadata length ", metadata_length)
        )
    for i in range(4):
        out.append(UInt8((metadata_length >> (8 * i)) & 0xFF))
    out.extend(StringSlice(PARQUET_MAGIC).as_bytes())


def read_page_header(
    data: Span[UInt8, _], offset: Int
) raises -> Tuple[PageHeader, Int]:
    """Decode one page header at `offset`, and say how long it was.

    The second element is the header's byte length, so the page body starts
    at `offset + header_len` and runs for `compressed_page_size` bytes.
    """
    if offset < 0 or offset > len(data):
        raise Error(
            String(
                "parquet: page header offset ",
                offset,
                " outside the ",
                len(data),
                "-byte file",
            )
        )
    var r = TCompactProtocolReader(data, offset)
    var header = PageHeader()
    header.read(r)
    return (header^, r.pos - offset)

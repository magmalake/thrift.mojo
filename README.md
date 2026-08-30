# thrift.mojo

[![mojoshelf](https://mojoshelf.org/badge/thrift-mojo.svg)](https://mojoshelf.org/tins/thrift-mojo) [![mojo nightly](https://mojoshelf.org/badge/thrift-mojo/nightly.svg)](https://mojoshelf.org/tins/thrift-mojo)

[![CI](https://github.com/magmalake/thrift.mojo/actions/workflows/ci.yml/badge.svg)](https://github.com/magmalake/thrift.mojo/actions/workflows/ci.yml) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Part of **magmalake** — data lake building blocks in Mojo.

A pure-[Mojo](https://www.modular.com/mojo) implementation of Apache Thrift
*serialization* — `TCompactProtocol` and `TBinaryProtocol` — plus every
struct, union and enum of the [Apache Parquet metadata
schema](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift),
and the helpers that find and decode a Parquet footer and its page headers.

No RPC. No IDL compiler at run time. No dependencies at all — the standard
library and nothing else, so a consumer needs only `-I ../thrift.mojo/src`.

```mojo
from thrift import read_footer, read_parquet_file

var bytes = read_parquet_file("part-0.parquet")
var meta = read_footer(Span(bytes))
print(meta.num_rows, "rows in", len(meta.row_groups), "row groups")
for rg in meta.row_groups:
    for cc in rg.columns:
        ref cm = cc.meta_data.value()
        print(cm.path_in_schema[0], cm.codec.name(), cm.num_values)
```

## Why only these two protocols

Parquet's footer, page headers, page index and bloom filter headers are all
Thrift `TCompactProtocol`. That is the only wire format a Parquet reader
needs, and it is the one this library is built around and benchmarked on.
`TBinaryProtocol` comes along because it is 200 more lines, it is what most
other Thrift data at rest uses, and having two protocols behind one trait is
what proves the generated struct code really is protocol-agnostic — the test
suite round-trips a real Parquet `FileMetaData` through the binary protocol
as well as the compact one.

What is deliberately *not* here: the RPC layer (`TProcessor`, servers,
`TFramedTransport`), the JSON and Debug protocols, and any run-time IDL
parsing. Struct code is generated ahead of time (see
[Regenerating](#regenerating-the-parquet-structs)).

## API

### Protocols — `thrift.protocol`

Four concrete types, two traits:

| | reads a `Span[UInt8]` | writes a `List[UInt8]` |
|---|---|---|
| compact | `TCompactProtocolReader[origin]` | `TCompactProtocolWriter` |
| binary | `TBinaryProtocolReader[origin]` | `TBinaryProtocolWriter` |
| trait | `TProtocolReader` | `TProtocolWriter` |

The full `TProtocol` surface is present on both:
`read`/`write_message_begin`/`end`, `…_struct_begin`/`end`,
`…_field_begin`/`end`, `write_field_stop`, `…_map_begin`/`end`,
`…_list_begin`/`end`, `…_set_begin`/`end`, and
`bool`/`byte`/`i16`/`i32`/`i64`/`double`/`binary`/`string`/`uuid`. Readers
also have `skip(ttype)`, `offset()`, `remaining()` and a zero-copy
`read_binary_view()`.

Type ids are the module-level `T_BOOL`, `T_BYTE`, `T_I16`, `T_I32`, `T_I64`,
`T_DOUBLE`, `T_STRING`, `T_STRUCT`, `T_MAP`, `T_SET`, `T_LIST`, `T_UUID`,
`T_STOP`, with `type_name(t)` for messages.

```mojo
from thrift import T_STOP, TCompactProtocolReader

var r = TCompactProtocolReader(Span(bytes))
r.read_struct_begin()
while True:
    var head = r.read_field_begin()          # (type, field id)
    if head[0] == T_STOP:
        break
    if head[1] == 1:
        var version = r.read_i32()
    else:
        r.skip(head[0])                      # forward compatibility
    r.read_field_end()
r.read_struct_end()
```

Writing a bool field is the one place the compact protocol is subtle: the
value lives in the field header's type nibble, so `write_field_begin(T_BOOL,
id)` writes nothing and the following `write_bool` emits the header. Bools
that are *elements* of a list, set or map are a whole byte, and the spec
spells false as `2`, not `0`. Both directions are covered by the vectors.

**Everything is bounds checked.** Truncated input, a bad compact protocol id
or version, a bad binary version word, a negative length, a nesting depth
past `MAX_SKIP_DEPTH` (64), a varint longer than 10 bytes, or a container
whose declared element count cannot fit in the bytes that remain — each
raises with a message that names the offset. A malicious size never reaches
an allocator: the reader refuses before reserving.

### Parquet metadata — `thrift.parquet_types`

Generated from `spec/parquet.thrift`, which is the current upstream file kept
in this repository with its Apache licence header for provenance. Optional
fields are `Optional[T]`; required fields are validated on read; unknown
field ids are skipped; `write` emits fields in ascending id order; unions
insist on exactly one member.

Enums are **open**: they are `Int32` wrappers with `comptime` members
(`Type.BYTE_ARRAY`, `CompressionCodec.ZSTD`, …) and a `name()` that falls
back to `CompressionCodec(99)` for a value the IDL does not define. Parquet
keeps adding codecs and encodings; a reader that raises on one it has not
heard of is a reader that breaks on next year's files.

Coverage is the whole IDL — **8 enums, 53 structs, 8 unions**:

| kind | types |
|---|---|
| enums (8) | `Type`, `ConvertedType`, `FieldRepetitionType`, `EdgeInterpolationAlgorithm`, `Encoding`, `CompressionCodec`, `PageType`, `BoundaryOrder` |
| unions (8) | `TimeUnit`, `LogicalType`, `BloomFilterAlgorithm`, `BloomFilterHash`, `BloomFilterCompression`, `ColumnCryptoMetaData`, `ColumnOrder`, `EncryptionAlgorithm` |
| logical types (19) | `StringType`, `UUIDType`, `MapType`, `ListType`, `EnumType`, `DateType`, `Float16Type`, `NullType`, `DecimalType`, `MilliSeconds`, `MicroSeconds`, `NanoSeconds`, `TimestampType`, `TimeType`, `IntType`, `JsonType`, `BsonType`, `VariantType`, `GeometryType`, `GeographyType`, `FileType` |
| schema | `SchemaElement`, `KeyValue`, `SortingColumn`, `TypeDefinedOrder`, `IEEE754TotalOrder`, `Int96TimestampOrder` |
| pages | `PageHeader`, `DataPageHeader`, `DataPageHeaderV2`, `DictionaryPageHeader`, `IndexPageHeader`, `PageEncodingStats` |
| statistics | `Statistics`, `SizeStatistics`, `BoundingBox`, `GeospatialStatistics` |
| chunks | `ColumnMetaData`, `ColumnChunk`, `RowGroup`, `FileMetaData` |
| page index | `PageLocation`, `OffsetIndex`, `ColumnIndex` |
| bloom filters | `BloomFilterHeader`, `SplitBlockAlgorithm`, `XxHash`, `Uncompressed` |
| encryption | `AesGcmV1`, `AesGcmCtrV1`, `EncryptionWithFooterKey`, `EncryptionWithColumnKey`, `FileCryptoMetaData` |

Each struct has `read[P: TProtocolReader](mut self, mut p: P) raises` and
`write[W: TProtocolWriter](self, mut p: W) raises`, so the same code works
over either protocol.

### Footer and page headers — `thrift.parquet_footer`

```mojo
read_parquet_file(path)             -> List[UInt8]
footer_length(file_bytes)           -> Int
read_footer(file_bytes)             -> FileMetaData
read_footer_bytes(metadata_bytes)   -> FileMetaData
write_footer(meta)                  -> List[UInt8]
write_footer_trailer(mut out, len)         # the i32 length + trailing PAR1
read_page_header(bytes, offset)     -> (PageHeader, header_len)
```

`read_footer` validates both magics, refuses a footer length that cannot fit
between them, and raises `encrypted footer unsupported` on `PARE`.
`read_page_header` returns the header's byte length, so the page body is at
`offset + header_len` for `compressed_page_size` bytes and the next header
follows it.

### CLI

```console
$ pixi run cli parquet-meta tests/fixtures/v2pages.parquet --pages
file:        tests/fixtures/v2pages.parquet
version:     2
created by:  parquet-cpp-arrow version 25.0.1
rows:        140
row groups:  2
schema nodes:4
key/value:   1 entr(y|ies)
  ARROW:schema (300 bytes)

schema:
message schema {
  optional int64 k;
  optional byte_array v (String);
  optional double d;
}

row group 0: 70 rows, 2255 bytes uncompressed, 3 column(s) offset=4
  [0] k: INT64 ZSTD PLAIN,RLE,RLE_DICTIONARY
      values=70 data@124 dict@4 compressed=260 uncompressed=717
      stats: min=0x0000000000000000 max=0x4500000000000000 nulls=0
      bloom@1851 length=80
      page @4 DICTIONARY_PAGE values=70 encoding=PLAIN header=17 …
      page @124 DATA_PAGE_V2 values=70 nulls=0 rows=70 encoding=RLE_DICTIONARY …
```

`--json` emits the same thing as JSON, `--pages` adds every page header.

## Tests

`pixi run test` — 49 tests, no network, no fixtures to download.

**Wire vectors from Apache Thrift itself.** `tools/gen_vectors.py` defines
thirteen small programs of protocol calls — i32 and i64 at their boundaries,
zigzag negatives, doubles including `-0.0`, both infinities and a NaN,
UTF-8 and raw binary, bools as field values and as list elements, field-id
deltas of 15 and 16 and a decreasing id and a negative one, nested structs
three deep, lists of structs, maps of lists, sets, UUIDs — and runs each
through `thrift.protocol.TCompactProtocol` and `TBinaryProtocol` over a
`TMemoryBuffer`. The same programs are emitted as Mojo, so the test replays
each one through our writers and compares the bytes, reads it back through
our readers and asserts every value, and `skip()`s it to prove the skipper
lands exactly on the end. **All 13 vectors are byte-identical to Apache
Thrift's own output in both protocols.**

Hostile input has its own tests: every prefix of a valid struct must raise;
list, map and binary headers claiming 2³¹ elements must be refused before
allocating; a tower of 74 struct headers must be refused rather than
overflow the stack; an 11-byte varint, an unknown compact type id and a
`struct_end` without a `struct_begin` must all raise.

**Parquet fixtures against two independent oracles.**
`tools/oracle_pyarrow.py` decodes each fixture's footer with the *Apache
Thrift compiler's* generated Python bindings (`thrift --gen py
spec/parquet.thrift`) — ground truth at the Thrift level, raw statistics
bytes included — and cross-checks every fact against
`pyarrow.parquet.ParquetFile(...).metadata` before writing the oracle. The
Mojo tests then reproduce that oracle from our own decode, line for line.
pyarrow's Python surface is lossy in exactly two documented places (it
reports `LZ4_RAW` as `LZ4`, and hides `TIMESTAMP_MILLIS`/`MICROS` converted
types on non-UTC columns); everything else has to agree or the tool refuses
to write an oracle.

| fixture | covers |
|---|---|
| `primitives.parquet` | every physical type — boolean, int32/64, float, double, byte array, fixed-len byte array — plus int8…uint64 logical widths, nulls everywhere, statistics on |
| `logical.parquet` | decimal, date, time ms/µs/ns, timestamp ms/µs/ns with and without UTC, string, large string |
| `extension.parquet` | the `UUID`, `Json` and `Float16` logical types |
| `nested.parquet` | list, list-of-list, map and struct — every repetition type and a 5-deep schema tree |
| `encodings.parquet` | dictionary, `PLAIN`, `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY`, `BYTE_STREAM_SPLIT` |
| `codecs.parquet` | one column each of uncompressed, snappy, gzip, zstd, lz4 (`LZ4_RAW` on disk) and brotli |
| `pageindex.parquet` | three row groups, `OffsetIndex` + `ColumnIndex`, several pages per chunk |
| `nostats.parquet` | statistics and page index disabled — the absent-optional path |
| `v2pages.parquet` | `DataPageHeaderV2`, two row groups, zstd, and a `BloomFilterHeader` |

Beyond metadata parity the suite:

- round-trips every footer: `write_footer(read_footer(x))` decodes to
  identical metadata, re-serialises to identical bytes, and survives a
  rebuild of the whole file including the 8-byte trailer;
- decodes **every page header of every column chunk of every fixture** and
  reconciles them against the chunk metadata — the headers plus bodies must
  tile `total_compressed_size` exactly, the data pages' `num_values` must
  sum to the chunk's, the dictionary page must be there iff
  `dictionary_page_offset` is, and the page counts must match
  `encoding_stats`;
- decodes the `OffsetIndex` and `ColumnIndex` of `pageindex.parquet` and
  checks each `PageLocation` against the page actually at that offset, plus
  the `BloomFilterHeader` of `v2pages.parquet`;
- rejects a short file, bad leading or trailing magic, `PARE`, an impossible
  footer length, and every truncation of a real footer;
- proves the generated structs are protocol-agnostic by round-tripping a
  real `FileMetaData` through `TBinaryProtocol`.

Both `default` (nightly) and `stable` (Mojo 1.0.0) run the same 49 tests on
Linux and macOS.

### Fixture provenance

`tests/fixtures/PROVENANCE.md` lists each file, its size and what it covers.
All nine are written by `pyarrow` 25.0.1 from `tools/gen_fixtures.py`; the
whole directory including oracles is 208 KiB. Regenerate with `pixi run
fixtures` (needs `uv`, and the Apache Thrift compiler — `pixi global install
thrift-compiler` — for the oracle).

One gap worth naming: pyarrow always writes `DECIMAL` over
`FIXED_LEN_BYTE_ARRAY`, so the decimal-over-`INT32`/`INT64` *physical*
layouts are not in a fixture. The metadata path is identical (`DecimalType`
is just precision and scale), and `SchemaElement.precision`/`scale` are
exercised by the fixed-len columns.

## Performance

`pixi run bench`, single-threaded, Apple M4:

| | |
|---|---|
| `read_footer` on a real 1,000-column × 50-row-group pyarrow file (50,000 column chunks, 5.0 MiB footer) | **78 ms** — 640k chunks/s, 64 MB/s |
| `write_footer` for the same metadata | **8 ms** |
| `read_footer` on the synthesised equivalent the bench builds (3.9 MiB) | 52 ms — 962k chunks/s |
| `skip()` over a 77 KiB footer, no allocation | 0.65 ms — 121 MB/s |
| 10,000 zigzag i64 varints | 0.19 ms — 53 M values/s |

The bench synthesises its wide footer in Mojo rather than shipping a
multi-megabyte fixture; the real-file numbers above were measured
separately against a pyarrow-written file and are the honest ones to quote.
Decoding is allocation-bound — a footer that size is 50,000 `ColumnChunk`
structs with their `Statistics` and `path_in_schema` — which is why pure
`skip()` is about twice as fast per byte.

A normal footer is microseconds: ten columns in one row group parse in
0.011 ms.

## Regenerating the Parquet structs

`src/thrift/parquet_types.mojo` is generated. `tools/gen_types.py` is a
small parser for the slice of the Thrift IDL that Parquet uses — `enum`,
`struct`, `union`, the primitives, `list<T>`, `binary` — and an emitter that
produces one Mojo struct per declaration in a deliberately regular style. It
raises on anything it does not recognise rather than emitting something
subtly wrong.

```console
$ curl -o spec/parquet.thrift \
    https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift
$ pixi run gen-types
spec/parquet.thrift: 8 enums, 53 structs, 8 unions -> src/thrift/parquet_types.mojo
```

The IDL comments become Mojo docstrings, so the generated file carries
Parquet's own documentation for every field.

## Using it from another tin

`parquet.mojo` and anything else in magmalake reaches everything through the
package root:

```mojo
from thrift import (
    # protocols
    TCompactProtocolReader, TCompactProtocolWriter,
    TBinaryProtocolReader, TBinaryProtocolWriter,
    TProtocolReader, TProtocolWriter,
    T_STOP, T_STRUCT, T_LIST, T_I32, type_name,
    # footer and page headers
    read_parquet_file, read_footer, read_footer_bytes, footer_length,
    read_page_header, write_footer, write_footer_trailer,
    # metadata structs — all 69 of them are re-exported
    FileMetaData, RowGroup, ColumnChunk, ColumnMetaData, SchemaElement,
    Statistics, PageHeader, OffsetIndex, ColumnIndex, BloomFilterHeader,
    Type, Encoding, CompressionCodec, PageType, LogicalType, ConvertedType,
    FieldRepetitionType,
)
```

Submodules are `thrift.protocol`, `thrift.parquet_types` and
`thrift.parquet_footer` if a narrower import is wanted.

## Install

As a pixi source dependency:

```toml
[dependencies]
thrift-mojo = { git = "https://github.com/magmalake/thrift.mojo" }
```

Note that the package's `.mojopkg` is built with stable Mojo 1.0.0 and the
nightly compiler will not load it, so a nightly consumer should put the
source on the include path instead — `-I ../thrift.mojo/src` — and check the
repository out next to its own.

## Tasks

| task | what it does |
|---|---|
| `pixi run test` | the test suite (`-e stable` for Mojo 1.0.0) |
| `pixi run bench` | the benchmarks above |
| `pixi run cli parquet-meta f.parquet` | build and run the CLI |
| `pixi run gen-types` | regenerate `parquet_types.mojo` from `spec/parquet.thrift` |
| `pixi run fixtures` | regenerate the fixtures, their oracles and the wire vectors |

## License

Apache-2.0. `spec/parquet.thrift` is Apache Software Foundation source,
included unmodified under the same licence for provenance.

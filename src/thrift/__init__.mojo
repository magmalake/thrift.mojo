"""thrift.mojo — Apache Thrift *serialization* in pure Mojo.

Part of magmalake: data lake building blocks in Mojo.

No RPC, no IDL compiler at run time — just the two wire protocols that matter
for reading columnar files, plus hand-checkable Mojo structs for the Apache
Parquet metadata schema.

```mojo
from thrift import TCompactProtocolReader, T_STOP

var r = TCompactProtocolReader(Span(footer_bytes))
r.read_struct_begin()
while True:
    var head = r.read_field_begin()
    if head[0] == T_STOP:
        break
    r.skip(head[0])
    r.read_field_end()
r.read_struct_end()
```

Everything here is dependency-free: the standard library and nothing else.
"""

from thrift.protocol import (
    BINARY_VERSION_1,
    BINARY_VERSION_MASK,
    COMPACT_PROTOCOL_ID,
    COMPACT_VERSION,
    MAX_SKIP_DEPTH,
    MESSAGE_CALL,
    MESSAGE_EXCEPTION,
    MESSAGE_ONEWAY,
    MESSAGE_REPLY,
    T_BINARY,
    T_BOOL,
    T_BYTE,
    T_DOUBLE,
    T_I08,
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
    T_VOID,
    TBinaryProtocolReader,
    TBinaryProtocolWriter,
    TCompactProtocolReader,
    TCompactProtocolWriter,
    TProtocolReader,
    TProtocolWriter,
    skip_value,
    type_name,
    unzigzag_i32,
    unzigzag_i64,
    zigzag_i32,
    zigzag_i64,
)

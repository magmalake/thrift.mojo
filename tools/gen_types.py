#!/usr/bin/env python3
"""Generate src/thrift/parquet_types.mojo from spec/parquet.thrift.

A small hand-rolled parser for the slice of the Thrift IDL that Parquet
actually uses — `enum`, `struct`, `union`, the primitive types, `list<T>`
and `binary`/`string` — plus a code emitter that produces one Mojo struct
per Thrift struct with `read(protocol)` / `write(protocol)` methods written
against the `TProtocolReader` / `TProtocolWriter` traits.

    python3 tools/gen_types.py spec/parquet.thrift src/thrift/parquet_types.mojo

The Parquet IDL has no `map<>`, `set<>`, `i8` outside two fields, service or
typedef declarations; the parser raises on anything it does not recognise
rather than emitting something subtly wrong.
"""

import re
import sys
import textwrap

# ── the IDL parser ─────────────────────────────────────────────────────────

PRIMITIVES = {
    "bool": ("Bool", "T_BOOL", "read_bool", "write_bool", "False"),
    "byte": ("Int8", "T_BYTE", "read_byte", "write_byte", "Int8(0)"),
    "i8": ("Int8", "T_BYTE", "read_byte", "write_byte", "Int8(0)"),
    "i16": ("Int16", "T_I16", "read_i16", "write_i16", "Int16(0)"),
    "i32": ("Int32", "T_I32", "read_i32", "write_i32", "Int32(0)"),
    "i64": ("Int64", "T_I64", "read_i64", "write_i64", "Int64(0)"),
    "double": ("Float64", "T_DOUBLE", "read_double", "write_double", "Float64(0)"),
    "string": ("String", "T_STRING", "read_string", "write_string", "String()"),
    "binary": (
        "List[UInt8]",
        "T_STRING",
        "read_binary",
        "write_binary",
        "List[UInt8]()",
    ),
    "uuid": ("List[UInt8]", "T_UUID", "read_uuid", "write_uuid", "List[UInt8]()"),
}


class Enum:
    def __init__(self, name, doc):
        self.name = name
        self.doc = doc
        self.values = []  # (name, value, doc)


class Field:
    def __init__(self, fid, req, ftype, name, default, doc):
        self.fid = fid
        self.req = req  # "required" | "optional"
        self.ftype = ftype  # ("prim", n) | ("list", inner) | ("ref", name)
        self.name = name
        self.default = default
        self.doc = doc


class Struct:
    def __init__(self, name, doc, is_union):
        self.name = name
        self.doc = doc
        self.is_union = is_union
        self.fields = []


def strip_comments(text):
    """Remove comments, keeping each doc comment attached to what follows."""
    out = []
    i = 0
    n = len(text)
    pending = []
    while i < n:
        if text.startswith("/*", i):
            j = text.index("*/", i) + 2
            body = text[i:j]
            lines = []
            for ln in body.splitlines():
                ln = ln.strip()
                ln = re.sub(r"^/\*+", "", ln)
                ln = re.sub(r"\*+/$", "", ln)
                ln = re.sub(r"^\*+ ?", "", ln)
                lines.append(ln.rstrip())
            while lines and not lines[0]:
                lines.pop(0)
            while lines and not lines[-1]:
                lines.pop()
            pending.append("\n".join(lines))
            out.append("\x00%d\x00" % (len(pending) - 1))
            i = j
        elif text.startswith("//", i):
            j = text.find("\n", i)
            if j < 0:
                j = n
            pending.append(text[i + 2 : j].strip())
            out.append("\x00%d\x00" % (len(pending) - 1))
            i = j
        else:
            out.append(text[i])
            i += 1
    return "".join(out), pending


DOC_RE = re.compile(r"\x00(\d+)\x00")


def take_doc(chunk, docs):
    """Pull the doc comments out of `chunk`, returning (clean, doc-text)."""
    found = [docs[int(m)] for m in DOC_RE.findall(chunk)]
    clean = DOC_RE.sub(" ", chunk)
    # Only the comment block immediately before the declaration documents it;
    # earlier ones belong to whatever came before (the file's licence header,
    # a previous member, ...).
    found = [d for d in found if d]
    return clean, (found[-1] if found else "")


def parse_type(tok):
    tok = tok.strip()
    m = re.fullmatch(r"list\s*<\s*(.+?)\s*>", tok)
    if m:
        return ("list", parse_type(m.group(1)))
    if tok in PRIMITIVES:
        return ("prim", tok)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tok):
        return ("ref", tok)
    raise ValueError("unsupported thrift type %r" % tok)


def parse(path):
    raw = open(path, encoding="utf-8").read()
    text, docs = strip_comments(raw)
    decls = []
    pos = 0
    pattern = re.compile(r"\b(enum|struct|union)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
    while True:
        m = pattern.search(text, pos)
        if not m:
            break
        kind, name = m.group(1), m.group(2)
        # The doc comment is whatever sits between the previous decl and here.
        _, doc = take_doc(text[pos : m.start()], docs)
        depth = 1
        i = m.end()
        while depth:
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
            i += 1
        body = text[m.end() : i - 1]
        pos = i
        if kind == "enum":
            decls.append(parse_enum(name, doc, body, docs))
        else:
            decls.append(parse_struct(name, doc, body, docs, kind == "union"))
    return decls


def parse_enum(name, doc, body, docs):
    e = Enum(name, doc)
    last = 0
    for chunk in body.split(";"):
        clean, fdoc = take_doc(chunk, docs)
        clean = clean.strip().rstrip(",").strip()
        if not clean:
            continue
        m = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:=\s*(-?\d+))?", clean)
        if not m:
            raise ValueError("bad enum member %r in %s" % (clean, name))
        val = int(m.group(2)) if m.group(2) else last
        last = val + 1
        e.values.append((m.group(1), val, fdoc))
    return e


FIELD_RE = re.compile(
    r"^(\d+)\s*:\s*(required|optional)?\s*(.+?)\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)\s*(?:=\s*(.+?))?$"
)


def parse_struct(name, doc, body, docs, is_union):
    s = Struct(name, doc, is_union)
    # Fields end at ';' or at a newline; normalise by splitting on ';' and
    # then on newlines, since parquet.thrift is inconsistent about the ';'.
    parts = []
    for piece in body.split(";"):
        for line in piece.split("\n"):
            parts.append(line)
    for chunk in parts:
        clean, fdoc = take_doc(chunk, docs)
        clean = clean.strip().rstrip(",").strip()
        if not clean:
            continue
        m = FIELD_RE.match(clean)
        if not m:
            raise ValueError("bad field %r in %s" % (clean, name))
        fid = int(m.group(1))
        req = m.group(2) or ("optional" if is_union else "required")
        ftype = parse_type(m.group(3))
        s.fields.append(Field(fid, req, ftype, m.group(4), m.group(5), fdoc))
    s.fields.sort(key=lambda f: f.fid)
    return s


# ── the Mojo emitter ───────────────────────────────────────────────────────

RESERVED = {"type", "in", "for", "var", "ref", "mut", "out", "deinit", "read"}


def mojo_field(name):
    return name + "_" if name in RESERVED else name


def wrap_doc(doc, indent, width=76):
    """Reflow an IDL comment into a Mojo docstring the linter is happy with."""
    if not doc:
        return []
    text = " ".join(doc.split())
    if not text:
        return []
    # Mojo's doc linter wants a summary that starts with a capital (or a
    # non-letter) and ends with terminal punctuation. Parquet's IDL comments
    # are prose fragments, so nudge them into shape.
    if text[0].isalpha() and text[0].islower():
        text = text[0].upper() + text[1:]
    if text[-1] not in ".!?`":
        text += "."
    pad = " " * indent
    lines = textwrap.wrap(text, width - indent - 3)
    if len(lines) == 1 and len(lines[0]) + indent + 6 < width:
        return ['%s"""%s"""' % (pad, lines[0].replace('"""', "'''"))]
    out = ['%s"""%s' % (pad, lines[0].replace('"""', "'''"))]
    for ln in lines[1:]:
        out.append(pad + ln.replace('"""', "'''"))
    out.append(pad + '"""')
    return out


class Emitter:
    def __init__(self, decls):
        self.decls = decls
        self.enums = {d.name: d for d in decls if isinstance(d, Enum)}
        self.structs = {d.name: d for d in decls if isinstance(d, Struct)}
        self.lines = []

    def a(self, s=""):
        self.lines.append(s)

    # -- type helpers ------------------------------------------------------

    def mojo_type(self, t):
        kind = t[0]
        if kind == "prim":
            return PRIMITIVES[t[1]][0]
        if kind == "list":
            return "List[%s]" % self.mojo_type(t[1])
        name = t[1]
        if name in self.enums:
            return name
        return name

    def ttype_const(self, t):
        if t[0] == "prim":
            return PRIMITIVES[t[1]][1]
        if t[0] == "list":
            return "T_LIST"
        if t[1] in self.enums:
            return "T_I32"
        return "T_STRUCT"

    def is_scalar(self, t):
        return t[0] == "prim" and t[1] in (
            "bool", "byte", "i8", "i16", "i32", "i64", "double",
        )

    def default_of(self, t):
        if t[0] == "prim":
            return PRIMITIVES[t[1]][4]
        if t[0] == "list":
            return "List[%s]()" % self.mojo_type(t[1])
        if t[1] in self.enums:
            return "%s(0)" % t[1]
        return "%s()" % t[1]

    def read_expr(self, t, var, indent):
        """Statements that put one decoded value of type `t` into `var`."""
        pad = " " * indent
        out = []
        if t[0] == "prim":
            out.append("%s%s = p.%s()" % (pad, var, PRIMITIVES[t[1]][2]))
        elif t[0] == "list":
            inner = t[1]
            elem = "_e%d" % indent
            out.append("%svar _h%d = p.read_list_begin()" % (pad, indent))
            out.append(
                "%sif _h%d[0] != %s:" % (pad, indent, self.ttype_const(inner))
            )
            out.append(
                '%s    raise Error(String("parquet: %s element type ",'
                % (pad, var)
            )
            out.append("%s        type_name(_h%d[0])))" % (pad, indent))
            out.append("%s%s = %s" % (pad, var, self.default_of(t)))
            out.append("%s%s.reserve(_h%d[1])" % (pad, var, indent))
            out.append("%sfor _ in range(_h%d[1]):" % (pad, indent))
            out.append(
                "%s    var %s = %s" % (pad, elem, self.default_of(inner))
            )
            out.extend(self.read_expr(inner, elem, indent + 4))
            xfer = "" if self.is_scalar(inner) else "^"
            out.append("%s    %s.append(%s%s)" % (pad, var, elem, xfer))
            out.append("%sp.read_list_end()" % pad)
        elif t[1] in self.enums:
            out.append("%s%s = %s(p.read_i32())" % (pad, var, t[1]))
        else:
            out.append("%s%s.read(p)" % (pad, var))
        return out

    def write_expr(self, t, var, indent):
        pad = " " * indent
        out = []
        if t[0] == "prim":
            name = PRIMITIVES[t[1]][3]
            if t[1] in ("binary", "uuid"):
                out.append("%sp.%s(Span(%s))" % (pad, name, var))
            else:
                out.append("%sp.%s(%s)" % (pad, name, var))
        elif t[0] == "list":
            inner = t[1]
            elem = "_e%d" % indent
            out.append(
                "%sp.write_list_begin(%s, len(%s))"
                % (pad, self.ttype_const(inner), var)
            )
            out.append("%sfor ref %s in %s:" % (pad, elem, var))
            out.extend(self.write_expr(inner, elem, indent + 4))
            out.append("%sp.write_list_end()" % pad)
        elif t[1] in self.enums:
            out.append("%sp.write_i32(%s.value)" % (pad, var))
        else:
            out.append("%s%s.write(p)" % (pad, var))
        return out

    # -- declarations ------------------------------------------------------

    def emit_enum(self, e):
        a = self.a
        a(
            "struct %s(Copyable, Defaultable, Equatable, ImplicitlyCopyable, Movable, Writable):"
            % e.name
        )
        for ln in wrap_doc(
            e.doc
            or ("The Parquet `%s` enum." % e.name),
            4,
        ):
            a(ln)
        a("")
        a("    var value: Int32")
        a("")
        for name, val, doc in e.values:
            a("    comptime %s = Self(%d)" % (name, val))
        a("")
        a("    def __init__(out self):")
        a("        self.value = %d" % (e.values[0][1] if e.values else 0))
        a("")
        a("    @implicit")
        a("    def __init__(out self, value: Int32):")
        a("        self.value = value")
        a("")
        a("    def __eq__(self, other: Self) -> Bool:")
        a("        return self.value == other.value")
        a("")
        a("    def __ne__(self, other: Self) -> Bool:")
        a("        return self.value != other.value")
        a("")
        a("    def name(self) -> String:")
        a('        """The IDL spelling, or `%s(n)` for a value we do not know."""'
          % e.name)
        for name, val, doc in e.values:
            a("        if self.value == %d:" % val)
            a('            return String("%s")' % name)
        a('        return String("%s(", self.value, ")")' % e.name)
        a("")
        a("    def write_to(self, mut writer: Some[Writer]):")
        a("        writer.write(self.name())")
        a("")
        a("")

    def emit_struct(self, s):
        a = self.a
        a("struct %s(Copyable, Defaultable, Movable):" % s.name)
        doc = s.doc or ("The Parquet `%s` struct." % s.name)
        if s.is_union:
            doc = (doc + "\n\nA Thrift union: exactly one member is set.").strip()
        for ln in wrap_doc(doc, 4):
            a(ln)
        a("")
        if not s.fields:
            a("    var _empty: Bool")
            a('    """This struct has no fields; it exists as a union tag."""')
        for f in s.fields:
            ty = self.mojo_type(f.ftype)
            if f.req == "optional":
                ty = "Optional[%s]" % ty
            a("    var %s: %s" % (mojo_field(f.name), ty))
            for ln in wrap_doc(f.doc, 4):
                a(ln)
        a("")
        # default constructor
        a("    def __init__(out self):")
        if not s.fields:
            a("        self._empty = False")
        for f in s.fields:
            if f.req == "optional":
                a("        self.%s = None" % mojo_field(f.name))
            else:
                a(
                    "        self.%s = %s"
                    % (mojo_field(f.name), self.default_value(f))
                )
        a("")
        # copy / move
        a("    def __init__(out self, *, copy: Self):")
        if not s.fields:
            a("        self._empty = copy._empty")
        for f in s.fields:
            n = mojo_field(f.name)
            a("        self.%s = copy.%s.copy()" % (n, n))
        a("")
        a("    def __init__(out self, *, deinit move: Self):")
        if not s.fields:
            a("        self._empty = move._empty")
        for f in s.fields:
            n = mojo_field(f.name)
            xfer = "" if (f.req == "required" and self.is_scalar(f.ftype)) else "^"
            a("        self.%s = move.%s%s" % (n, n, xfer))
        a("")
        self.emit_read(s)
        self.emit_write(s)
        a("")

    def default_value(self, f):
        if f.default is not None:
            d = f.default.strip()
            if d in ("true", "false"):
                return "True" if d == "true" else "False"
            if re.fullmatch(r"-?\d+", d):
                return "%s(%s)" % (self.mojo_type(f.ftype), d)
        return self.default_of(f.ftype)

    def emit_read(self, s):
        a = self.a
        a("    def read[P: TProtocolReader, //](mut self, mut p: P) raises:")
        a('        """Decode one `%s` from `p`, skipping fields we do not know."""'
          % s.name)
        required = [f for f in s.fields if f.req == "required"]
        for f in required:
            a("        var _seen_%s = False" % mojo_field(f.name))
        if s.is_union:
            a("        var _set_count = 0")
        a("        p.read_struct_begin()")
        a("        while True:")
        a("            var _head = p.read_field_begin()")
        a("            if _head[0] == T_STOP:")
        a("                break")
        first = True
        for f in s.fields:
            n = mojo_field(f.name)
            a(
                "            %s _head[1] == %d and _head[0] == %s:"
                % ("if" if first else "elif", f.fid, self.ttype_const(f.ftype))
            )
            first = False
            if f.req == "optional":
                a("                var _v = %s" % self.default_of(f.ftype))
                self.lines.extend(self.read_expr(f.ftype, "_v", 16))
                # Scalars are trivial register types; `^` on them is a warning.
                xfer = "" if self.is_scalar(f.ftype) else "^"
                a("                self.%s = _v%s" % (n, xfer))
                if s.is_union:
                    a("                _set_count += 1")
            else:
                self.lines.extend(self.read_expr(f.ftype, "self." + n, 16))
                a("                _seen_%s = True" % n)
        if first:
            a("            p.skip(_head[0])")
        else:
            a("            else:")
            a("                p.skip(_head[0])")
        a("            p.read_field_end()")
        a("        p.read_struct_end()")
        for f in required:
            n = mojo_field(f.name)
            a("        if not _seen_%s:" % n)
            a(
                '            raise Error(String("parquet.%s: missing required'
                ' field %s"))' % (s.name, f.name)
            )
        if s.is_union and s.fields:
            a("        if _set_count != 1:")
            a(
                '            raise Error(String("parquet.%s: a union must have'
                ' exactly one member set, got ", _set_count))' % s.name
            )
        a("")

    def emit_write(self, s):
        a = self.a
        a("    def write[W: TProtocolWriter, //](self, mut p: W) raises:")
        a('        """Encode this `%s`, fields in ascending id order."""' % s.name)
        a("        p.write_struct_begin()")
        for f in s.fields:
            n = mojo_field(f.name)
            if f.req == "optional":
                a("        if self.%s:" % n)
                a(
                    "            p.write_field_begin(%s, %d)"
                    % (self.ttype_const(f.ftype), f.fid)
                )
                a("            ref _v = self.%s.value()" % n)
                self.lines.extend(self.write_expr(f.ftype, "_v", 12))
                a("            p.write_field_end()")
            else:
                a(
                    "        p.write_field_begin(%s, %d)"
                    % (self.ttype_const(f.ftype), f.fid)
                )
                self.lines.extend(self.write_expr(f.ftype, "self." + n, 8))
                a("        p.write_field_end()")
        a("        p.write_field_stop()")
        a("        p.write_struct_end()")
        a("")

    def emit_write_to(self, s):
        a = self.a
        a("    def write_to(self, mut writer: Some[Writer]):")
        a('        writer.write("%s(")' % s.name)
        sep = ""
        for f in s.fields:
            n = mojo_field(f.name)
            if f.req == "optional":
                a("        if self.%s:" % n)
                a('            writer.write("%s%s=", ' % (sep, f.name))
                a("                self.%s.value())" % n)
            else:
                a('        writer.write("%s%s=", self.%s)' % (sep, f.name, n))
            sep = ", "
        a('        writer.write(")")')

    def emit(self):
        a = self.a
        a('"""The Apache Parquet metadata schema as Mojo structs.')
        a("")
        a("GENERATED by tools/gen_types.py from spec/parquet.thrift — do not")
        a("edit by hand. Regenerate with `pixi run gen-types` after dropping a")
        a("newer parquet.thrift into spec/.")
        a("")
        a("Every Thrift `struct`, `union` and `enum` in the IDL is here.")
        a("Optional fields are `Optional[T]`; required fields are validated on")
        a("read; unknown field ids are skipped so a file written by a newer")
        a("Parquet still decodes; `write` emits fields in ascending id order.")
        a("")
        a("Enums are open: an unknown value round-trips as its integer rather")
        a("than raising, because Parquet adds codecs and encodings over time.")
        a('"""')
        a("")
        a("from thrift.protocol import (")
        for n in (
            "T_BOOL",
            "T_BYTE",
            "T_DOUBLE",
            "T_I16",
            "T_I32",
            "T_I64",
            "T_LIST",
            "T_STOP",
            "T_STRING",
            "T_STRUCT",
            "T_UUID",
            "TProtocolReader",
            "TProtocolWriter",
            "type_name",
        ):
            a("    %s," % n)
        a(")")
        a("")
        a("")
        for d in self.decls:
            if isinstance(d, Enum):
                self.emit_enum(d)
        for d in self.decls:
            if isinstance(d, Struct):
                self.emit_struct(d)
        return "\n".join(self.lines).rstrip() + "\n"


def main():
    src, dst = sys.argv[1], sys.argv[2]
    decls = parse(src)
    text = Emitter(decls).emit()
    with open(dst, "w", encoding="utf-8") as fh:
        fh.write(text)
    n_enum = sum(1 for d in decls if isinstance(d, Enum))
    n_union = sum(1 for d in decls if isinstance(d, Struct) and d.is_union)
    n_struct = sum(
        1 for d in decls if isinstance(d, Struct) and not d.is_union
    )
    sys.stderr.write(
        "%s: %d enums, %d structs, %d unions -> %s\n"
        % (src, n_enum, n_struct, n_union, dst)
    )


if __name__ == "__main__":
    main()

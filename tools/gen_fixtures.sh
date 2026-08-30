#!/usr/bin/env bash
# Regenerate tests/fixtures/*.parquet and their oracles.
#
# Needs `uv` on PATH (it builds a throwaway venv with pyarrow and the Apache
# Thrift Python runtime) and the Apache Thrift *compiler* for the oracle:
#
#     pixi global install thrift-compiler      # or conda install thrift-compiler
#
# The generated files are committed, so nobody needs any of this to run the
# test suite.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV="${TMPDIR:-/tmp}/thrift-mojo-fixtures-venv"
if [ ! -d "$VENV" ]; then
  uv venv "$VENV"
fi
uv pip install -q --python "$VENV/bin/python" pyarrow thrift numpy

command -v thrift >/dev/null || {
  echo "the Apache Thrift compiler is not on PATH; install it with" >&2
  echo "  pixi global install thrift-compiler" >&2
  exit 1
}

"$VENV/bin/python" tools/gen_fixtures.py tests/fixtures
"$VENV/bin/python" tools/oracle_pyarrow.py tests/fixtures
"$VENV/bin/python" tools/gen_vectors.py > tests/vectors.mojo
echo "fixtures, oracles and wire vectors regenerated"

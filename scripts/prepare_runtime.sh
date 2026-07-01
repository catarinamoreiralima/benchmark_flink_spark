#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p /tmp/gsdgrad02-compose/jobs /tmp/gsdgrad02-compose/results
cp -R "$ROOT_DIR"/src/jobs/* /tmp/gsdgrad02-compose/jobs/

mkdir -p "$ROOT_DIR/results/raw"

echo "Runtime preparado:"
echo "- jobs sincronizados em /tmp/gsdgrad02-compose/jobs"
echo "- resultados locais em $ROOT_DIR/results/raw"

#!/bin/sh
# Rebuild docs/final-report.docx from docs/final-report.md with the original report's styling.
# Requires pandoc. Run from anywhere: sh docs/build/build.sh
set -e
cd "$(dirname "$0")/.."
pandoc final-report.md \
  -f markdown-implicit_figures-auto_identifiers \
  -t docx \
  --resource-path=. \
  --reference-doc=build/reference.docx \
  --lua-filter=build/report.lua \
  -o final-report.docx
echo "wrote docs/final-report.docx"

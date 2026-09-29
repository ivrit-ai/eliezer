#!/bin/bash
# Run every end-to-end suite; exit non-zero if any check fails. See tests/README.md.
cd "$(dirname "$0")/.." || exit 1
status=0
for suite in ${SUITES:-tests/test_queue.py tests/test_whatsapp.py tests/test_telegram.py}; do
  echo "=== $suite"
  output=$(python "$suite" 2>&1)
  rc=$?
  grep -Ev '^PASS ' <<< "$output"
  [ "$rc" -eq 0 ] || status=1
done
exit $status

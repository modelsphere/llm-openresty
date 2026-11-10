#!/usr/bin/env bash
# Run every Lua unit test (test/utest_*.lua) with the resty CLI; used by CI (.github/workflows/test.yml)
# and locally:
#   bash test/run_lua_unit_tests.sh [resty]        default resty: $RESTY, else `resty` on PATH
# Each test exits non-zero on failure (os.exit(1), or an assert error). Exits non-zero if any test fails,
# or if no test was found -- a runner that finds nothing must not pass.
# Most tests run from the repo root with `-I lua`; the ones below need their own invocation.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESTY=${1:-${RESTY:-resty}}
command -v "$RESTY" >/dev/null || { echo "no resty CLI ($RESTY)"; exit 2; }
cd "$ROOT"
pass=0 fail=0 failed=""
for t in test/utest_*.lua; do
  [ -f "$t" ] || continue
  case $t in
    test/utest_bodylog_gate.lua) out=$("$RESTY" -I lua --shdict 'bodylog_ctl 1m' "$t" 2>&1) ;;
    test/utest_window_stat.lua)  out=$(cd test && "$RESTY" -I ../lua "${t#test/}" 2>&1) ;;  # loads ../lua/util.lua
    *)                           out=$("$RESTY" -I lua "$t" 2>&1) ;;
  esac
  rc=$?
  # resty prints a _G write-guard warning with a traceback for each module that sets a global; not a failure
  summary=$(printf '%s\n' "$out" | grep -vE "_G write guard|stack traceback|in main chunk|file_gen|init_worker_by_lua|xpcall|^[[:space:]]*\[C\]|^[[:space:]]*(test|lua)/" | grep . | tail -1)
  if [ $rc = 0 ]; then
    pass=$((pass + 1)); printf 'ok    %-46s %s\n' "$t" "$summary"
  else
    fail=$((fail + 1)); failed="$failed $t"
    printf 'FAIL  %-46s rc=%s\n' "$t" "$rc"; printf '%s\n' "$out" | sed 's/^/      /' | tail -30
  fi
done
echo "lua unit tests: $pass passed, $fail failed${failed:+ (}${failed# }${failed:+)}"
[ $((pass + fail)) -gt 0 ] || { echo "no test/utest_*.lua found"; exit 1; }
[ $fail = 0 ]

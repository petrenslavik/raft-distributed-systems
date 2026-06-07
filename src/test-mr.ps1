# Build the Lab 1 MapReduce binaries, then run the mr test suite.
#
# The tests in mr/ exec the compiled binaries in main/, so they must be
# rebuilt after every code change. This script does that for you, then runs
# `go test`. Any extra arguments are passed straight through to `go test`.
#
# Usage (from the src directory):
#   ./test-mr.ps1                      # run ALL mr tests, verbose
#   ./test-mr.ps1 -run TestWc          # run a single test
#   ./test-mr.ps1 -run 'TestWc|TestIndexer'   # run several (regex)
#   ./test-mr.ps1 -count=1             # disable test caching (force a fresh run)
#   ./test-mr.ps1 -timeout 120s        # override the timeout

$ErrorActionPreference = "Stop"

# 1. Rebuild the binaries the tests exec.
& "$PSScriptRoot\build-mr.ps1"

# 2. Run the tests (any args passed to this script are forwarded to `go test`).
Set-Location "$PSScriptRoot\mr"
go test -v @args

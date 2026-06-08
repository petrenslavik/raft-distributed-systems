# Build the kvsrv daemon binary and run the Lab 2 (kvsrv1) tests.
#
# The 2026 tester runs each server as a separate process (main/kvsrv1d.exe)
# and talks to it over a socket, so the daemon must be rebuilt after any
# server-side change. Stray daemons left by a hung/timed-out run can lock the
# binary or hold sockets, so we kill them first.
#
# Usage (from the src directory):
#   ./test-kv.ps1                  # all kvsrv1 tests
#   ./test-kv.ps1 -run Reliable    # filtered (any go test flags pass through)
#   ./test-kv.ps1 -count=1         # bypass test cache
#
# For the lock tests (the daemon is already built by this script):
#   cd kvsrv1/lock; go test -v

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Kill any stray server daemons from a previous run (ignore if none).
Get-Process kvsrv1d -ErrorAction SilentlyContinue | Stop-Process -Force

go build -o main/kvsrv1d.exe main/kvsrv1d.go

Set-Location "$PSScriptRoot\kvsrv1"
go test -v @args

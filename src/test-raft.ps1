# Build the raft daemon and run the Lab 3 (raft1) tests with the race detector.
#
# The tester spawns main/raft1d.exe, so it's rebuilt on every run; stray
# daemons from a hung/timed-out run are killed first. -race is on by default
# (the lab strongly recommends it; Raft is heavily concurrent).
#
# Usage (from the src directory):
#   ./test-raft.ps1 -run 3A            # part 3A only
#   ./test-raft.ps1 -run 3A -count=5   # run 5x to shake out flaky races
#   ./test-raft.ps1 -run TestInitialElection3A
#   ./test-raft.ps1                    # all raft tests (slow)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Kill stray daemons from a previous run (ignore if none).
Get-Process raft1d -ErrorAction SilentlyContinue | Stop-Process -Force

go build -race -o main/raft1d.exe main/raft1d.go

Set-Location "$PSScriptRoot\raft1"
go test -race -v @args

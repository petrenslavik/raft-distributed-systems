# Build the Lab 1 (MapReduce) helper binaries on Windows-native Go.
#
# The stock lab uses `make mr`, which relies on `make` and on
# -buildmode=plugin (unsupported on Windows). This script is the Windows
# replacement: it builds the three single-file `main` programs into .exe
# binaries that the tests in mr/ exec. The map/reduce apps are compiled
# into the binaries via the mrapps/apps registry, so there are no plugins.
#
# Usage (from the src directory):
#   ./build-mr.ps1          # build the binaries
#   cd mr; go test -v       # run the Lab 1 test suite
#
# Note: -race is intentionally omitted because it needs a C compiler (gcc)
# on Windows. Add it once you have one installed if you want race detection.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

go build -o main/mrsequential.exe main/mrsequential.go
go build -o main/mrcoordinator.exe main/mrcoordinator.go
go build -o main/mrworker.exe main/mrworker.go

Write-Host "Built main/mrsequential.exe, main/mrcoordinator.exe, main/mrworker.exe"

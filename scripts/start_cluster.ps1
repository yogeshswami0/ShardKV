$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir
$BuildDir = "$ProjectDir\build\Debug"

if (-not (Test-Path "$BuildDir\shard-kv.exe")) {
    Write-Host "Build not found. Building..."
    cmake -B "$ProjectDir\build" -S "$ProjectDir" -DCMAKE_BUILD_TYPE=Debug
    cmake --build "$ProjectDir\build" --config Debug --parallel
}

New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node1\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node1\wal" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node2\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node2\wal" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node3\data" | Out-Null
New-Item -ItemType Directory -Force -Path "$env:TEMP\shard\node3\wal" | Out-Null

Write-Host "Starting node 1..."
$env:SHARD_NODE_ID=1
$env:SHARD_LISTEN_ADDR="127.0.0.1:7071"
$env:SHARD_PEERS="127.0.0.1:7072,127.0.0.1:7073"
$env:SHARD_DATA_DIR="$env:TEMP\shard\node1\data"
$env:SHARD_WAL_DIR="$env:TEMP\shard\node1\wal"
$env:SHARD_LOG_LEVEL="INFO"
$proc1 = Start-Process -FilePath "$BuildDir\shard-kv.exe" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

Write-Host "Starting node 2..."
$env:SHARD_NODE_ID=2
$env:SHARD_LISTEN_ADDR="127.0.0.1:7072"
$env:SHARD_PEERS="127.0.0.1:7071,127.0.0.1:7073"
$env:SHARD_DATA_DIR="$env:TEMP\shard\node2\data"
$env:SHARD_WAL_DIR="$env:TEMP\shard\node2\wal"
$env:SHARD_LOG_LEVEL="INFO"
$proc2 = Start-Process -FilePath "$BuildDir\shard-kv.exe" -PassThru -NoNewWindow
Start-Sleep -Seconds 1

Write-Host "Starting node 3..."
$env:SHARD_NODE_ID=3
$env:SHARD_LISTEN_ADDR="127.0.0.1:7073"
$env:SHARD_PEERS="127.0.0.1:7071,127.0.0.1:7072"
$env:SHARD_DATA_DIR="$env:TEMP\shard\node3\data"
$env:SHARD_WAL_DIR="$env:TEMP\shard\node3\wal"
$env:SHARD_LOG_LEVEL="INFO"
$proc3 = Start-Process -FilePath "$BuildDir\shard-kv.exe" -PassThru -NoNewWindow

Write-Host ""
Write-Host "Cluster started!"
Write-Host "  Node 1: 127.0.0.1:7071"
Write-Host "  Node 2: 127.0.0.1:7072"
Write-Host "  Node 3: 127.0.0.1:7073"
Write-Host ""
Write-Host "Press Ctrl+C to stop all nodes"

try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    Write-Host "Stopping nodes..."
    if ($proc1 -and -not $proc1.HasExited) { Stop-Process -Id $proc1.Id -Force -ErrorAction SilentlyContinue }
    if ($proc2 -and -not $proc2.HasExited) { Stop-Process -Id $proc2.Id -Force -ErrorAction SilentlyContinue }
    if ($proc3 -and -not $proc3.HasExited) { Stop-Process -Id $proc3.Id -Force -ErrorAction SilentlyContinue }
}

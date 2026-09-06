@echo off
echo =======================================
echo     Starting ShardKV System
echo =======================================

echo 1. Starting the C++ Cluster...
start "ShardKV Cluster" powershell.exe -ExecutionPolicy Bypass -File .\scripts\start_cluster.ps1

echo 2. Waiting for cluster to initialize...
timeout /t 3 /nobreak > nul

echo 3. Starting the Python Dashboard...
start "ShardKV Dashboard" cmd /c "python dashboard\server.py"

echo 4. Waiting for dashboard to start...
timeout /t 2 /nobreak > nul

echo 5. Opening browser...
start http://localhost:8006

echo.
echo All services are running! 
echo To stop everything, close the command prompt windows that were opened.
pause

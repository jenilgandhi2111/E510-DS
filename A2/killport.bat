@echo off
setlocal enabledelayedexpansion

REM Check if ports list is provided
if "%~1"=="" (
    echo Usage: %~nx0 ^<port_list^>
    exit /b 1
)

REM Loop through each port in the list
for %%p in (%~1) do (
    REM Check if the port is a number
    set "port=%%p"
    set "valid=true"
    for /f "delims=0123456789" %%a in ("!port!") do set "valid=false"
    if "!valid!"=="false" (
        echo Port '!port!' is not a valid port number.
        continue
    )

    REM Find the process listening on the port
    for /f "tokens=2" %%i in ('netstat -aon ^| findstr /r /c:"^.*:%%p .*LISTENING.*"') do (
        set "pid=%%i"
    )

    REM Check if a process is listening on the port
    if "!pid!"=="" (
        echo No process found listening on port !port!
    ) else (
        REM Kill the process
        echo Killing process !pid! listening on port !port!
        taskkill /F /PID !pid!
    )
)

@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Canal startup helper for Java 17.
REM It removes PermSize and adds plugin\* to classpath for RocketMQ support.

if "%CANAL_HOME%"=="" set "CANAL_HOME=D:\javawork\canal"
if "%JAVA_HOME%"=="" set "JAVA_HOME=D:\javawork\java17"

set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
set "JCMD_EXE=%JAVA_HOME%\bin\jcmd.exe"
set "CANAL_CONF=%CANAL_HOME%\conf\canal.properties"
set "LOGBACK_CONF=%CANAL_HOME%\conf\logback.xml"
set "CLASSPATH=%CANAL_HOME%\lib\*;%CANAL_HOME%\plugin\*;%CANAL_HOME%\conf"
set "MAIN_CLASS=com.alibaba.otter.canal.deployer.CanalLauncher"
set "MODE=%~1"

if "%MODE%"=="" set "MODE=start"

set "JAVA_OPTS=-Xms128m -Xmx512m -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dapplication.codeset=UTF-8 -Dfile.encoding=UTF-8 -DappName=otter-canal -Dlogback.configurationFile=%LOGBACK_CONF% -Dcanal.conf=%CANAL_CONF%"

if not exist "%JAVA_EXE%" (
    echo [ERROR] Java executable not found: %JAVA_EXE%
    echo [ERROR] Override it by setting JAVA_HOME first.
    exit /b 1
)

if not exist "%JCMD_EXE%" (
    echo [ERROR] jcmd executable not found: %JCMD_EXE%
    exit /b 1
)

if not exist "%CANAL_HOME%\lib\canal.deployer-1.1.8.jar" (
    echo [ERROR] Canal home not found: %CANAL_HOME%
    echo [ERROR] Override it by setting CANAL_HOME first.
    exit /b 1
)

if not exist "%CANAL_CONF%" (
    echo [ERROR] Canal config not found: %CANAL_CONF%
    exit /b 1
)

call :find_running

if /I "%MODE%"=="status" (
    if defined CANAL_PID (
        echo [INFO] Canal is running. PID=%CANAL_PID%
        echo [INFO] Canal PID list: %CANAL_PIDS%
        exit /b 0
    )
    echo [INFO] Canal is not running.
    exit /b 1
)

if /I "%MODE%"=="stop" (
    if not defined CANAL_PID (
        echo [INFO] Canal is not running.
        exit /b 0
    )
    for %%p in (%CANAL_PIDS%) do (
        taskkill /PID %%p /F >nul 2>&1
        if errorlevel 1 (
            echo [ERROR] Failed to stop Canal. PID=%%p
            exit /b 1
        )
    )
    echo [INFO] Canal stopped. PID list=%CANAL_PIDS%
    exit /b 0
)

if /I "%MODE%"=="restart" (
    if defined CANAL_PID (
        for %%p in (%CANAL_PIDS%) do (
            taskkill /PID %%p /F >nul 2>&1
            if errorlevel 1 (
                echo [ERROR] Failed to stop Canal before restart. PID=%%p
                exit /b 1
            )
        )
        echo [INFO] Stopped old Canal process list. PID list=%CANAL_PIDS%
    )
    set "MODE=start"
)

if /I "%MODE%"=="foreground" (
    if defined CANAL_PID (
        echo [ERROR] Canal is already running. PID=%CANAL_PID%
        echo [ERROR] Run "%~nx0 stop" first if you need foreground mode.
        exit /b 1
    )
    echo [INFO] Starting Canal in foreground mode...
    pushd "%CANAL_HOME%\bin" >nul
    "%JAVA_EXE%" %JAVA_OPTS% -classpath "%CLASSPATH%" %MAIN_CLASS%
    set "EXIT_CODE=%ERRORLEVEL%"
    popd >nul
    exit /b %EXIT_CODE%
)

if /I not "%MODE%"=="start" (
    echo [ERROR] Unsupported command: %MODE%
    call :print_usage
    exit /b 1
)

if defined CANAL_PID (
    echo [INFO] Canal is already running. PID=%CANAL_PID%
    echo [INFO] Canal PID list: %CANAL_PIDS%
    exit /b 0
)

echo [INFO] Starting Canal in background mode...
pushd "%CANAL_HOME%\bin" >nul
start "canal-java17" /b "%JAVA_EXE%" %JAVA_OPTS% -classpath "%CLASSPATH%" %MAIN_CLASS%
popd >nul

set "CANAL_PID="
for /L %%i in (1,1,5) do (
    timeout /t 1 /nobreak >nul
    call :find_running
    if defined CANAL_PID goto started
)

echo [ERROR] Canal did not stay alive after start.
echo [ERROR] Check logs:
echo [ERROR]   D:\javawork\canal\logs\canal\canal.log
echo [ERROR]   D:\javawork\canal\logs\example\example.log
exit /b 1

:started
echo [INFO] Canal started successfully. PID=%CANAL_PID%
echo [INFO] Log directory: D:\javawork\canal\logs
exit /b 0

:find_running
set "CANAL_PID="
set "CANAL_PIDS="
for /f "tokens=1,* delims= " %%i in ('"%JCMD_EXE%" -l 2^>nul') do (
    if /I "%%j"=="%MAIN_CLASS%" (
        if not defined CANAL_PID set "CANAL_PID=%%i"
        set "CANAL_PIDS=!CANAL_PIDS! %%i"
    )
)
if defined CANAL_PIDS set "CANAL_PIDS=!CANAL_PIDS:~1!"
exit /b 0

:print_usage
echo Usage:
echo   %~nx0 start       ^<default^> start Canal in background mode
echo   %~nx0 foreground  start Canal in foreground mode
echo   %~nx0 status      show Canal process status
echo   %~nx0 stop        stop Canal
echo   %~nx0 restart     restart Canal
exit /b 0

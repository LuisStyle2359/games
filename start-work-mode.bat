@echo off
rem Opens work-mode.html (or the newest work-mode*.html next to this file)
rem in an Edge app window: no tabs, no address bar.
setlocal
set "F="
for /f "delims=" %%F in ('dir /b /o-d "%~dp0work-mode*.html" 2^>nul') do if not defined F set "F=%%F"
if not defined F (
  echo work-mode.html was not found next to this file.
  pause
  exit /b 1
)
set "P=%~dp0%F%"
set "P=%P:\=/%"
start "" msedge --app="file:///%P%#app"

@echo off

if defined JAVA_HOME (
  if exist "%JAVA_HOME%\bin\java.exe" (
     set "JAVA_CMD=%JAVA_HOME%\bin\java
  ) else (
     set "JAVA_CMD=java"
  )
) else (
  set "JAVA_CMD=java"
)

"%JAVA_CMD%" -classpath "%~dp0\synclite-client-${revision}.jar;%~dp0\*" com.synclite.client.Main %1 %2 %3 %4 %5 %6 %7 %8 %9
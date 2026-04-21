# PR: SyncLite Client Hardening, Refactor, and Server Feature Alignment

## Summary
This PR modernizes and hardens the SyncLite client by:
- Refactoring a monolithic client into focused helper classes.
- Aligning client/server protocol behavior with recent synclite-db capabilities.
- Improving interactive CLI behavior and SQL-client-like output.
- Strengthening security defaults, validation, and connection handling.
- Adding graceful shutdown and resource cleanup paths.

## Why
The previous client implementation had multiple gaps:
- Monolithic structure that was difficult to maintain and extend.
- Incomplete validation and weak defaults for network/security options.
- UX inconsistencies in command handling and output formatting.
- Pagination flow that was not optimized for on-demand consumption.
- Cleanup and termination behavior that needed explicit control.

This PR addresses those issues and brings the client experience closer to a standard SQL CLI while preserving SyncLite-specific behavior.

## Major Changes

### 1) Architecture Refactor
The client has been split into dedicated modules:
- Main orchestration logic in Main.
- Runtime configuration/state in RuntimeContext.
- Validation and auth helpers in SecurityUtil.
- Config generation and file-permission handling in ConfigUtil.
- Table rendering and CLI output in OutputUtil.
- Interactive setup wizard in InteractiveSetup.
- HTTP transport, auth headers, and pagination calls in ServerTransport.

### 2) Device Type and Initialization Updates
- Migrated to logger DeviceType usage and removed legacy SyncLiteDeviceType.
- Added support for STORE variants and STREAMING where applicable.
- Prevented unsupported DBLOGGER usage in client flows.
- Replaced reflection-like initialization complexity with direct typed initialization calls.

### 3) Security and Validation Hardening
- Strict CLI option parsing and stronger argument validation.
- Secure-by-default server URL behavior (HTTPS by default; explicit opt-in for insecure HTTP).
- Timeouts for connect/read operations.
- Auth support for:
- Token-based auth.
- App HMAC auth headers with timestamp/nonce/signature.
- Optional local-path redaction in server requests.
- Safer configuration handling with permission-hardening best effort.

### 4) Server Protocol and Resultset Enhancements
- Added request fields for pagination size and resultset options.
- Added support for resultset include metadata and resultset data format options.
- Transaction handle tracking for server transaction continuity.
- Improved server-side response handling and consistent command feedback.

### 5) Pagination UX (On-Demand)
- Added on-demand pagination with NEXT command.
- First page is shown immediately; subsequent pages fetched only when requested.
- Pagination state (handle, has-more, columns, widths) persisted in runtime context.

### 6) Interactive UX Improvements
- No-args interactive setup now guides embedded and server mode cleanly.
- Server-only prompts are shown only when running in server mode.
- Improved default device-name derivation from database filename base (without extension).

### 7) SQL Output and Client Command Behavior
- Added structured table output with aligned columns, separators, NULL handling, and truncation behavior.
- Added SQL-client-like command feedback:
- DDL/DML in embedded mode now print rows affected or OK.
- Server/DB mode now prints message, rows affected (when present), or OK fallback.
- Command loop robustness improvements:
- SQL errors no longer terminate the session.
- QUIT/EXIT and NEXT now work with or without trailing semicolon when entered as standalone commands.

### 8) Cleanup and Shutdown
- Added explicit graceful shutdown behavior.
- Added cleanup paths for initialized embedded device types.
- Kept HTTP connection cleanup in finally paths in transport layer.

### 9) Launcher and Build Updates
- Added synclite-cli launchers while preserving synclite-client alias scripts.
- Updated build and project settings for current client behavior and packaging.

## Files Added
- client/src/main/java/com/synclite/client/ConfigUtil.java
- client/src/main/java/com/synclite/client/InteractiveSetup.java
- client/src/main/java/com/synclite/client/OutputUtil.java
- client/src/main/java/com/synclite/client/RuntimeContext.java
- client/src/main/java/com/synclite/client/SecurityUtil.java
- client/src/main/java/com/synclite/client/ServerTransport.java
- client/src/main/resources/synclite-cli.bat
- client/src/main/resources/synclite-cli.sh
- client/.settings/org.eclipse.jdt.apt.core.prefs

## Files Updated
- client/src/main/java/com/synclite/client/Main.java
- client/pom.xml
- client/src/main/resources/synclite-client.bat
- client/src/main/resources/synclite-client.sh
- client/.classpath
- client/.settings/org.eclipse.jdt.core.prefs

## Files Removed
- client/src/main/java/com/synclite/client/SyncLiteDeviceType.java

## Validation
Compilation check performed successfully:
- mvn -q -f client/pom.xml -Drevision=oss -DskipTests compile

## Behavioral Outcomes
- Client remains interactive after statement-level SQL errors.
- QUIT/EXIT terminate the session reliably (with or without semicolon).
- DDL/DML feedback is shown consistently in embedded mode and server mode.
- Resultsets are displayed in SQL-client-like tabular form even when server payload is JSON.
- Pagination is now user-driven and predictable.

## Notes
- Strict permission enforcement warnings on Windows are expected best-effort behavior and non-fatal.
- This PR keeps backward compatibility launchers while introducing synclite-cli as primary naming.

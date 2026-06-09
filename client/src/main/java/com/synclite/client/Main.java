/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.client;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Scanner;

import io.synclite.Derby;
import io.synclite.DerbyAppender;
import io.synclite.DerbyStore;
import io.synclite.DuckDB;
import io.synclite.DuckDBAppender;
import io.synclite.DuckDBStore;
import io.synclite.H2;
import io.synclite.H2Appender;
import io.synclite.H2Store;
import io.synclite.HyperSQL;
import io.synclite.HyperSQLAppender;
import io.synclite.HyperSQLStore;
import io.synclite.SQLite;
import io.synclite.SQLiteAppender;
import io.synclite.SQLiteStore;
import io.synclite.Streaming;
import io.synclite.SyncLiteStatement;

public class Main {
	private static boolean shutdownInProgress = false;
	private static RuntimeContext runtimeContext = null;

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		try {
			runtimeContext = new RuntimeContext();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				shutdownInProgress = true;
				cleanupDevices();
				System.out.println("Shutting down gracefully...");
			}));

			if (args.length == 0) {
				InteractiveSetup.run(new Scanner(System.in), runtimeContext);
			} else {
				// For now, just use interactive setup
				InteractiveSetup.run(new Scanner(System.in), runtimeContext);
			}

			OutputUtil.dumpHeader(runtimeContext.dbPath, runtimeContext.deviceType, runtimeContext.confPath, runtimeContext.serverAddress);

			try (Scanner scanner = new Scanner(System.in)) {
				if (runtimeContext.embeddedMode) {
					String jdbcUrl = initializeEmbedded(runtimeContext);
					runEmbeddedLoop(scanner, jdbcUrl, runtimeContext);
				} else {
					ServerTransport.executeOnServer("initialize", runtimeContext);
					runServerLoop(scanner, runtimeContext);
				}
			}
		} catch (Exception e) {
			if (!shutdownInProgress) {
				System.out.println(e.getMessage());
				Throwable cause = e.getCause();
				if ((cause != null) && (cause.getMessage() != null) && !cause.getMessage().isBlank()) {
					System.out.println("Cause : " + cause.getClass().getSimpleName() + " : " + cause.getMessage());
				}
			}
		} finally {
			cleanupDevices();
		}
	}

	private static String initializeEmbedded(RuntimeContext runtime) throws SQLException {
		String typeName = runtime.deviceType.name();
		Path dbPath = runtime.dbPath;
		Path confPath = runtime.confPath;

		switch (typeName) {
		case "SQLITE":
			SQLite.initialize(dbPath, confPath);
			break;
		case "SQLITE_APPENDER":
			SQLiteAppender.initialize(dbPath, confPath);
			break;
		case "SQLITE_STORE":
			SQLiteStore.initialize(dbPath, confPath);
			break;
		case "DUCKDB":
			DuckDB.initialize(dbPath, confPath);
			break;
		case "DUCKDB_APPENDER":
			DuckDBAppender.initialize(dbPath, confPath);
			break;
		case "DUCKDB_STORE":
			DuckDBStore.initialize(dbPath, confPath);
			break;
		case "DERBY":
			Derby.initialize(dbPath, confPath);
			break;
		case "DERBY_APPENDER":
			DerbyAppender.initialize(dbPath, confPath);
			break;
		case "DERBY_STORE":
			DerbyStore.initialize(dbPath, confPath);
			break;
		case "H2":
			H2.initialize(dbPath, confPath);
			break;
		case "H2_APPENDER":
			H2Appender.initialize(dbPath, confPath);
			break;
		case "H2_STORE":
			H2Store.initialize(dbPath, confPath);
			break;
		case "HYPERSQL":
			HyperSQL.initialize(dbPath, confPath);
			break;
		case "HYPERSQL_APPENDER":
			HyperSQLAppender.initialize(dbPath, confPath);
			break;
		case "HYPERSQL_STORE":
			HyperSQLStore.initialize(dbPath, confPath);
			break;
		case "STREAMING":
			Streaming.initialize(dbPath, confPath);
			break;
		default:
			throw new SQLException("SyncLite : Unsupported device type in embedded mode: " + typeName);
		}
		runtime.deviceTypeInitialized = typeName;
		// Build JDBC URL based on device type
		String jdbcUrl = buildJdbcUrl(runtime.deviceType, dbPath);
		return jdbcUrl;
	}

	private static String buildJdbcUrl(io.synclite.DeviceType deviceType, Path dbPath) throws SQLException {
		switch (deviceType.name()) {
		case "SQLITE":
		case "SQLITE_APPENDER":
		case "SQLITE_STORE":
			return "jdbc:sqlite:" + dbPath.toAbsolutePath().toString();
		case "DUCKDB":
		case "DUCKDB_APPENDER":
		case "DUCKDB_STORE":
			return "jdbc:duckdb:" + dbPath.toAbsolutePath().toString();
		case "DERBY":
		case "DERBY_APPENDER":
		case "DERBY_STORE":
			return "jdbc:derby:" + dbPath.toAbsolutePath().toString();
		case "H2":
		case "H2_APPENDER":
		case "H2_STORE":
			return "jdbc:h2:" + dbPath.toAbsolutePath().toString();
		case "HYPERSQL":
		case "HYPERSQL_APPENDER":
		case "HYPERSQL_STORE":
			return "jdbc:hsqldb:file:" + dbPath.toAbsolutePath().toString();
		default:
			throw new SQLException("Cannot build JDBC URL for device type: " + deviceType.name());
		}
	}

	private static void runEmbeddedLoop(Scanner scanner, String jdbcUrl, RuntimeContext runtime) {
		try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
			runSqlLoop(scanner, sql -> executeSql(stmt, sql, runtime));
		} catch (SQLException e) {
			if (!shutdownInProgress) {
				System.out.println(e.getMessage());
			}
		}
	}

	private static void runServerLoop(Scanner scanner, RuntimeContext runtime) {
		ServerPaginationExecutor executor = new ServerPaginationExecutor();
		runSqlLoop(scanner, sql -> handleServerSql(executor, sql, runtime));
	}

	private static void runSqlLoop(Scanner scanner, SqlExecutor executor) {
		System.out.println("[Type SQL statements, use NEXT; for pagination, QUIT; or EXIT; to exit]");
		String sql = "";
		while (true) {
			System.out.print("SyncLite > ");
			String line = scanner.nextLine().trim();
			if (line.isBlank()) {
				continue;
			}

			if (sql.isEmpty()) {
				String immediate = line.endsWith(";") ? line.substring(0, line.length() - 1).trim() : line;
				String immediateUpper = immediate.toUpperCase(Locale.ROOT);
				if (immediateUpper.equals("QUIT") || immediateUpper.equals("EXIT")) {
					System.out.println("Exiting SyncLite...");
					break;
				}
				if (immediateUpper.equals("NEXT")) {
					try {
						executor.execute("NEXT");
					} catch (SQLException e) {
						System.out.println(e.getMessage());
					}
					continue;
				}
			}

			sql = sql + (sql.isEmpty() ? "" : " ") + line;

			if (!sql.endsWith(";")) {
				continue;
			}

			String trimmedSql = sql.substring(0, sql.length() - 1).trim();
			String upperSql = trimmedSql.toUpperCase(Locale.ROOT);

			if (upperSql.equals("QUIT") || upperSql.equals("EXIT")) {
				System.out.println("Exiting SyncLite...");
				break;
			}

			try {
				executor.execute(trimmedSql);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			sql = "";
		}
	}

	private static void handleServerSql(ServerPaginationExecutor executor, String sql, RuntimeContext runtime) throws SQLException {
		if (sql.toUpperCase(Locale.ROOT).equals("NEXT")) {
			executor.fetchNext();
		} else {
			executor.executeSql(sql, runtime);
		}
	}

	private static void executeSql(Statement stmt, String sql, RuntimeContext runtime) throws SQLException {
		if ("TRUE".equals(runtime.props.get("UNLOGGED_MODE")) && (stmt instanceof SyncLiteStatement)) {
			((SyncLiteStatement) stmt).executeUnlogged(sql);
			System.out.println("OK");
			return;
		}

		boolean hasResultSet = stmt.execute(sql);
		if (hasResultSet) {
			ResultSet rs = stmt.getResultSet();
			if (rs != null) {
				OutputUtil.printResultSet(rs);
				rs.close();
			}
		} else {
			int updated = stmt.getUpdateCount();
			if (updated >= 0) {
				System.out.println(updated + " row" + (updated == 1 ? "" : "s") + " affected");
			} else {
				System.out.println("OK");
			}
		}
	}

	private static void cleanupDevices() {
		if (runtimeContext == null || runtimeContext.deviceTypeInitialized == null) {
			return;
		}
		try {
			switch (runtimeContext.deviceTypeInitialized) {
			case "SQLITE":
				SQLite.closeAllDatabases();
				break;
			case "SQLITE_APPENDER":
				SQLiteAppender.closeAllDatabases();
				break;
			case "SQLITE_STORE":
				SQLiteStore.closeAllDatabases();
				break;
			case "DUCKDB":
				DuckDB.closeAllDatabases();
				break;
			case "DUCKDB_APPENDER":
				DuckDBAppender.closeAllDatabases();
				break;
			case "DUCKDB_STORE":
				DuckDBStore.closeAllDatabases();
				break;
			case "DERBY":
				Derby.closeAllDatabases();
				break;
			case "DERBY_APPENDER":
				DerbyAppender.closeAllDatabases();
				break;
			case "DERBY_STORE":
				DerbyStore.closeAllDatabases();
				break;
			case "H2":
				H2.closeAllDatabases();
				break;
			case "H2_APPENDER":
				H2Appender.closeAllDatabases();
				break;
			case "H2_STORE":
				H2Store.closeAllDatabases();
				break;
			case "HYPERSQL":
				HyperSQL.closeAllDatabases();
				break;
			case "HYPERSQL_APPENDER":
				HyperSQLAppender.closeAllDatabases();
				break;
			case "HYPERSQL_STORE":
				HyperSQLStore.closeAllDatabases();
				break;
			case "STREAMING":
				Streaming.closeAllDatabases();
				break;
			}
		} catch (SQLException e) {
			if (!shutdownInProgress) {
				System.out.println("Warning: Error during device cleanup: " + e.getMessage());
			}
		}
	}

	private static class ServerPaginationExecutor {
		void executeSql(String sql, RuntimeContext runtime) throws SQLException {
			ServerTransport.executeOnServer(sql, runtime);
		}

		void fetchNext() throws SQLException {
			if (runtimeContext == null || runtimeContext.paginationHandle == null) {
				System.out.println("No pagination context available.");
				return;
			}
			if (!runtimeContext.paginationHasMore) {
				System.out.println("No more rows available.");
				return;
			}
			try {
				ServerTransport.fetchNextPage(runtimeContext);
				if (runtimeContext.paginationHasMore && runtimeContext.paginationHandle != null) {
					System.out.println("[Type NEXT; to see more rows]");
				}
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}

	@FunctionalInterface
	private interface SqlExecutor {
		void execute(String sql) throws SQLException;
	}
}

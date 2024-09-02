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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hsqldb.lib.OrderedHashSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import io.synclite.logger.*;

public class Main {
	private static boolean shutdownInProgress = false;
	private static HashMap<String, String> props = new HashMap<String, String>();
	public static SyncLiteDeviceType deviceType;
	static Path dbPath = null;
	static Path confPath = null;
	static String deviceName = "";
	static boolean embeddedMode = true;
	static String serverAddress = null;

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		try {
			// Register a shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				shutdownInProgress = true;
				try {
					SQLite.closeAllDatabases();
				} catch (SQLException e) {
				}
				System.out.println("Shutting down gracefully...");
			}));

			deviceType = SyncLiteDeviceType.SQLITE;

			if (args.length == 0) {
				// Start with DB <user/home/dir/synclite/db/>/test.db, device type
				// TRANSACTIONAL, config file <user/home/dir/synclite/db/>/synclite_logger.conf

				Path dbDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "db");
				Path stageDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "stageDir");
				try {
					Files.createDirectories(dbDir);
				} catch (IOException e) {
					throw new SQLException("Failed to create a db directory : " + dbDir, e);
				}

				try {
					Files.createDirectories(stageDir);
				} catch (IOException e) {
					throw new SQLException("Failed to create a stage directory : " + stageDir, e);
				}

				dbPath = dbDir.resolve("test.db");
				deviceType = SyncLiteDeviceType.SQLITE;
				confPath = createDefaultConf("test", dbDir, stageDir);

				dumpHeader(dbPath, deviceType, confPath);
			} else if (args.length == 1) {
				if (args[0].equals("--version")) {
					// Dump version from version file.
					ClassLoader classLoader = Main.class.getClassLoader();
					try (InputStream inputStream = classLoader.getResourceAsStream("synclite.version")) {
						if (inputStream != null) {
							// Read the contents of the file as
							// aC:\Users\arati\synclite\db\synclite_logger.conf string
							Scanner scanner = new Scanner(inputStream, "UTF-8");
							String fileContents = scanner.useDelimiter("\\A").next();
							System.out.println(fileContents);
							System.exit(0);
						}
					} catch (IOException e) {
						throw new SQLException("SyncLite : Failed to read version information", e);
					}
				} else {
					usage();
				}
			} else {
				dbPath = Paths.get(args[0]);
				if (dbPath.toFile().exists()) {
					if (!dbPath.toFile().canWrite()) {
						throw new SQLException("SyncLite : Specified database path : " + dbPath + " does not have write permission");
					}
				}

				for (int i = 1; i < args.length - 1; ++i) {
					String argName = args[i];
					String argVal = args[i + 1];

					switch (argName) {
					case "--device-type":
						deviceType = SyncLiteDeviceType.valueOf(argVal);
						if (deviceType == null) {
							throw new SQLException("SyncLite : Specified device-type : " + args[1]
									+ " is invalid. Valid values are SQLITE/DUCKDB/DERBY/H2/HYPERSQL/STREAMING/TELEMETRY/SQLITE_APPENDER/DUCKDB_APPENDER/DERBY_APPENDER/H2_APPENDER/HYPERSQL_APPENDER");
						}
						break;

					case "--synclite-logger-config":
						confPath = Paths.get(argVal);
						if (!confPath.toFile().exists()) {
							throw new SQLException("SyncLite : Specified configuration file path : " + confPath + " does not exist");
						}
						break;

					case "--device-name":
						deviceName = argVal;
						break;

					case "--server":
						serverAddress = argVal;
						// validate if it connects.
						//
						if (!isServerUp(serverAddress)) {
							throw new SQLException("SyncLite : Unable to connect to specified server : " + serverAddress);
						}
						embeddedMode = false;
					}

				}

				if (deviceType == null) {
					usage();
				}
				if (confPath == null) {
					usage();
				}
				dumpHeader(dbPath, deviceType, confPath);
			}

			Scanner scanner = new Scanner(System.in);

			String url = "jdbc:synclite_sqlite:" + dbPath;

			if (embeddedMode) {
				switch (deviceType) {
				case SQLITE:
					url = "jdbc:synclite_sqlite:" + dbPath;
					Class.forName("io.synclite.logger.SQLite");
					SQLite.initialize(dbPath, confPath);
					break;
				case SQLITE_APPENDER:
					url = "jdbc:synclite_sqlite_appender:" + dbPath;
					Class.forName("io.synclite.logger.SQLiteAppender");
					SQLiteAppender.initialize(dbPath, confPath);
					break;
				case DUCKDB:
					url = "jdbc:synclite_duckdb:" + dbPath;
					Class.forName("io.synclite.logger.DuckDB");
					DuckDB.initialize(dbPath, confPath);
					break;
				case DUCKDB_APPENDER:
					url = "jdbc:synclite_duckdb_appender:" + dbPath;
					Class.forName("io.synclite.logger.DuckDBAppender");
					DuckDBAppender.initialize(dbPath, confPath);
					break;
				case DERBY:
					url = "jdbc:synclite_derby:" + dbPath;
					Class.forName("io.synclite.logger.Derby");
					Derby.initialize(dbPath, confPath);
					break;
				case DERBY_APPENDER:
					url = "jdbc:synclite_derby_appender:" + dbPath;
					Class.forName("io.synclite.logger.DerbyAppender");
					DerbyAppender.initialize(dbPath, confPath);
					break;
				case H2:
					url = "jdbc:synclite_h2:" + dbPath;
					Class.forName("io.synclite.logger.H2");
					H2.initialize(dbPath, confPath);
					break;
				case H2_APPENDER:
					url = "jdbc:synclite_h2_appender:" + dbPath;
					Class.forName("io.synclite.logger.H2Appender");
					H2Appender.initialize(dbPath, confPath);
					break;
				case HYPERSQL:
					url = "jdbc:synclite_hsqldb:" + dbPath;
					Class.forName("io.synclite.logger.HyperSQL");
					HyperSQL.initialize(dbPath, confPath);
					break;
				case HYPERSQL_APPENDER:
					url = "jdbc:synclite_hsqldb_appender:" + dbPath;
					Class.forName("io.synclite.logger.HyperSQLAppender");
					HyperSQLAppender.initialize(dbPath, confPath);
					break;
				case STREAMING:
					url = "jdbc:synclite_streaming:" + dbPath;
					Class.forName("io.synclite.logger.Streaming");
					Streaming.initialize(dbPath, confPath);
					break;
				case TELEMETRY:
					url = "jdbc:synclite_telemetry:" + dbPath;
					Class.forName("io.synclite.logger.Telemetry");
					Telemetry.initialize(dbPath, confPath);
					break;
				}
			} else {
				//
				//Send initialize request to server
				//Prepare the request
				//
				executeOnServer("initialize");
			}


			if (embeddedMode) {
				try (Connection conn = DriverManager.getConnection(url)) {
					try (Statement stmt = conn.createStatement()) {
						String sql = "";
						while (!Thread.currentThread().isInterrupted()) {
							System.out.print("SyncLite>");
							String append;
							try {
								append = scanner.nextLine();
							} catch (NoSuchElementException e) {
								Thread.interrupted();
								break;
							}
							sql += append;
							boolean lastTokenComplete = false;
							if (sql.endsWith(";")) {
								lastTokenComplete = true;
							}
							String[] tokens = sql.split(";");
							int i = 0;
							for (i = 0; i < tokens.length - 1; ++i) {
								sql = tokens[i];
								// Check if sql is
								// set unlogged_mode = true/false
								String parsableSql = sql.strip().toUpperCase();
								if (parsableSql.startsWith("SET")) {
									String[] kv = parsableSql.substring(3).split("=", 2);
									if (kv.length == 2) {
										props.put(kv[0], kv[1]);
									}
								} else {
									// Execute SQL
									try {
										executeSql(stmt, sql);
									} catch (SQLException e) {
										if ((e.getSQLState() == null) || (!e.getSQLState().equals("SQLITE_DONE"))) {
											throw e;
										}
									}
								}
							}
							sql = tokens[i];
							if (lastTokenComplete) {
								// Check if sql is
								// set unlogged_mode = true/false
								String parsableSql = sql.strip().toUpperCase();
								if (parsableSql.startsWith("SET")) {
									String[] kv = parsableSql.substring(3).split("=", 2);
									if (kv.length == 2) {
										props.put(kv[0].strip(), kv[1].strip());
									}
								} else {
									// Execute SQL
									try {
										executeSql(stmt, sql);
									} catch (SQLException e) {
										if ((e.getSQLState() == null) || (!e.getSQLState().equals("SQLITE_DONE"))) {
											throw e;
										}
									}
								}
								sql = "";
							}
						}
					}
				} catch (SQLException e) {
					if (shutdownInProgress != true) {
						System.out.println(e.getMessage());
					}
				}
			} else {
				//server mode
				String sql = "";
				while (!Thread.currentThread().isInterrupted()) {
					System.out.print("SyncLite>");
					String append;
					try {
						append = scanner.nextLine();
					} catch (NoSuchElementException e) {
						break;
					}
					sql += append;
					boolean lastTokenComplete = false;
					if (sql.endsWith(";")) {
						lastTokenComplete = true;
					}
					String[] tokens = sql.split(";");
					int i = 0;
					for (i = 0; i < tokens.length - 1; ++i) {
						sql = tokens[i];							
						//execute on server and get response
						//
						//Send initialize request to server
						//Prepare the request
						//
						executeOnServer(sql);
					}
					sql = tokens[i];
					if (lastTokenComplete) {
						executeOnServer(sql);
						sql = "";
					}
				}
			}
			scanner.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static void executeOnServer(String sql) throws SQLException {
		JSONObject jsonRequest = new JSONObject();
		jsonRequest.put("db-type", deviceType.toString());
		jsonRequest.put("db-name", deviceName.toString());
		jsonRequest.put("db-path", dbPath);
		jsonRequest.put("synclite-logger-config", confPath);
		jsonRequest.put("sql", sql);

		JSONObject jsonResponse = sendRquest(jsonRequest);
		if (jsonResponse.has("result")) {
			Object result = jsonResponse.get("result");
			if (result instanceof JSONArray) {
				JSONArray resultSet = (JSONArray) result;
				List<String> columnNames = new ArrayList<String>();
				if (resultSet.length() > 0) {
					//Get the first record to form the column name list
					JSONObject row = resultSet.getJSONObject(0);
					// Iterate over each key in the JSON object
					Iterator<String> keys = row.keys();
					while (keys.hasNext()) {
						String colName = keys.next();
						columnNames.add(colName);
						System.out.print(colName + " | ");
					}
					System.out.println();
					for (int j = 0; j < resultSet.length(); j++) {
						row = resultSet.getJSONObject(j);
						for (String colName : columnNames) {
							System.out.print(row.get(colName) + " | ");
						}
						System.out.println();
					}
				}
				System.out.println(resultSet.length() + " rows");

			} else {
				System.out.println(result);
			}
		} else {
			System.out.println(jsonResponse.getString("message"));
		}
	}

	private static JSONObject sendRquest(JSONObject jsonRequest) throws SQLException {
		try (ZContext context = new ZContext()) {
			// Create a ZMQ Request socket
			ZMQ.Socket socket = context.createSocket(ZMQ.REQ);
			socket.connect(serverAddress);

			// Send the request
			String requestString = jsonRequest.toString();
			socket.send(requestString.getBytes(ZMQ.CHARSET), 0);

			// Receive the response
			String responseString = socket.recvStr(0);
			//System.out.println("Response: " + responseString);

			// Parse and print the result of SQL query from the response
			JSONObject jsonResponse = new JSONObject(responseString);
			return jsonResponse;
		} catch (Exception e) {
			throw new SQLException("Failed to connect to specified server : " + serverAddress + " : " + e.getMessage(), e);
		}
	}

	private static void dumpHeader(Path dbPath, SyncLiteDeviceType deviceType, Path confPath) {
		// Dump version from version file.
		ClassLoader classLoader = Main.class.getClassLoader();
		String version = "UNKNOWN";
		try (InputStream inputStream = classLoader.getResourceAsStream("synclite.version")) {
			if (inputStream != null) {
				// Read the contents of the file as a string
				Scanner scanner = new Scanner(inputStream, "UTF-8");
				version = scanner.useDelimiter("\\A").next();
			}
		} catch (Exception e) {
			// Skip
		}
		System.out.println();
		System.out.println("===================SyncLite Client " + version + "==========================");
		System.out.println("DB : " + dbPath);
		System.out.println("Device Type : " + deviceType);
		System.out.println("Logger Config File : " + confPath);
		System.out.println("=========================================================================");
		System.out.println();
	}

	private static void executeSql(Statement stmt, String sql) throws SQLException {
		if (props.get("UNLOGGED_MODE") != null && props.get("UNLOGGED_MODE").equals("TRUE")) {
			((SyncLiteStatement) stmt).executeUnlogged(sql);
		} else {
			switch (deviceType) {
			case SQLITE:
			case SQLITE_APPENDER:
			case STREAMING:
			case TELEMETRY:
				try (ResultSet rs = stmt.executeQuery(sql)) {
					if (rs != null) {
						ResultSetMetaData rsMetadata = rs.getMetaData();
						int colCount = rsMetadata.getColumnCount();
						for (int j = 1; j <= colCount; ++j) {
							String colDisplayName = rsMetadata.getColumnName(j);
							System.out.print(colDisplayName + " | ");
						}
						System.out.println();
						while (rs.next()) {
							for (int k = 1; k <= colCount; ++k) {
								System.out.print(rs.getString(k) + " | ");
							}
							System.out.println();
						}
						System.out.println();
					}
				}
				break;
			default:
				try (ResultSet rs = stmt.executeQuery(sql)) {
					if (rs != null) {
						ResultSetMetaData rsMetadata = rs.getMetaData();
						int colCount = rsMetadata.getColumnCount();
						for (int j = 1; j <= colCount; ++j) {
							String colDisplayName = rsMetadata.getColumnName(j);
							System.out.print(colDisplayName + " | ");
						}
						System.out.println();
						while (rs.next()) {
							for (int k = 1; k <= colCount; ++k) {
								System.out.print(rs.getString(k) + " | ");
							}
							System.out.println();
						}
						System.out.println();
					}
				} catch (SQLException e) {
					// Since devices other than SQLITE do not support DDLs and DMLs over
					// executeQuery API
					if (e.getMessage().contains("Unsupported SQL") || e.getMessage().contains("DDL statements") ||
							/* DuckDB */e.getMessage().contains("can only be used with queries that return a ResultSet") ||
							/* Derby */e.getMessage().contains("cannot be called with a statement that returns a row count") ||
							/* H2 */e.getMessage().contains("Method is only allowed for a query")) {
						// Try executing using execute API
						stmt.execute(sql);
						System.out.println();
					} else {
						throw e;
					}
				}
			}
		}
	}

	private static void usage() throws SQLException {
		if (isWindows()) {
			System.out.println("Usage1 : synclite.bat");
			System.out.println(
					"Usage2 : synclite.bat <path-to-synclite-database-file> --device-type <SQLITE|DUCKDB|DERBY|H2|HYPERSQL|STREAMING|TELEMETRY|SQLITE_APPENDER|DUCKDB_APPENDER|DERBY_APPENDER|H2_APPENDER|HYPERSQL_APPENDER> --synclite-logger-config <path-to-synclite-logger-config-file> --device-name <device-name> --server <synclitedb host:port>");
		} else {
			System.out.println("Usage1 : synclite.sh");
			System.out.println(
					"Usage2 : synclite.sh <path-to-synclite-database-file> --device-type <SQLITE|DUCKDB|DERBY|H2|HYPERSQL|STREAMING|TELEMETRY|SQLITE_APPENDER|DUCKDB_APPENDER|DERBY_APPENDER|H2_APPENDER|HYPERSQL_APPENDER> --synclite-logger-config <path-to-synclite-logger-config-file> --device-name <device-name> --server <synclitedb host:port>");
		}
		System.exit(0);
	}

	private final static boolean isWindows() {
		String osName = System.getProperty("os.name").toLowerCase();
		if (osName.contains("win")) {
			return true;
		} else {
			return false;
		}
	}

	private static Path createDefaultConf(String deviceName, Path confDir, Path stageDir) throws SQLException {
		StringBuilder confBuilder = new StringBuilder();
		String newLine = System.getProperty("line.separator");

		confBuilder.append("#==============Device Stage Properties==================");
		confBuilder.append(newLine);
		confBuilder.append("local-data-stage-directory=").append(stageDir);
		confBuilder.append(newLine);
		confBuilder.append("#local-data-stage-directory=<path/to/local/stage/directory>");
		confBuilder.append(newLine);
		confBuilder.append("destination-type=FS");
		confBuilder.append(newLine);
		confBuilder.append("#destination-type=<FS|MS_ONEDRIVE|GOOGLE_DRIVE|SFTP|MINIO|KAFKA|S3>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============SFTP Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:host=<host name of remote host for shipping device log files>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:user-name=<user name to connect to remote host>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:password=<password>");
		confBuilder.append(newLine);
		confBuilder.append(
				"#sftp:remote-data-stage-directory=<remote data directory name which will host the device directory>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============MinIO  Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#minio:endpoint=<MinIO endpoint to upload devices>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:bucket-name=<MinIO bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:access-key=<MinIO access key>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:secret-key=<MinIO secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============S3 Configuration=====================");
		confBuilder.append(newLine);
		confBuilder.append("#s3:endpoint=https://s3-<region>.amazonaws.com");
		confBuilder.append(newLine);
		confBuilder.append("#s3:bucket-name=<S3 bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:access-key=<S3 access key>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:secret-key=<S3 secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Kafka Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:<any_other_kafka_producer_property> = <kafka_producer_property_value>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Table filtering Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#include-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append("#exclude-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Logger Configuration==================");
		confBuilder.append("#log-queue-size=2147483647");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-flush-batch-size=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-log-count-threshold=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-duration-threshold-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-shipping-frequency-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-page-size=4096");
		confBuilder.append(newLine);
		confBuilder.append("#log-max-inlined-arg-count=16");
		confBuilder.append(newLine);
		confBuilder.append("#use-precreated-data-backup=false");
		confBuilder.append(newLine);
		confBuilder.append("#vacuum-data-backup=true");
		confBuilder.append(newLine);
		confBuilder.append("#skip-restart-recovery=false");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Device Configuration==================");
		confBuilder.append(newLine);
		String deviceEncryptionKeyFile = Path.of(System.getProperty("user.home"), ".ssh", "synclite_public_key.der")
				.toString();
		confBuilder.append("#device-encryption-key-file=" + deviceEncryptionKeyFile);
		confBuilder.append(newLine);
		if (deviceName == null) {
			confBuilder.append("#device-name=");
		} else {
			confBuilder.append("device-name=" + deviceName);
		}
		confBuilder.append(newLine);

		String confStr = confBuilder.toString();
		Path confPath = confDir.resolve("synclite_logger.conf");

		try {
			Files.writeString(confPath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLite logger configuration file : " + confPath, e);
		}

		return confPath;
	}


	private static boolean isServerUp(String serverAddress) {
		try (ZContext context = new ZContext()) {
			// Create a ZMQ Request socket to check server status
			ZMQ.Socket socket = context.createSocket(ZMQ.REQ);
			socket.setReceiveTimeOut(10000); // Set timeout for 10 seconds
			socket.connect(serverAddress);
			try {
				socket.send("ping", 0); 
				String response = socket.recvStr(0);
				if (response != null && !response.isEmpty()) {
					return true; 
				}
			} catch (Exception e) {
				System.out.println("Attempt to connect to specified server failed: " + e.getMessage());
			} finally {
				socket.close();
			}
		} catch (Exception e) {
			System.out.println("Attempt to connect to specified server failed: " + e.getMessage());
		}
		return false; 
	}
}

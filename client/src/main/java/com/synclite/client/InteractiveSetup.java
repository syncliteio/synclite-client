package com.synclite.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Scanner;

import io.synclite.DeviceType;

final class InteractiveSetup {
	private InteractiveSetup() {
	}

	static void run(Scanner scanner, RuntimeContext runtime) throws SQLException {
		Path defaultDbDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "db");
		Path defaultStageDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "stageDir");
		Path defaultDbPath = defaultDbDir.resolve("test.db");

		System.out.println();
		System.out.println("SyncLite interactive setup");
		System.out.println("Press Enter to accept default values.");
		System.out.println();

		runtime.embeddedMode = promptYesNo(scanner, "Run in embedded mode?", true);

		runtime.dbPath = Paths.get(promptWithDefault(scanner, "Database file path", defaultDbPath.toString())).toAbsolutePath().normalize();
		Path dbParent = runtime.dbPath.getParent();
		if (dbParent != null && !Files.exists(dbParent)) {
			if (promptYesNo(scanner, "Create database directory " + dbParent + "?", true)) {
				try {
					Files.createDirectories(dbParent);
				} catch (IOException e) {
					throw new SQLException("Failed to create database directory : " + dbParent, e);
				}
			} else {
				throw new SQLException("Database directory does not exist : " + dbParent);
			}
		}

		String supportedTypes = OutputUtil.supportedDeviceTypeList().replace("|", ",");
		while (true) {
			String deviceTypeInput = promptWithDefault(scanner, "Device type (" + supportedTypes + ")", RuntimeContext.DEFAULT_DEVICE_TYPE.name());
			try {
				runtime.deviceType = DeviceType.valueOf(deviceTypeInput.trim().toUpperCase(Locale.ROOT));
				if (runtime.deviceType == DeviceType.DBLOGGER) {
					System.out.println("DBLOGGER is not supported by this client.");
					continue;
				}
				break;
			} catch (Exception e) {
				System.out.println("Invalid device type. Supported values: " + supportedTypes);
			}
		}

		String defaultName = "test";
		if (runtime.dbPath.getFileName() != null) {
			String fileName = runtime.dbPath.getFileName().toString();
			int lastDot = fileName.lastIndexOf('.');
			defaultName = lastDot > 0 ? fileName.substring(0, lastDot) : fileName;
		}
		runtime.deviceName = promptWithDefault(scanner, "Device name", defaultName);

		String suppliedConf = promptWithDefault(scanner, "SyncLite logger config path (leave blank to auto-create)", "").trim();
		if (suppliedConf.isEmpty()) {
			Path stageDir = Paths.get(promptWithDefault(scanner, "Stage directory", defaultStageDir.toString())).toAbsolutePath().normalize();
			try {
				Files.createDirectories(stageDir);
			} catch (IOException e) {
				throw new SQLException("Failed to create stage directory : " + stageDir, e);
			}
			Path confDir = runtime.dbPath.getParent() == null ? defaultDbDir : runtime.dbPath.getParent();
			try {
				Files.createDirectories(confDir);
			} catch (IOException e) {
				throw new SQLException("Failed to create config directory : " + confDir, e);
			}
			runtime.confPath = ConfigUtil.createDefaultConf(runtime.deviceName, confDir, stageDir);
		} else {
			runtime.confPath = Paths.get(suppliedConf).toAbsolutePath().normalize();
			if (!Files.exists(runtime.confPath) || !runtime.confPath.toFile().isFile() || !runtime.confPath.toFile().canRead()) {
				throw new SQLException("Specified configuration file path is invalid or not readable : " + runtime.confPath);
			}
		}

		if (!runtime.embeddedMode) {
			runtime.connectTimeoutMs = promptPositiveInt(scanner, "Connect timeout (ms)", runtime.connectTimeoutMs);
			runtime.readTimeoutMs = promptPositiveInt(scanner, "Read timeout (ms)", runtime.readTimeoutMs);
			runtime.resultsetPaginationSize = promptPositiveInt(scanner, "Resultset pagination size", runtime.resultsetPaginationSize);
			runtime.resultsetIncludeMetadata = promptYesNo(scanner, "Include resultset metadata?", true);
			
			String formatChoice = promptWithDefault(scanner, "Resultset data format (JSON|DB)", "JSON").trim().toUpperCase(Locale.ROOT);
			if ("JSON".equals(formatChoice) || "DB".equals(formatChoice)) {
				runtime.resultsetDataFormat = formatChoice;
			}
			
			runtime.redactLocalPaths = promptYesNo(scanner, "Redact local paths in server requests?", false);

			runtime.authToken = SecurityUtil.resolveAuthToken(promptWithDefault(scanner, "Auth token (optional)", "").trim());
			runtime.appId = SecurityUtil.resolveAppId(promptWithDefault(scanner, "App ID for HMAC auth (optional)", "").trim());
			runtime.appSecret = SecurityUtil.resolveAppSecret(promptWithDefault(scanner, "App secret for HMAC auth (optional)", "").trim());

			if ((runtime.appId == null) != (runtime.appSecret == null)) {
				throw new SQLException("Both app auth values must be supplied together (app-id and app-secret)");
			}

			runtime.allowInsecureHttp = promptYesNo(scanner, "Allow insecure HTTP server URL?", false);
			runtime.serverAddress = SecurityUtil.validateServerAddress(
					promptWithDefault(scanner, "SyncLiteDB server URL", "https://localhost:5555").trim(),
					runtime.allowInsecureHttp);
			if (!ServerTransport.isServerUp(runtime.serverAddress, runtime)) {
				throw new SQLException("Unable to connect to specified server : " + runtime.serverAddress);
			}
		}
	}

	private static String promptWithDefault(Scanner scanner, String label, String defaultValue) {
		System.out.print(label + (defaultValue == null || defaultValue.isEmpty() ? "" : " [" + defaultValue + "]") + ": ");
		String value = scanner.nextLine();
		if (value == null || value.trim().isEmpty()) {
			return defaultValue == null ? "" : defaultValue;
		}
		return value.trim();
	}

	private static boolean promptYesNo(Scanner scanner, String label, boolean defaultValue) {
		String defaultText = defaultValue ? "Y/n" : "y/N";
		while (true) {
			System.out.print(label + " [" + defaultText + "]: ");
			String value = scanner.nextLine();
			if (value == null || value.trim().isEmpty()) {
				return defaultValue;
			}
			String normalized = value.trim().toLowerCase(Locale.ROOT);
			if (normalized.equals("y") || normalized.equals("yes")) {
				return true;
			}
			if (normalized.equals("n") || normalized.equals("no")) {
				return false;
			}
			System.out.println("Please answer yes or no.");
		}
	}

	private static int promptPositiveInt(Scanner scanner, String label, int defaultValue) {
		while (true) {
			String raw = promptWithDefault(scanner, label, String.valueOf(defaultValue));
			try {
				int parsed = Integer.parseInt(raw);
				if (parsed <= 0) {
					System.out.println("Please enter a positive integer value.");
					continue;
				}
				return parsed;
			} catch (NumberFormatException e) {
				System.out.println("Invalid numeric value.");
			}
		}
	}
}


package com.synclite.client;

import java.io.InputStream;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import io.synclite.DeviceType;

final class OutputUtil {
	private OutputUtil() {
	}

	static void dumpHeader(Path dbPath, DeviceType deviceType, Path confPath, String serverAddress) {
		ClassLoader classLoader = Main.class.getClassLoader();
		String version = "UNKNOWN";
		try (InputStream inputStream = classLoader.getResourceAsStream("synclite.version")) {
			if (inputStream != null) {
				Scanner scanner = new Scanner(inputStream, "UTF-8");
				version = scanner.useDelimiter("\\A").next();
			}
		} catch (Exception e) {
		}
		System.out.println();
		System.out.println("===================SyncLite Client " + version + "==========================");
		System.out.println("DB : " + dbPath);
		System.out.println("Device Type : " + deviceType);
		System.out.println("Logger Config File : " + confPath);
		if (serverAddress != null) {
			System.out.println("SyncLiteDB : " + serverAddress);
		}
		System.out.println("=========================================================================");
		System.out.println();
	}

	static void printResultSet(ResultSet rs) throws SQLException {
		if (rs == null) {
			return;
		}
		ResultSetMetaData meta = rs.getMetaData();
		int colCount = meta.getColumnCount();
		List<String> headers = new ArrayList<String>();
		for (int i = 1; i <= colCount; i++) {
			headers.add(meta.getColumnName(i));
		}
		List<List<String>> rows = new ArrayList<List<String>>();
		while (rs.next()) {
			List<String> row = new ArrayList<String>();
			for (int i = 1; i <= colCount; i++) {
				String v = rs.getString(i);
				row.add(v == null ? "NULL" : v);
			}
			rows.add(row);
		}
		int[] widths = computeColumnWidths(headers, rows);
		printTableHeader(headers, widths);
		for (List<String> row : rows) {
			printTableRow(row, widths);
		}
		printTableEnd(widths, rows.size());
	}

	static int[] printFirstTablePage(List<String> headers, List<List<String>> rows) {
		int[] widths = computeColumnWidths(headers, rows);
		printTableHeader(headers, widths);
		for (List<String> row : rows) {
			printTableRow(row, widths);
		}
		return widths;
	}

	static void printTablePage(List<List<String>> rows, int[] widths) {
		for (List<String> row : rows) {
			printTableRow(row, widths);
		}
	}

	static void printTableEnd(int[] widths, int totalRows) {
		printSeparatorLine(widths);
		System.out.println(totalRows + " row" + (totalRows == 1 ? "" : "s"));
	}

	private static final int MAX_COL_DISPLAY_WIDTH = 40;
	private static final int MIN_COL_DISPLAY_WIDTH = 3;

	private static int[] computeColumnWidths(List<String> headers, List<List<String>> rows) {
		int[] widths = new int[headers.size()];
		for (int i = 0; i < headers.size(); i++) {
			widths[i] = Math.max(MIN_COL_DISPLAY_WIDTH, Math.min(MAX_COL_DISPLAY_WIDTH, headers.get(i).length()));
		}
		for (List<String> row : rows) {
			for (int i = 0; i < Math.min(row.size(), widths.length); i++) {
				widths[i] = Math.min(MAX_COL_DISPLAY_WIDTH, Math.max(widths[i], row.get(i).length()));
			}
		}
		return widths;
	}

	private static void printTableHeader(List<String> headers, int[] widths) {
		printSeparatorLine(widths);
		printTableRow(headers, widths);
		printSeparatorLine(widths);
	}

	private static void printTableRow(List<String> values, int[] widths) {
		StringBuilder sb = new StringBuilder("|");
		for (int i = 0; i < widths.length; i++) {
			String val = i < values.size() ? values.get(i) : "";
			if (val == null) {
				val = "NULL";
			}
			if (val.length() > widths[i]) {
				val = val.substring(0, widths[i] - 1) + ">";
			}
			sb.append(" ").append(padRight(val, widths[i])).append(" |");
		}
		System.out.println(sb);
	}

	private static void printSeparatorLine(int[] widths) {
		StringBuilder sb = new StringBuilder("+");
		for (int w : widths) {
			for (int i = 0; i < w + 2; i++) {
				sb.append('-');
			}
			sb.append('+');
		}
		System.out.println(sb);
	}

	private static String padRight(String s, int width) {
		if (s.length() >= width) {
			return s;
		}
		StringBuilder sb = new StringBuilder(s);
		for (int i = s.length(); i < width; i++) {
			sb.append(' ');
		}
		return sb.toString();
	}

	static void usage() {
		if (isWindows()) {
			System.out.println("Command : synclite-cli.bat (alias: synclite-client.bat)");
			System.out.println("Usage1 : synclite-cli.bat");
			System.out.println();
			System.out.println(
					"Usage2 : synclite-cli.bat <path-to-synclite-database-file> --device-type <" + supportedDeviceTypeList() + "> --synclite-logger-config <path-to-synclite-logger-config-file> --device-name <device-name>");
			System.out.println();
			System.out.println(
					"Usage3 : synclite-cli.bat <path-to-synclite-database-file> --db-type <" + supportedDeviceTypeList() + "> --synclite-logger-config <path-to-synclite-logger-config-file> --db-name <device-name> --synclite-db <https://host:port> [--auth-token <token>] [--app-id <id> --app-secret <secret>] [--resultset-pagination-size <n>] [--connect-timeout-ms <ms>] [--read-timeout-ms <ms>] [--allow-insecure-http] [--redact-local-paths]");
		} else {
			System.out.println("Command : synclite-cli.sh (alias: synclite-client.sh)");
			System.out.println("Usage1 : synclite-cli.sh");
			System.out.println();
			System.out.println(
					"Usage2 : synclite-cli.sh <path-to-synclite-database-file> --device-type <" + supportedDeviceTypeList() + "> --synclite-logger-config <path-to-synclite-logger-config-file> --device-name <device-name> --server <synclitedb host:port>");
			System.out.println();
			System.out.println(
					"Usage3 : synclite-cli.sh <path-to-synclite-database-file> --db-type <" + supportedDeviceTypeList() + "> --synclite-logger-config <path-to-synclite-logger-config-file> --db-name <device-name> --synclite-db <https://host:port> [--auth-token <token>] [--app-id <id> --app-secret <secret>] [--resultset-pagination-size <n>] [--connect-timeout-ms <ms>] [--read-timeout-ms <ms>] [--allow-insecure-http] [--redact-local-paths]");
		}
		System.exit(1);
	}

	static String supportedDeviceTypeList() {
		StringBuilder builder = new StringBuilder();
		for (DeviceType type : DeviceType.values()) {
			if (type == DeviceType.DBLOGGER) {
				continue;
			}
			if (builder.length() > 0) {
				builder.append("|");
			}
			builder.append(type.name());
		}
		return builder.toString();
	}

	private static boolean isWindows() {
		String osName = System.getProperty("os.name").toLowerCase();
		return osName.contains("win");
	}
}


package com.synclite.client;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.json.JSONArray;
import org.json.JSONObject;

final class ServerTransport {
	private ServerTransport() {
	}

	static void executeOnServer(String sql, RuntimeContext runtime) throws SQLException {
		JSONObject jsonRequest = new JSONObject();
		jsonRequest.put("protocol-version", RuntimeContext.PROTOCOL_VERSION);
		jsonRequest.put("db-type", runtime.deviceType.toString());
		jsonRequest.put("db-name", runtime.deviceName);
		jsonRequest.put("synclite-logger-options", readLoggerOptions(runtime));
		jsonRequest.put("sql", sql);
		jsonRequest.put("resultset-pagination-size", runtime.resultsetPaginationSize);
		jsonRequest.put("resultset-include-metadata", runtime.resultsetIncludeMetadata);
		jsonRequest.put("resultset-data-format", runtime.resultsetDataFormat);
		if (runtime.currentTxnHandle != null) {
			jsonRequest.put("txn-handle", runtime.currentTxnHandle);
		}

		JSONObject jsonResponse = sendRequest(jsonRequest, runtime);

		if (jsonResponse.has("txn-handle")) {
			runtime.currentTxnHandle = jsonResponse.get("txn-handle").toString();
		}

		if (sql.strip().equalsIgnoreCase("commit") || sql.strip().equalsIgnoreCase("rollback")) {
			runtime.currentTxnHandle = null;
		}

		if (jsonResponse.has("result") && !jsonResponse.optBoolean("result", false)) {
			String code = jsonResponse.optString("code", "ERR_GENERIC");
			System.out.println("ERROR [" + code + "] " + jsonResponse.optString("message", "Request failed"));
			return;
		}

		if (jsonResponse.has("resultset")) {
			Object result = jsonResponse.get("resultset");
			if ((result != JSONObject.NULL) && (result instanceof JSONArray)) {
				JSONArray firstPage = (JSONArray) result;
				List<String> columnNames = new ArrayList<String>();
				int[] widths = null;

				if (firstPage.length() > 0) {
					List<List<String>> firstRows = jsonArrayToRows(firstPage, columnNames);
					widths = OutputUtil.printFirstTablePage(columnNames, firstRows);
				} else {
					System.out.println("0 rows");
				}

				runtime.paginationHasMore = jsonResponse.optBoolean("has-more", false);
				runtime.paginationHandle = jsonResponse.optString("resultset-handle", null);
				runtime.paginationColumnNames = columnNames;
				runtime.paginationColumnWidths = widths;
			}
		} else {
			printCommandFeedback(jsonResponse);
		}
	}

	private static void printCommandFeedback(JSONObject jsonResponse) {
		String message = jsonResponse.optString("message", "").trim();
		if (!message.isEmpty()) {
			System.out.println(message);
			return;
		}

		Integer affectedRows = extractAffectedRows(jsonResponse);
		if (affectedRows != null) {
			System.out.println(affectedRows + " row" + (affectedRows == 1 ? "" : "s") + " affected");
			return;
		}

		if (jsonResponse.optBoolean("result", false)) {
			System.out.println("OK");
		}
	}

	private static Integer extractAffectedRows(JSONObject jsonResponse) {
		String[] keys = new String[] { "rows-affected", "affected-rows", "update-count", "row-count", "count" };
		for (String key : keys) {
			if (!jsonResponse.has(key)) {
				continue;
			}
			Object value = jsonResponse.get(key);
			if (value instanceof Number) {
				return Integer.valueOf(((Number) value).intValue());
			}
			if (value != null && value != JSONObject.NULL) {
				try {
					return Integer.valueOf(Integer.parseInt(value.toString().trim()));
				} catch (NumberFormatException ignored) {
				}
			}
		}
		return null;
	}

	static boolean isServerUp(String serverAddress, RuntimeContext runtime) {
		HttpURLConnection conn = null;
		try {
			URL url = new URL(serverAddress);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setConnectTimeout(runtime.connectTimeoutMs);
			conn.setReadTimeout(runtime.readTimeoutMs);
			if ((runtime.authToken != null) && !runtime.authToken.isBlank()) {
				conn.setRequestProperty("X-SyncLite-Token", runtime.authToken);
			}
			int responseCode = conn.getResponseCode();
			return responseCode == HttpURLConnection.HTTP_OK;
		} catch (Exception e) {
			System.out.println("Attempt to connect to specified server failed: " + e.getMessage());
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return false;
	}

	static void fetchNextPage(RuntimeContext runtime) throws SQLException {
		if ((runtime.paginationHandle == null) || runtime.paginationHandle.isBlank()) {
			throw new SQLException("No pagination handle available.");
		}
		if (!runtime.paginationHasMore) {
			throw new SQLException("No more rows available.");
		}

		JSONObject nextRequest = buildNextPageRequest(runtime.paginationHandle, runtime);
		JSONObject nextResponse = sendRequest(nextRequest, runtime);

		if (nextResponse.has("result") && !nextResponse.optBoolean("result", false)) {
			String code = nextResponse.optString("code", "ERR_GENERIC");
			throw new SQLException("Failed to fetch next page : [" + code + "] " + nextResponse.optString("message", "Request failed"));
		}

		JSONArray nextPage = nextResponse.optJSONArray("resultset");
		if (nextPage != null && nextPage.length() > 0) {
			List<List<String>> nextRows = jsonArrayToRows(nextPage, runtime.paginationColumnNames);
			if (runtime.paginationColumnWidths == null) {
				runtime.paginationColumnWidths = OutputUtil.printFirstTablePage(runtime.paginationColumnNames, nextRows);
			} else {
				OutputUtil.printTablePage(nextRows, runtime.paginationColumnWidths);
			}
		}

		runtime.paginationHasMore = nextResponse.optBoolean("has-more", false);
		runtime.paginationHandle = nextResponse.optString("resultset-handle", runtime.paginationHandle);
	}

	private static JSONObject buildNextPageRequest(String resultsetHandle, RuntimeContext runtime) {
		JSONObject nextRequest = new JSONObject();
		nextRequest.put("protocol-version", RuntimeContext.PROTOCOL_VERSION);
		nextRequest.put("request-type", "next");
		nextRequest.put("resultset-handle", resultsetHandle);
		nextRequest.put("resultset-pagination-size", runtime.resultsetPaginationSize);
		nextRequest.put("resultset-include-metadata", runtime.resultsetIncludeMetadata);
		nextRequest.put("resultset-data-format", runtime.resultsetDataFormat);
		return nextRequest;
	}

	private static List<List<String>> jsonArrayToRows(JSONArray jsonArray, List<String> columnNames) {
		List<List<String>> rows = new ArrayList<List<String>>();
		if (jsonArray == null || jsonArray.length() == 0) {
			return rows;
		}
		if (columnNames.isEmpty()) {
			JSONObject firstRow = jsonArray.getJSONObject(0);
			Iterator<String> keys = firstRow.keys();
			while (keys.hasNext()) {
				columnNames.add(keys.next());
			}
		}
		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject obj = jsonArray.getJSONObject(i);
			List<String> row = new ArrayList<String>();
			for (String col : columnNames) {
				Object v = obj.opt(col);
				row.add((v == null || v == JSONObject.NULL) ? "NULL" : v.toString());
			}
			rows.add(row);
		}
		return rows;
	}

	private static JSONObject sendRequest(JSONObject jsonRequest, RuntimeContext runtime) throws SQLException {
		HttpURLConnection conn = null;
		try {
			URL url = new URL(runtime.serverAddress);
			String requestPath = url.getPath();
			if ((requestPath == null) || requestPath.isBlank()) {
				requestPath = "/";
			}
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setConnectTimeout(runtime.connectTimeoutMs);
			conn.setReadTimeout(runtime.readTimeoutMs);
			conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			if ((runtime.authToken != null) && !runtime.authToken.isBlank()) {
				conn.setRequestProperty("X-SyncLite-Token", runtime.authToken);
			}
			if ((runtime.appId != null) && !runtime.appId.isBlank() && (runtime.appSecret != null)
					&& !runtime.appSecret.isBlank()) {
				String timestamp = String.valueOf(System.currentTimeMillis());
				String nonce = UUID.randomUUID().toString();
				String requestString = jsonRequest.toString();
				String canonical = "POST\\n" + requestPath + "\\n" + timestamp + "\\n" + nonce + "\\n"
						+ sha256Hex(requestString);
				String signature = signHmacSha256(runtime.appSecret, canonical);
				conn.setRequestProperty("X-SyncLite-App-Id", runtime.appId);
				conn.setRequestProperty("X-SyncLite-Timestamp", timestamp);
				conn.setRequestProperty("X-SyncLite-Nonce", nonce);
				conn.setRequestProperty("X-SyncLite-Signature", signature);
			}

			String requestString = jsonRequest.toString();
			try (OutputStream os = conn.getOutputStream()) {
				os.write(requestString.getBytes(StandardCharsets.UTF_8));
				os.flush();
			}

			int responseCode = conn.getResponseCode();
			InputStream responseStream = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();
			if (responseStream == null) {
				throw new SQLException(
						"Failed to connect to specified server : " + runtime.serverAddress + " : Empty response body");
			}

			StringBuilder response = new StringBuilder();
			try (BufferedReader br = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
				String responseLine;
				while ((responseLine = br.readLine()) != null) {
					response.append(responseLine);
				}
			}
			try {
				return new JSONObject(response.toString());
			} catch (Exception jsonEx) {
				throw new SQLException("Failed to connect to specified server : " + runtime.serverAddress + " : HTTP "
						+ responseCode + " : " + response.toString(), jsonEx);
			}

		} catch (Exception e) {
			throw new SQLException("Failed to connect to specified server : " + runtime.serverAddress + " : "
					+ e.getMessage(), e);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	private static JSONObject readLoggerOptions(RuntimeContext runtime) throws SQLException {
		try {
			if (runtime.confPath == null) {
				throw new SQLException("synclite-logger-config path is not set");
			}
			if (!Files.exists(runtime.confPath)) {
				throw new SQLException("synclite-logger-config file does not exist: " + runtime.confPath);
			}

			JSONObject options = new JSONObject();
			List<String> lines = Files.readAllLines(runtime.confPath, StandardCharsets.UTF_8);
			for (String line : lines) {
				String trimmed = line == null ? "" : line.trim();
				if (trimmed.isEmpty() || trimmed.startsWith("#")) {
					continue;
				}
				int separatorIndex = trimmed.indexOf('=');
				if (separatorIndex <= 0) {
					continue;
				}
				String key = trimmed.substring(0, separatorIndex).trim();
				String value = trimmed.substring(separatorIndex + 1).trim();
				if (!key.isEmpty()) {
					options.put(key, value);
				}
			}
			return options;
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException("Failed to parse synclite-logger-config file", e);
		}
	}

	private static String sha256Hex(String value) throws SQLException {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
			StringBuilder builder = new StringBuilder();
			for (byte b : hash) {
				builder.append(String.format("%02x", b));
			}
			return builder.toString();
		} catch (Exception e) {
			throw new SQLException("Failed to hash request payload", e);
		}
	}

	private static String signHmacSha256(String secret, String payload) throws SQLException {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
			byte[] signed = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
			return Base64.getEncoder().encodeToString(signed);
		} catch (Exception e) {
			throw new SQLException("Failed to sign request payload", e);
		}
	}
}


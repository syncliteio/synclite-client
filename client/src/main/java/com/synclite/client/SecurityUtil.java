package com.synclite.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

final class SecurityUtil {
	private SecurityUtil() {
	}

	static int parsePositiveInt(String argName, String argVal) throws java.sql.SQLException {
		try {
			int val = Integer.parseInt(argVal);
			if (val <= 0) {
				throw new java.sql.SQLException("SyncLite : " + argName + " must be a positive integer");
			}
			return val;
		} catch (NumberFormatException e) {
			throw new java.sql.SQLException("SyncLite : " + argName + " must be a positive integer", e);
		}
	}

	static String validateServerAddress(String rawAddress, boolean allowInsecureHttp) throws java.sql.SQLException {
		try {
			URL parsed = new URL(rawAddress);
			String scheme = parsed.getProtocol().toLowerCase(Locale.ROOT);
			if (!("https".equals(scheme) || "http".equals(scheme))) {
				throw new java.sql.SQLException("SyncLite : Server URL must use http or https : " + rawAddress);
			}
			if ("http".equals(scheme) && !allowInsecureHttp) {
				throw new java.sql.SQLException("SyncLite : Insecure HTTP is blocked by default. Use --allow-insecure-http to override");
			}
			if ((parsed.getHost() == null) || parsed.getHost().isBlank()) {
				throw new java.sql.SQLException("SyncLite : Invalid server URL (missing host) : " + rawAddress);
			}
			return parsed.toString();
		} catch (MalformedURLException e) {
			throw new java.sql.SQLException("SyncLite : Invalid server URL : " + rawAddress, e);
		}
	}

	static String resolveAuthToken(String cliToken) {
		if ((cliToken != null) && !cliToken.isBlank()) {
			return cliToken;
		}
		String envToken = System.getenv("SYNCLITE_CLIENT_AUTH_TOKEN");
		if ((envToken != null) && !envToken.isBlank()) {
			return envToken;
		}
		envToken = System.getenv("SYNCLITE_DB_AUTH_TOKEN");
		if ((envToken != null) && !envToken.isBlank()) {
			return envToken;
		}
		return null;
	}

	static String resolveAppId(String cliAppId) {
		if ((cliAppId != null) && !cliAppId.isBlank()) {
			return cliAppId;
		}
		String envAppId = System.getenv("SYNCLITE_DB_APP_ID");
		if ((envAppId != null) && !envAppId.isBlank()) {
			return envAppId;
		}
		return null;
	}

	static String resolveAppSecret(String cliAppSecret) {
		if ((cliAppSecret != null) && !cliAppSecret.isBlank()) {
			return cliAppSecret;
		}
		String envAppSecret = System.getenv("SYNCLITE_DB_APP_SECRET");
		if ((envAppSecret != null) && !envAppSecret.isBlank()) {
			return envAppSecret;
		}
		return null;
	}
}


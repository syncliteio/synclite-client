package com.synclite.client;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.synclite.DeviceType;

final class RuntimeContext {
	static final DeviceType DEFAULT_DEVICE_TYPE = DeviceType.SQLITE;
	static final String PROTOCOL_VERSION = "1";

	DeviceType deviceType = DEFAULT_DEVICE_TYPE;
	Path dbPath;
	Path confPath;
	String deviceName = "";
	boolean embeddedMode = true;
	String serverAddress;
	int connectTimeoutMs = 10000;
	int readTimeoutMs = 10000;
	boolean allowInsecureHttp = false;
	boolean redactLocalPaths = false;
	String authToken;
	String appId;
	String appSecret;
	int resultsetPaginationSize = 1000;
	String currentTxnHandle;
	boolean resultsetIncludeMetadata = true;
	String resultsetDataFormat = "JSON";
	
	String paginationHandle;
	boolean paginationHasMore;
	List<String> paginationColumnNames;
	int[] paginationColumnWidths;
	
	String deviceTypeInitialized;
	Map<String, String> props = new HashMap<>();
}


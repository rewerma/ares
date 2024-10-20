package com.github.ares.connctor.jdbc.catalog.sqlserver;

import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqlServerURLParser {
    private static final int DEFAULT_PORT = 1433;

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        String serverName = "";
        Integer port = DEFAULT_PORT;
        String dbInstance = null;
        int hostIndex = url.indexOf("://");
        if (hostIndex <= 0) {
            return null;
        }

        Map<String, String> props = Collections.emptyMap();
        String[] split = url.split(";", 2);
        if (split.length > 1) {
            props = parseQueryParams(split[1], ";");
            serverName = props.get("serverName");
            dbInstance = props.getOrDefault("databaseName", props.get("database"));
            if (props.containsKey("portNumber")) {
                String portNumber = props.get("portNumber");
                try {
                    port = Integer.parseInt(portNumber);
                } catch (NumberFormatException e) {
                }
            }
        }

        String urlServerName = split[0].substring(hostIndex + 3);
        if (!urlServerName.isEmpty()) {
            serverName = urlServerName;
        }

        int portLoc = serverName.indexOf(":");
        if (portLoc > 1) {
            port = Integer.parseInt(serverName.substring(portLoc + 1));
            serverName = serverName.substring(0, portLoc);
        }

        int instanceLoc = serverName.indexOf("\\");
        if (instanceLoc > 1) {
            serverName = serverName.substring(0, instanceLoc);
        }

        if (serverName.isEmpty()) {
            return null;
        }

        String suffix =
                props.entrySet().stream()
                        .filter(
                                e ->
                                        !e.getKey().equals("databaseName")
                                                && !e.getKey().equals("database"))
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(";", "", ""));
        suffix = Optional.ofNullable(suffix).orElse("");
        return new JdbcUrlUtil.UrlInfo(
                url,
                String.format("jdbc:sqlserver://%s:%s", serverName, port) + ";" + suffix,
                serverName,
                port,
                dbInstance,
                suffix);
    }

    private static Map<String, String> parseQueryParams(String query, String separator) {
        if (query == null || query.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> queryParams = new LinkedHashMap<>();
        String[] pairs = query.split(separator);
        for (String pair : pairs) {
            try {
                int idx = pair.indexOf("=");
                String key =
                        idx > 0
                                ? URLDecoder.decode(
                                        pair.substring(0, idx), StandardCharsets.UTF_8.name())
                                : pair;
                if (!queryParams.containsKey(key)) {
                    String value =
                            idx > 0 && pair.length() > idx + 1
                                    ? URLDecoder.decode(
                                            pair.substring(idx + 1), StandardCharsets.UTF_8.name())
                                    : null;
                    queryParams.put(key, value);
                }
            } catch (UnsupportedEncodingException e) {
                // Ignore.
            }
        }
        return queryParams;
    }
}

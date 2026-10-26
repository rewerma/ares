package com.github.ares.connector.doris.febe;

import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.doris.config.DorisOptions;
import com.github.ares.connector.jdbc.config.JdbcOptions;
import com.github.ares.connector.jdbc.config.JdbcSourceOptions;
import com.github.ares.connector.jdbc.utils.JdbcUrlUtil;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class DorisFeBeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<String> feNodes;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final String scanFilter;
    private final String query;
    private final String jdbcUrl;
    private final String driverName;
    private final int requestTabletSize;
    private final int maxRetries;
    private final int scanConnectTimeoutMs;
    private final int scanBatchRows;
    private final int scanKeepAliveMin;
    private final int scanQueryTimeoutSec;
    private final long scanMemLimit;
    private final int batchMaxRows;
    private final long batchMaxBytes;
    private final int httpSocketTimeoutMs;
    private final boolean partialUpdate;
    private final Map<String, String> streamLoadProps;
    private List<String> columns = new ArrayList<>();

    private DorisFeBeConfig(
            List<String> feNodes,
            String username,
            String password,
            String database,
            String table,
            String scanFilter,
            String query,
            String jdbcUrl,
            String driverName,
            int requestTabletSize,
            int maxRetries,
            int scanConnectTimeoutMs,
            int scanBatchRows,
            int scanKeepAliveMin,
            int scanQueryTimeoutSec,
            long scanMemLimit,
            int batchMaxRows,
            long batchMaxBytes,
            int httpSocketTimeoutMs,
            boolean partialUpdate,
            Map<String, String> streamLoadProps) {
        this.feNodes = feNodes;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.scanFilter = scanFilter;
        this.query = query;
        this.jdbcUrl = jdbcUrl;
        this.driverName = driverName;
        this.requestTabletSize = requestTabletSize;
        this.maxRetries = maxRetries;
        this.scanConnectTimeoutMs = scanConnectTimeoutMs;
        this.scanBatchRows = scanBatchRows;
        this.scanKeepAliveMin = scanKeepAliveMin;
        this.scanQueryTimeoutSec = scanQueryTimeoutSec;
        this.scanMemLimit = scanMemLimit;
        this.batchMaxRows = batchMaxRows;
        this.batchMaxBytes = batchMaxBytes;
        this.httpSocketTimeoutMs = httpSocketTimeoutMs;
        this.partialUpdate = partialUpdate;
        this.streamLoadProps = streamLoadProps;
    }

    public static boolean isFeBe(ReadonlyConfig options) {
        String mode = options.get(DorisOptions.MODE);
        return DorisOptions.FEBE.equalsIgnoreCase(mode);
    }

    public static void checkMode(ReadonlyConfig options) {
        String mode = options.get(DorisOptions.MODE);
        if (DorisOptions.JDBC.equalsIgnoreCase(mode) || DorisOptions.FEBE.equalsIgnoreCase(mode)) {
            return;
        }
        throw new AresException(
                "Unsupported Doris mode '"
                        + mode
                        + "'. Use "
                        + DorisOptions.JDBC
                        + " or "
                        + DorisOptions.FEBE
                        + ".");
    }

    public static DorisFeBeConfig from(ReadonlyConfig options) {
        List<String> feNodes = parseFeNodes(options);
        if (feNodes.isEmpty()) {
            throw new AresException(
                    "Doris febe mode requires fe_nodes, for example 127.0.0.1:8030");
        }
        String username = options.getOptional(JdbcOptions.USER).orElse("");
        String password = options.getOptional(JdbcOptions.PASSWORD).orElse("");
        String tableName =
                options.getOptional(JdbcOptions.TABLE_NAME)
                        .orElseGet(() -> options.getOptional(JdbcOptions.TABLE).orElse(null));
        if (StringUtils.isBlank(tableName)) {
            throw new AresException("Doris febe mode requires table_name");
        }
        TablePath tablePath = TablePath.of(tableName, false);
        String database = tablePath.getDatabaseName();
        if (StringUtils.isBlank(database)) {
            database = options.getOptional(JdbcOptions.DATABASE).orElse(null);
        }
        String jdbcUrl = options.getOptional(JdbcOptions.URL).orElse(null);
        if (StringUtils.isBlank(database) && StringUtils.isNotBlank(jdbcUrl)) {
            database = JdbcUrlUtil.getUrlInfo(jdbcUrl).getDefaultDatabase().orElse(null);
        }
        if (StringUtils.isBlank(database) || StringUtils.isBlank(tablePath.getTableName())) {
            throw new AresException(
                    "Doris table_name must be database.table, or set database together with the table name");
        }
        String scanFilter = options.getOptional(DorisOptions.SCAN_FILTER).orElse(null);
        if (StringUtils.isBlank(scanFilter)) {
            scanFilter = options.getOptional(JdbcSourceOptions.WHERE_CONDITION).orElse(null);
        }
        Map<String, String> props =
                options.getOptional(DorisOptions.STREAM_LOAD_PROPS).orElse(Collections.emptyMap());
        int queryTimeout = options.get(JdbcOptions.QUERY_TIMEOUT_SEC);
        if (queryTimeout < 0) {
            queryTimeout = 3600;
        }
        return new DorisFeBeConfig(
                feNodes,
                username,
                password,
                database,
                tablePath.getTableName(),
                StringUtils.trimToNull(scanFilter),
                options.getOptional(JdbcOptions.QUERY).orElse(null),
                jdbcUrl,
                options.getOptional(JdbcOptions.DRIVER).orElse(DorisOptions.MYSQL_DRIVER),
                options.get(DorisOptions.REQUEST_TABLET_SIZE),
                Math.max(options.get(JdbcOptions.MAX_RETRIES), 3),
                options.get(DorisOptions.SCAN_CONNECT_TIMEOUT_MS),
                options.get(DorisOptions.SCAN_BATCH_ROWS),
                options.get(DorisOptions.SCAN_KEEP_ALIVE_MIN),
                queryTimeout,
                options.get(DorisOptions.SCAN_MEM_LIMIT),
                options.get(JdbcOptions.BATCH_SIZE),
                options.get(DorisOptions.BATCH_MAX_BYTES),
                options.get(DorisOptions.HTTP_SOCKET_TIMEOUT_MS),
                options.get(DorisOptions.PARTIAL_UPDATE),
                new LinkedHashMap<>(props));
    }

    public static List<String> parseFeNodes(ReadonlyConfig options) {
        Object raw = rawFeNodes(options);
        if (raw == null) {
            return Collections.emptyList();
        }
        List<String> nodes = new ArrayList<>();
        if (raw instanceof List) {
            for (Object item : (List<?>) raw) {
                addNode(nodes, item == null ? null : String.valueOf(item));
            }
        } else {
            String text = String.valueOf(raw);
            for (String part : text.split(",")) {
                addNode(nodes, part);
            }
        }
        return nodes;
    }

    private static Object rawFeNodes(ReadonlyConfig options) {
        if (options.getConfData().containsKey(DorisOptions.FE_NODES.key())) {
            return options.getConfData().get(DorisOptions.FE_NODES.key());
        }
        for (String fallback : DorisOptions.FE_NODES.getFallbackKeys()) {
            if (options.getConfData().containsKey(fallback)) {
                return options.getConfData().get(fallback);
            }
        }
        return null;
    }

    private static void addNode(List<String> nodes, String node) {
        String normalized = normalizeFeNode(node);
        if (normalized != null) {
            nodes.add(normalized);
        }
    }

    public static String normalizeFeNode(String node) {
        if (node == null) {
            return null;
        }
        String value = node.trim();
        if (value.isEmpty()) {
            return null;
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://")) {
            value = value.substring("http://".length());
        } else if (lower.startsWith("https://")) {
            value = value.substring("https://".length());
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    public List<String> getFeNodes() {
        return feNodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getScanFilter() {
        return scanFilter;
    }

    public String getQuery() {
        return query;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getDriverName() {
        return driverName;
    }

    public int getRequestTabletSize() {
        return requestTabletSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getScanConnectTimeoutMs() {
        return scanConnectTimeoutMs;
    }

    public int getScanBatchRows() {
        return scanBatchRows;
    }

    public int getScanKeepAliveMin() {
        return scanKeepAliveMin;
    }

    public int getScanQueryTimeoutSec() {
        return scanQueryTimeoutSec;
    }

    public long getScanMemLimit() {
        return scanMemLimit;
    }

    public int getBatchMaxRows() {
        return batchMaxRows;
    }

    public long getBatchMaxBytes() {
        return batchMaxBytes;
    }

    public int getHttpSocketTimeoutMs() {
        return httpSocketTimeoutMs;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public Map<String, String> getStreamLoadProps() {
        return streamLoadProps;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns == null ? new ArrayList<>() : new ArrayList<>(columns);
    }
}

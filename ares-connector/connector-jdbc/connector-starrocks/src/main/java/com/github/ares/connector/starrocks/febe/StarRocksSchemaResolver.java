package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.com.fasterxml.jackson.databind.JsonNode;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connector.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connector.jdbc.internal.dialect.StarRocksTypeMapper;
import com.github.ares.connector.jdbc.utils.CatalogUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

final class StarRocksSchemaResolver {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StarRocksSchemaResolver() {}

    static CatalogTable resolve(StarRocksFeBeConfig config) {
        if (StringUtils.isNotBlank(config.getQuery()) && StringUtils.isBlank(config.getJdbcUrl())) {
            throw new AresException(
                    "StarRocks febe custom query requires url so the result schema can be resolved");
        }
        if (StringUtils.isNotBlank(config.getJdbcUrl())) {
            return resolveByJdbc(config);
        }
        return resolveByHttp(config);
    }

    private static CatalogTable resolveByJdbc(StarRocksFeBeConfig config) {
        String sql =
                StringUtils.isNotBlank(config.getQuery())
                        ? config.getQuery()
                        : "SELECT * FROM `"
                                + config.getDatabase()
                                + "`.`"
                                + config.getTable()
                                + "` WHERE 1 = 0";
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .dbType(DatabaseIdentifier.STARROCKS)
                        .url(config.getJdbcUrl())
                        .driverName(config.getDriverName())
                        .username(config.getUsername())
                        .password(config.getPassword())
                        .queryTimeoutSec(config.getScanQueryTimeoutSec())
                        .build();
        try {
            Class.forName(config.getDriverName());
            Properties properties = new Properties();
            properties.setProperty("user", config.getUsername());
            properties.setProperty("password", config.getPassword());
            try (Connection connection =
                    DriverManager.getConnection(config.getJdbcUrl(), properties)) {
                connectionConfig.applySessionSettings(connection);
                CatalogTable queried =
                        CatalogUtils.getCatalogTable(connection, sql, new StarRocksTypeMapper());
                return CatalogTable.of(
                        TableIdentifier.of("starrocks", config.getDatabase(), config.getTable()),
                        queried);
            }
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException(
                    "Failed to resolve StarRocks schema through JDBC " + config.getJdbcUrl(), e);
        }
    }

    private static CatalogTable resolveByHttp(StarRocksFeBeConfig config) {
        AresException last = null;
        for (String feNode : config.getFeNodes()) {
            String url =
                    "http://"
                            + feNode
                            + "/api/"
                            + config.getDatabase()
                            + "/"
                            + config.getTable()
                            + "/_schema";
            try {
                String body =
                        StarRocksHttp.get(
                                url,
                                config.getUsername(),
                                config.getPassword(),
                                config.getHttpSocketTimeoutMs());
                return parseSchema(body, config.getDatabase(), config.getTable());
            } catch (Exception e) {
                last =
                        e instanceof AresException
                                ? (AresException) e
                                : new AresException(
                                        "Failed to read StarRocks schema from " + feNode, e);
            }
        }
        if (last != null) {
            throw last;
        }
        throw new AresException("StarRocks schema response is empty");
    }

    static CatalogTable parseSchema(String body, String database, String table) {
        try {
            JsonNode root = MAPPER.readTree(body);
            JsonNode status = root.get("status");
            if (status != null && status.isNumber() && status.asInt() != 200) {
                throw new AresException("StarRocks schema request failed: " + body);
            }
            JsonNode properties = root.get("properties");
            if (properties == null || !properties.isArray() || properties.size() == 0) {
                throw new AresException("StarRocks schema has no columns: " + body);
            }
            StarRocksTypeMapper mapper = new StarRocksTypeMapper();
            TableSchema.Builder builder = TableSchema.builder();
            for (JsonNode column : properties) {
                String name = column.path("name").asText();
                String type = column.path("type").asText("STRING");
                int precision = 0;
                int scale = 0;
                Long length = null;
                int paren = type.indexOf('(');
                if (paren > 0 && type.endsWith(")")) {
                    String args = type.substring(paren + 1, type.length() - 1);
                    String[] parts = args.split(",");
                    precision = Integer.parseInt(parts[0].trim());
                    length = (long) precision;
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1].trim());
                    }
                }
                AresDataType<?> dataType = mapper.map(type, precision, scale, name);
                String comment = column.path("comment").asText("");
                builder.column(
                        PhysicalColumn.builder()
                                .name(name)
                                .dataType(dataType)
                                .columnLength(length)
                                .scale(scale)
                                .nullable(true)
                                .comment(comment.isEmpty() ? null : comment)
                                .sourceType(type)
                                .options(Collections.emptyMap())
                                .build());
            }
            return CatalogTable.of(
                    TableIdentifier.of("starrocks", database, table),
                    builder.build(),
                    Collections.emptyMap(),
                    new ArrayList<>(),
                    "");
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException("Failed to parse StarRocks schema", e);
        }
    }

    static List<String> columnNames(CatalogTable catalogTable) {
        List<String> names = new ArrayList<>();
        for (Column column : catalogTable.getTableSchema().getColumns()) {
            if (column.isPhysical()) {
                names.add(column.getName());
            }
        }
        return names;
    }
}

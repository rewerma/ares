package com.github.ares.connctor.jdbc.utils;

import com.github.ares.api.table.catalog.Catalog;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.ConstraintKey;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TableIdentifier;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.factory.FactoryUtil;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.catalog.AbstractJdbcCatalog;
import com.github.ares.connctor.jdbc.catalog.JdbcCatalogOptions;
import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.config.JdbcSourceTableConfig;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectLoader;
import com.github.ares.connctor.jdbc.source.JdbcSourceTable;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JdbcCatalogUtils {
    private static final Logger log = LoggerFactory.getLogger(JdbcCatalogUtils.class);
    private static final String DEFAULT_CATALOG_NAME = "jdbc_catalog";

    public static Map<TablePath, JdbcSourceTable> getTables(
            JdbcConnectionConfig jdbcConnectionConfig, List<JdbcSourceTableConfig> tablesConfig)
            throws SQLException, ClassNotFoundException {
        Map<TablePath, JdbcSourceTable> tables = new LinkedHashMap<>();

        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        jdbcConnectionConfig.getUrl(), jdbcConnectionConfig.getCompatibleMode());
       /* Optional<Catalog> catalog = findCatalog(jdbcConnectionConfig, jdbcDialect);
        if (catalog.isPresent()) {
            try (AbstractJdbcCatalog jdbcCatalog = (AbstractJdbcCatalog) catalog.get()) {
                log.info("Loading catalog tables for catalog : {}", jdbcCatalog.getClass());

                jdbcCatalog.open();
                Map<String, Map<String, String>> unsupportedTable = new HashMap<>();
                for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                    try {
                        CatalogTable catalogTable =
                                getCatalogTable(tableConfig, jdbcCatalog, jdbcDialect);
                        TablePath tablePath = catalogTable.getTableId().toTablePath();
                        JdbcSourceTable jdbcSourceTable = new JdbcSourceTable(tablePath, tableConfig.getQuery(), tableConfig.getPartitionColumn(), tableConfig.getPartitionNumber(),
                                tableConfig.getPartitionStart(), tableConfig.getPartitionEnd(), catalogTable);
                        tables.put(tablePath, jdbcSourceTable);
                        log.info("Loaded catalog table : {}, {}", tablePath, jdbcSourceTable);
                    } catch (AresException e) {
                        throw e;
                    }
                }
                if (!unsupportedTable.isEmpty()) {
                    throw new AresException("unsupported " + unsupportedTable); //CommonError.getCatalogTablesWithUnsupportedType(jdbcDialect.dialectName(), unsupportedTable);
                }
                log.info(
                        "Loaded {} catalog tables for catalog : {}",
                        tables.size(),
                        jdbcCatalog.getClass());
            }
            return tables;
        }

        log.warn(
                "Catalog not found, loading tables from jdbc directly. url : {}",
                jdbcConnectionConfig.getUrl());
         */
        try (Connection connection = getConnection(jdbcConnectionConfig, jdbcDialect)) {
            log.info("Loading catalog tables for jdbc : {}", jdbcConnectionConfig.getUrl());
            for (JdbcSourceTableConfig tableConfig : tablesConfig) {
                CatalogTable catalogTable = getCatalogTable(tableConfig, connection, jdbcDialect);
                TablePath tablePath = catalogTable.getTableId().toTablePath();
                JdbcSourceTable jdbcSourceTable = new JdbcSourceTable(tablePath, tableConfig.getQuery(), tableConfig.getPartitionColumn(), tableConfig.getPartitionNumber(),
                        tableConfig.getPartitionStart(), tableConfig.getPartitionEnd(), catalogTable);


                tables.put(tablePath, jdbcSourceTable);
                log.info("Loaded catalog table : {}, {}", tablePath, jdbcSourceTable);
            }
            log.info(
                    "Loaded {} catalog tables for jdbc : {}",
                    tables.size(),
                    jdbcConnectionConfig.getUrl());
            return tables;
        }
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig,
            AbstractJdbcCatalog jdbcCatalog,
            JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(jdbcCatalog, tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath = jdbcCatalog.getTable(tablePath);
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            CatalogTable tableOfQuery = jdbcCatalog.getTable(tableConfig.getQuery());
            if (tableOfPath == null) {
                String catalogName =
                        tableOfQuery.getTableId() == null
                                ? DEFAULT_CATALOG_NAME
                                : tableOfQuery.getTableId().getCatalogName();
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(jdbcCatalog, tableConfig.getTablePath());
            return jdbcCatalog.getTable(tablePath);
        }

        return jdbcCatalog.getTable(tableConfig.getQuery());
    }

    static CatalogTable mergeCatalogTable(CatalogTable tableOfPath, CatalogTable tableOfQuery) {
        TableSchema tableSchemaOfPath = tableOfPath.getTableSchema();
        Map<String, Column> columnsOfPath =
                tableSchemaOfPath.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        Function.identity(),
                                        (o1, o2) -> o1,
                                        LinkedHashMap::new));
        TableSchema tableSchemaOfQuery = tableOfQuery.getTableSchema();
        Map<String, Column> columnsOfQuery =
                tableSchemaOfQuery.getColumns().stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName,
                                        Function.identity(),
                                        (o1, o2) -> o1,
                                        LinkedHashMap::new));
        Set<String> columnKeysOfQuery = columnsOfQuery.keySet();

        List<Column> columnsOfMerge =
                tableSchemaOfQuery.getColumns().stream()
                        .filter(
                                column ->
                                        columnsOfPath.containsKey(column.getName())
                                                && columnsOfPath
                                                .get(column.getName())
                                                .getDataType()
                                                .equals(
                                                        columnsOfQuery
                                                                .get(column.getName())
                                                                .getDataType()))
                        .map(column -> columnsOfPath.get(column.getName()))
                        .collect(Collectors.toList());
        boolean schemaIncludeAllColumns = columnsOfMerge.size() == columnKeysOfQuery.size();
        boolean schemaEquals =
                schemaIncludeAllColumns && columnsOfMerge.size() == columnsOfPath.size();
        if (schemaEquals) {
            return tableOfPath;
        }

        PrimaryKey primaryKeyOfPath = tableSchemaOfPath.getPrimaryKey();
        List<ConstraintKey> constraintKeysOfPath = tableSchemaOfPath.getConstraintKeys();
        List<String> partitionKeysOfPath = tableOfPath.getPartitionKeys();
        PrimaryKey primaryKeyOfMerge = null;
        List<ConstraintKey> constraintKeysOfMerge = new ArrayList<>();
        List<String> partitionKeysOfMerge = new ArrayList<>();

        if (primaryKeyOfPath != null
                && columnKeysOfQuery.containsAll(primaryKeyOfPath.getColumnNames())) {
            primaryKeyOfMerge = primaryKeyOfPath;
        }
        if (constraintKeysOfPath != null) {
            for (ConstraintKey constraintKey : constraintKeysOfPath) {
                Set<String> constraintKeyFields =
                        constraintKey.getColumnNames().stream()
                                .map(e -> e.getColumnName())
                                .collect(Collectors.toSet());
                if (columnKeysOfQuery.containsAll(constraintKeyFields)) {
                    constraintKeysOfMerge.add(constraintKey);
                }
            }
        }
        if (partitionKeysOfPath != null && columnKeysOfQuery.containsAll(partitionKeysOfPath)) {
            partitionKeysOfMerge = partitionKeysOfPath;
        }
        if (schemaIncludeAllColumns) {
            return CatalogTable.of(
                    tableOfPath.getTableId(),
                    TableSchema.builder()
                            .primaryKey(primaryKeyOfMerge)
                            .constraintKey(constraintKeysOfMerge)
                            .columns(columnsOfMerge)
                            .build(),
                    tableOfPath.getOptions(),
                    partitionKeysOfMerge,
                    tableOfPath.getComment());
        }

        String catalogName =
                tableOfQuery.getTableId() == null
                        ? DEFAULT_CATALOG_NAME
                        : tableOfQuery.getTableId().getCatalogName();
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogName,
                        tableOfPath.getTableId().getDatabaseName(),
                        tableOfPath.getTableId().getSchemaName(),
                        tableOfPath.getTableId().getTableName());
        CatalogTable mergedCatalogTable =
                CatalogTable.of(
                        tableIdentifier,
                        TableSchema.builder()
                                .primaryKey(primaryKeyOfMerge)
                                .constraintKey(constraintKeysOfMerge)
                                .columns(tableSchemaOfQuery.getColumns())
                                .build(),
                        tableOfPath.getOptions(),
                        partitionKeysOfMerge,
                        tableOfPath.getComment());

        log.info("Merged catalog table of path {}", tableOfPath.getTableId().toTablePath());
        return mergedCatalogTable;
    }

    private static CatalogTable getCatalogTable(
            JdbcSourceTableConfig tableConfig, Connection connection, JdbcDialect jdbcDialect)
            throws SQLException {
        if (Strings.isNullOrEmpty(tableConfig.getTablePath())
                && Strings.isNullOrEmpty(tableConfig.getQuery())) {
            throw new IllegalArgumentException(
                    "Either table path or query must be specified in source configuration.");
        }

        if (StringUtils.isNotEmpty(tableConfig.getTablePath())
                && StringUtils.isNotEmpty(tableConfig.getQuery())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            CatalogTable tableOfPath = null;
            try {
                tableOfPath = CatalogUtils.getCatalogTable(connection, tablePath);
            } catch (Exception e) {
                // ignore
                log.debug("User-defined table path: {}", tablePath);
            }
            CatalogTable tableOfQuery =
                    getCatalogTable(connection, tableConfig.getQuery(), jdbcDialect);
            if (tableOfPath == null) {
                String catalogName =
                        tableOfQuery.getTableId() == null
                                ? DEFAULT_CATALOG_NAME
                                : tableOfQuery.getTableId().getCatalogName();
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(tableIdentifier, tableOfQuery);
            }
            return mergeCatalogTable(tableOfPath, tableOfQuery);
        }
        if (StringUtils.isNotEmpty(tableConfig.getTablePath())) {
            TablePath tablePath = jdbcDialect.parse(tableConfig.getTablePath());
            return CatalogUtils.getCatalogTable(connection, tablePath);
        }

        return getCatalogTable(connection, tableConfig.getQuery(), jdbcDialect);
    }

    private static CatalogTable getCatalogTable(
            Connection connection, String sqlQuery, JdbcDialect jdbcDialect) throws SQLException {
        ResultSetMetaData resultSetMetaData =
                jdbcDialect.getResultSetMetaData(connection, sqlQuery);
        return CatalogUtils.getCatalogTable(
                resultSetMetaData, jdbcDialect.getJdbcDialectTypeMapper(), sqlQuery);
    }

    private static Connection getConnection(JdbcConnectionConfig config, JdbcDialect jdbcDialect)
            throws SQLException, ClassNotFoundException {
        JdbcConnectionProvider connectionProvider = jdbcDialect.getJdbcConnectionProvider(config);
        return connectionProvider.getOrEstablishConnection();
    }

    public static Optional<Catalog> findCatalog(JdbcConnectionConfig config, JdbcDialect dialect) {
        ReadonlyConfig catalogConfig = extractCatalogConfig(config);
        return FactoryUtil.createOptionalCatalog(
                dialect.dialectName(),
                catalogConfig,
                JdbcCatalogUtils.class.getClassLoader(),
                dialect.dialectName());
    }

    private static ReadonlyConfig extractCatalogConfig(JdbcConnectionConfig config) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put(JdbcCatalogOptions.BASE_URL.key(), config.getUrl());
        config.getUsername()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.USERNAME.key(), val));
        config.getPassword()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.PASSWORD.key(), val));
        return ReadonlyConfig.fromMap(catalogConfig);
    }
}

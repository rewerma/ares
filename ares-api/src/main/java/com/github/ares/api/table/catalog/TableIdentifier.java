package com.github.ares.api.table.catalog;

import java.io.Serializable;

public final class TableIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String catalogName;

    private final String databaseName;

    private final String schemaName;

    private final String tableName;

    public TableIdentifier(String catalogName, String databaseName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public static TableIdentifier of(String catalogName, String databaseName, String tableName) {
        return new TableIdentifier(catalogName, databaseName, null, tableName);
    }

    public static TableIdentifier of(String catalogName, TablePath tablePath) {
        return new TableIdentifier(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    public static TableIdentifier of(
            String catalogName, String databaseName, String schemaName, String tableName) {
        return new TableIdentifier(catalogName, databaseName, schemaName, tableName);
    }

    public TablePath toTablePath() {
        return TablePath.of(databaseName, schemaName, tableName);
    }

    public TableIdentifier copy() {
        return TableIdentifier.of(catalogName, databaseName, schemaName, tableName);
    }

    @Override
    public String toString() {
        if (schemaName == null) {
            return String.join(".", catalogName, databaseName, tableName);
        }
        return String.join(".", catalogName, databaseName, schemaName, tableName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }
}

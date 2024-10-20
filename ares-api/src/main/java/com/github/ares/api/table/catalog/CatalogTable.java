package com.github.ares.api.table.catalog;

import com.github.ares.api.table.type.AresRowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Represent the table metadata in Ares. */
public final class CatalogTable implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Used to identify the table. */
    private final TableIdentifier tableId;

    /** The table schema metadata. */
    private final TableSchema tableSchema;

    private final Map<String, String> options;

    private final List<String> partitionKeys;

    private final String comment;

    private final String catalogName;

    public static CatalogTable of(TableIdentifier tableId, CatalogTable catalogTable) {
        CatalogTable newTable = catalogTable.copy();
        return new CatalogTable(
                tableId,
                newTable.getTableSchema(),
                newTable.getOptions(),
                newTable.getPartitionKeys(),
                newTable.getComment(),
                newTable.getCatalogName());
    }

    public static CatalogTable of(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment) {
        return new CatalogTable(
                tableId, tableSchema, options, partitionKeys, comment, tableId.getCatalogName());
    }

    public static CatalogTable of(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment,
            String catalogName) {
        return new CatalogTable(tableId, tableSchema, options, partitionKeys, comment, catalogName);
    }

    private CatalogTable(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment) {
        this(tableId, tableSchema, options, partitionKeys, comment, tableId.getCatalogName());
    }

    private CatalogTable(
            TableIdentifier tableId,
            TableSchema tableSchema,
            Map<String, String> options,
            List<String> partitionKeys,
            String comment,
            String catalogName) {
        this.tableId = tableId;
        this.tableSchema = tableSchema;
        // Make sure the options and partitionKeys are mutable
        this.options = new HashMap<>(options);
        this.partitionKeys = new ArrayList<>(partitionKeys);
        this.comment = comment;
        this.catalogName = catalogName;
    }

    public CatalogTable copy() {
        return new CatalogTable(
                tableId.copy(),
                tableSchema.copy(),
                new HashMap<>(options),
                new ArrayList<>(partitionKeys),
                comment,
                catalogName);
    }

    public TableIdentifier getTableId() {
        return tableId;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public AresRowType getAresRowType() {
        return tableSchema.toPhysicalRowDataType();
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public String getComment() {
        return comment;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toString() {
        return "CatalogTable{"
                + "tableId="
                + tableId
                + ", tableSchema="
                + tableSchema
                + ", options="
                + options
                + ", partitionKeys="
                + partitionKeys
                + ", comment='"
                + comment
                + '\''
                + ", catalogName='"
                + catalogName
                + '\''
                + '}';
    }
}

package com.github.ares.connctor.jdbc.catalog.psql;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.ConstraintKey;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.psql.PostgresTypeConverter;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PostgresCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;
    private String fieldIde;
    private List<ConstraintKey> constraintKeys;
    public Boolean isHaveConstraintKey = false;

    @Getter public List<String> createIndexSqls = new ArrayList<>();

    public PostgresCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append(CatalogUtils.quoteIdentifier("CREATE TABLE ", fieldIde))
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(
                                column ->
                                        CatalogUtils.quoteIdentifier(
                                                buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && StringUtils.equals(
                                        primaryKey.getPrimaryKey(),
                                        constraintKey.getConstraintName()))) {
                    continue;
                }
                isHaveConstraintKey = true;
                switch (constraintKey.getConstraintType()) {
                    case UNIQUE_KEY:
                        String uniqueKeySql = buildUniqueKeySql(constraintKey);
                        columnSqls.add("\t" + uniqueKeySql);
                        break;
                    case INDEX_KEY:
                        String indexKeySql = buildIndexKeySql(tablePath, constraintKey);
                        createIndexSqls.add(indexKeySql);
                        break;
                    case FOREIGN_KEY:
                        // todo: add foreign key
                        break;
                }
            }
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns, tablePath.getSchemaAndTableName("\"")))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append("\n");
            createTableSql.append(String.join(";\n", commentSqls)).append(";");
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        // For simplicity, assume the column type in AresDataType is the same as in PostgreSQL
        String columnType =
                StringUtils.equalsIgnoreCase(DatabaseIdentifier.POSTGRESQL, sourceCatalogName)
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (primaryKey != null && primaryKey.getColumnNames().contains(column.getName())) {
            columnSql.append(" PRIMARY KEY");
        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        return PostgresTypeConverter.INSTANCE.reconvert(column).getColumnType();
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier("COMMENT ON COLUMN ", fieldIde))
                .append(tableName)
                .append(".");
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "\""))
                .append(CatalogUtils.quoteIdentifier(" IS '", fieldIde))
                .append(column.getComment())
                .append("'");
        return columnCommentSql.toString();
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String constraintName = constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));
        return "CONSTRAINT " + constraintName + " UNIQUE (" + indexColumns + ")";
    }

    private String buildIndexKeySql(TablePath tablePath, ConstraintKey constraintKey) {
        // We add table name to index name to avoid name conflict in PG
        // Since index name in PG should unique in the schema
        String constraintName = tablePath.getTableName() + "_" + constraintKey.getConstraintName();
        if (constraintName.length() > 25) {
            constraintName = constraintName.substring(0, 25);
        }
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));

        return "CREATE INDEX "
                + constraintName
                + " ON "
                + tablePath.getSchemaAndTableName("\"")
                + "("
                + indexColumns
                + ");";
    }
}

package com.github.ares.connctor.jdbc.catalog.oracle;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.oracle.OracleTypeConverter;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class OracleCreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;
    private String fieldIde;

    public OracleCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(column -> CatalogUtils.getFieldIde(buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        // Add primary key directly in the create table statement
        if (primaryKey != null
                && primaryKey.getColumnNames() != null
                && primaryKey.getColumnNames().size() > 0) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                column ->
                                        buildColumnCommentSql(
                                                column, tablePath.getSchemaAndTableName("\"")))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append(";\n");
            createTableSql.append(String.join(";\n", commentSqls));
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        String columnType =
                StringUtils.equalsIgnoreCase(DatabaseIdentifier.ORACLE, sourceCatalogName)
                        ? column.getSourceType()
                        : OracleTypeConverter.INSTANCE.reconvert(column).getColumnType();
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));

        // In Oracle database, the maximum length for an identifier is 30 characters.
        String primaryKeyStr = primaryKey.getPrimaryKey();
        if (primaryKeyStr.length() > 25) {
            primaryKeyStr = primaryKeyStr.substring(0, 25);
        }

        return CatalogUtils.getFieldIde(
                "CONSTRAINT "
                        + primaryKeyStr
                        + "_"
                        + randomSuffix
                        + " PRIMARY KEY ("
                        + columnNamesString
                        + ")",
                fieldIde);
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
}

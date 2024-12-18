package com.github.ares.connctor.jdbc.catalog.sqlserver;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.ConstraintKey;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.ares.com.google.common.base.Preconditions.checkArgument;
import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;


public class SqlServerCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private String fieldIde;

    private SqlServerCreateTableSqlBuilder(String tableName) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
    }

    public static SqlServerCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new SqlServerCreateTableSqlBuilder(tablePath.getTableName())
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"));
    }

    public SqlServerCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public SqlServerCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public SqlServerCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public SqlServerCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public SqlServerCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public SqlServerCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public SqlServerCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public SqlServerCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(TablePath tablePath, CatalogTable catalogTable) {
        List<String> sqls = new ArrayList<>();
        String sqlTableName = tablePath.getFullNameWithQuoted("[", "]");
        Map<String, String> columnComments = new HashMap<>();
        sqls.add(
                String.format(
                        "IF OBJECT_ID('%s', 'U') IS NULL \n"
                                + "BEGIN \n"
                                + "CREATE TABLE %s ( \n%s\n)",
                        sqlTableName,
                        sqlTableName,
                        buildColumnsIdentifySql(catalogTable.getCatalogName(), columnComments)));
        if (engine != null) {
            sqls.add("ENGINE = " + engine);
        }
        if (charset != null) {
            sqls.add("DEFAULT CHARSET = " + charset);
        }
        if (collate != null) {
            sqls.add("COLLATE = " + collate);
        }
        String sqlTableSql = String.join(" ", sqls) + ";";
        sqlTableSql = CatalogUtils.quoteIdentifier(sqlTableSql, fieldIde);
        StringBuilder tableAndColumnComment = new StringBuilder();
        if (comment != null) {
            sqls.add("COMMENT = '" + comment + "'");
            tableAndColumnComment.append(
                    String.format(
                            "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s';\n",
                            tablePath.getDatabaseName(),
                            comment,
                            tablePath.getSchemaName(),
                            tablePath.getTableName()));
        }
        String columnComment =
                "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';\n";
        columnComments.forEach(
                (fieldName, com) -> {
                    tableAndColumnComment.append(
                            String.format(
                                    columnComment,
                                    tablePath.getDatabaseName(),
                                    com,
                                    tablePath.getSchemaName(),
                                    tablePath.getTableName(),
                                    fieldName));
                });
        return String.join("\n", sqlTableSql, tableAndColumnComment.toString(), "END");
    }

    private String buildColumnsIdentifySql(String catalogName, Map<String, String> columnComments) {
        List<String> columnSqls = new ArrayList<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName, columnComments));
        }
        if (primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(
            Column column, String catalogName, Map<String, String> columnComments) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add("[" + column.getName() + "]");
        if (StringUtils.equals(catalogName, DatabaseIdentifier.SQLSERVER)) {
            columnSqls.add(column.getSourceType());
        } else {
            columnSqls.add(SqlServerTypeConverter.INSTANCE.reconvert(column).getColumnType());
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        // comment
        if (column.getComment() != null) {
            columnComments.put(column.getName(), column.getComment());
        }

        return String.join(" ", columnSqls);
    }

    private String buildPrimaryKeySql() {
        //                        .map(columnName -> "`" + columnName + "`")
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "[" + columnName + "]")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", key);
    }

    private String buildConstraintKeySql(ConstraintKey constraintKey) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn -> {
                                    if (constraintKeyColumn.getSortType() == null) {
                                        return String.format(
                                                "`%s`", constraintKeyColumn.getColumnName());
                                    }
                                    return String.format(
                                            "`%s` %s",
                                            constraintKeyColumn.getColumnName(),
                                            constraintKeyColumn.getSortType().name());
                                })
                        .collect(Collectors.joining(", "));
        String keyName = null;
        switch (constraintType) {
            case INDEX_KEY:
                keyName = "KEY";
                break;
            case UNIQUE_KEY:
                keyName = "UNIQUE KEY";
                break;
            case FOREIGN_KEY:
                keyName = "FOREIGN KEY";
                // todo:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported constraint type: " + constraintType);
        }
        return String.format(
                "%s `%s` (%s)", keyName, constraintKey.getConstraintName(), indexColumns);
    }
}

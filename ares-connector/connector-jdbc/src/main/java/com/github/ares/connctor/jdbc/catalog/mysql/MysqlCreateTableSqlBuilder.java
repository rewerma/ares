package com.github.ares.connctor.jdbc.catalog.mysql;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.ConstraintKey;
import com.github.ares.api.table.catalog.PrimaryKey;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;
import com.github.ares.connctor.jdbc.internal.dialect.mysql.MySqlTypeConverter;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import com.mysql.cj.MysqlType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;

public class MysqlCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private String fieldIde;

    private MysqlCreateTableSqlBuilder(String tableName) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
    }

    public static MysqlCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new MysqlCreateTableSqlBuilder(tablePath.getTableName())
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"));
    }

    public MysqlCreateTableSqlBuilder addColumn(List<Column> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new AresException("columns must not be empty");
        }
        this.columns = columns;
        return this;
    }

    public MysqlCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public MysqlCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public MysqlCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public MysqlCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public MysqlCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public MysqlCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public MysqlCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(String catalogName) {
        List<String> sqls = new ArrayList<>();
        sqls.add(
                String.format(
                        "CREATE TABLE %s (\n%s\n)",
                        CatalogUtils.quoteIdentifier(tableName, fieldIde, "`"),
                        buildColumnsIdentifySql(catalogName)));
        if (engine != null) {
            sqls.add("ENGINE = " + engine);
        }
        if (charset != null) {
            sqls.add("DEFAULT CHARSET = " + charset);
        }
        if (collate != null) {
            sqls.add("COLLATE = " + collate);
        }
        if (comment != null) {
            sqls.add("COMMENT = '" + comment + "'");
        }
        return String.join(" ", sqls) + ";";
    }

    private String buildColumnsIdentifySql(String catalogName) {
        List<String> columnSqls = new ArrayList<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName));
        }
        if (primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (constraintKeys != null && !constraintKeys.isEmpty()) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
                //                columnSqls.add("\t" + buildConstraintKeySql(constraintKey));
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(Column column, String catalogName) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "`"));
        boolean isSupportDef = true;
        if (StringUtils.equals(catalogName, DatabaseIdentifier.MYSQL)) {
            columnSqls.add(column.getSourceType());
        } else {
            BasicTypeDefine<MysqlType> typeDefine = MySqlTypeConverter.INSTANCE.reconvert(column);
            columnSqls.add(typeDefine.getColumnType());
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        if (column.getComment() != null) {
            columnSqls.add("COMMENT '" + column.getComment() + "'");
        }

        return String.join(" ", columnSqls);
    }

    private String buildPrimaryKeySql() {
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "`" + columnName + "`")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
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

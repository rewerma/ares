package com.github.ares.connctor.jdbc.catalog.sqlserver;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.exception.DatabaseNotExistException;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.connctor.jdbc.catalog.AbstractJdbcCatalog;
import com.github.ares.connctor.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;
import com.github.ares.connctor.jdbc.internal.dialect.sqlserver.SqlserverTypeMapper;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT tbl.name AS table_name,\n"
                    + "       col.name AS column_name,\n"
                    + "       ext.value AS comment,\n"
                    + "       col.column_id AS column_id,\n"
                    + "       types.name AS type,\n"
                    + "       col.max_length AS max_length,\n"
                    + "       col.precision AS precision,\n"
                    + "       col.scale AS scale,\n"
                    + "       col.is_nullable AS is_nullable,\n"
                    + "       def.definition AS default_value\n"
                    + "FROM sys.tables tbl\n"
                    + "    INNER JOIN sys.columns col ON tbl.object_id = col.object_id\n"
                    + "    LEFT JOIN sys.types types ON col.user_type_id = types.user_type_id\n"
                    + "    LEFT JOIN sys.extended_properties ext ON ext.major_id = col.object_id AND ext.minor_id = col.column_id\n"
                    + "    LEFT JOIN sys.default_constraints def ON col.default_object_id = def.object_id AND ext.minor_id = col.column_id AND ext.name = 'MS_Description'\n"
                    + "WHERE schema_name(tbl.schema_id) = '%s' %s\n"
                    + "ORDER BY tbl.name, col.column_id";

    public SqlServerCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT NAME FROM sys.databases";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT TABLE_SCHEMA, TABLE_NAME FROM "
                + databaseName
                + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        String tableSql =
                StringUtils.isNotEmpty(tablePath.getTableName())
                        ? "AND tbl.name = '" + tablePath.getTableName() + "'"
                        : "";

        return String.format(SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tableSql);
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String dataType = resultSet.getString("type");
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getBoolean("is_nullable");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision((long) precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return SqlServerTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return SqlServerCreateTableSqlBuilder.builder(tablePath, table).build(tablePath, table);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getFullName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE %s", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE %s;", databaseName);
    }

    @Override
    protected void dropDatabaseInternal(String databaseName)  {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }
            return listTables(defaultDatabase).contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SqlserverTypeMapper());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select TOP 1 * from %s ;", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format("TRUNCATE TABLE  %s", tablePath.getFullNameWithQuoted("[", "]"));
    }
}

package com.github.ares.connctor.jdbc.catalog.dm;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.catalog.exception.DatabaseNotExistException;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.connctor.jdbc.catalog.AbstractJdbcCatalog;
import com.github.ares.connctor.jdbc.internal.dialect.dm.DmdbTypeConverter;
import com.github.ares.connctor.jdbc.internal.dialect.dm.DmdbTypeMapper;
import com.github.ares.connctor.jdbc.utils.CatalogUtils;
import com.github.ares.connctor.jdbc.utils.JdbcUrlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DamengCatalog extends AbstractJdbcCatalog {
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList("SYS", "SYSDBA", "SYSSSO", "SYSAUDITOR", "CTISYS"));

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COLUMNS.DATA_LENGTH, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "
                    + ", COLUMNS.NULLABLE, COLUMNS.DATA_DEFAULT, COMMENTS.COMMENTS "
                    + "FROM ALL_TAB_COLUMNS COLUMNS "
                    + "LEFT JOIN ALL_COL_COMMENTS COMMENTS "
                    + "ON COLUMNS.OWNER = COMMENTS.SCHEMA_NAME "
                    + "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME "
                    + "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME "
                    + "WHERE COLUMNS.OWNER = '%s' "
                    + "AND COLUMNS.TABLE_NAME = '%s' "
                    + "ORDER BY COLUMNS.COLUMN_ID ASC";

    public DamengCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT name FROM v$database";
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", getTableName(tablePath));
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName().toUpperCase();
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
            return null;
        }
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE");
        long columnLength = resultSet.getLong("DATA_LENGTH");
        long columnPrecision = resultSet.getLong("DATA_PRECISION");
        int columnScale = resultSet.getInt("DATA_SCALE");
        String columnComment = resultSet.getString("COMMENTS");
        Object defaultValue = resultSet.getObject("DATA_DEFAULT");
        boolean isNullable = resultSet.getString("NULLABLE").equals("Y");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return DmdbTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public boolean tableExists(TablePath tablePath) {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                        .contains(tablePath.getSchemaAndTableName());
            }
            return listTables().contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (PreparedStatement ps =
                     getConnection(defaultUrl)
                             .prepareStatement("SELECT OWNER, TABLE_NAME FROM ALL_TABLES");
             ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
                    continue;
                }
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new AresException(
                    String.format("Failed listing table in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DmdbTypeMapper());
    }
}

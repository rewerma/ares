package com.github.ares.connector.jdbc.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.jdbc.internal.dialect.JdbcDialect;
import com.github.ares.connector.jdbc.internal.dialect.JdbcDialectLoader;
import com.github.ares.connector.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import java.io.Serializable;
import java.util.List;

public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private String dbType;
    private JdbcConnectionConfig jdbcConnectionConfig;
    private String simpleSql;
    private String database;
    private String table;
    private String fieldIde;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public JdbcConnectionConfig getJdbcConnectionConfig() {
        return jdbcConnectionConfig;
    }

    public void setJdbcConnectionConfig(JdbcConnectionConfig jdbcConnectionConfig) {
        this.jdbcConnectionConfig = jdbcConnectionConfig;
    }

    public String getSimpleSql() {
        return simpleSql;
    }

    public void setSimpleSql(String simpleSql) {
        this.simpleSql = simpleSql;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFieldIde() {
        return fieldIde;
    }

    public void setFieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    public static JdbcSinkConfig of(ReadonlyConfig config, List<Column> sourceColumns) {
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig();
        jdbcSinkConfig.setDbType(config.get(CommonOptions.CONNECTOR));
        jdbcSinkConfig.setJdbcConnectionConfig(JdbcConnectionConfig.of(config));
        config.getOptional(JdbcOptions.DATABASE).ifPresent(jdbcSinkConfig::setDatabase);
        config.getOptional(JdbcOptions.TABLE).ifPresent(jdbcSinkConfig::setTable);
        config.getOptional(JdbcOptions.FIELD_IDE)
                .ifPresent(fieldIdeEnum -> jdbcSinkConfig.setFieldIde(fieldIdeEnum.getValue()));

        String sinkType = config.get(CommonOptions.SINK_TYPE);
        FieldIdeEnum fieldIdeEnum = config.getOptional(JdbcOptions.FIELD_IDE).orElse(null);
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        jdbcSinkConfig.getDbType(),
                        jdbcSinkConfig.getJdbcConnectionConfig().getUrl(),
                        jdbcSinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        String statementSql =
                (String)
                        JdbcSinkTypeHandler.handleSinkType(
                                SinkType.valueOf(sinkType), config, sourceColumns, dialect);
        jdbcSinkConfig.setSimpleSql(statementSql);

        return jdbcSinkConfig;
    }
}

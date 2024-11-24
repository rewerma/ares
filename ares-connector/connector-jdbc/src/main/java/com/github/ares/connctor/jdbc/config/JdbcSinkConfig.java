package com.github.ares.connctor.jdbc.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.CriteriaClause;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.github.ares.connctor.jdbc.config.JdbcOptions.ENABLE_UPSERT;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.IS_PRIMARY_KEY_UPDATED;
import static com.github.ares.connctor.jdbc.config.JdbcOptions.SUPPORT_UPSERT_BY_INSERT_ONLY;

public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private boolean isExactlyOnce;
    private String simpleSql;
    private String database;
    private String table;
    private List<String> primaryKeys;
    private boolean enableUpsert;
    private boolean isPrimaryKeyUpdated = true;
    private boolean supportUpsertByInsertOnly;

    public JdbcConnectionConfig getJdbcConnectionConfig() {
        return jdbcConnectionConfig;
    }

    public void setJdbcConnectionConfig(JdbcConnectionConfig jdbcConnectionConfig) {
        this.jdbcConnectionConfig = jdbcConnectionConfig;
    }

    public boolean isExactlyOnce() {
        return isExactlyOnce;
    }

    public void setExactlyOnce(boolean exactlyOnce) {
        isExactlyOnce = exactlyOnce;
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

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public boolean isEnableUpsert() {
        return enableUpsert;
    }

    public void setEnableUpsert(boolean enableUpsert) {
        this.enableUpsert = enableUpsert;
    }

    public boolean isPrimaryKeyUpdated() {
        return isPrimaryKeyUpdated;
    }

    public void setPrimaryKeyUpdated(boolean primaryKeyUpdated) {
        isPrimaryKeyUpdated = primaryKeyUpdated;
    }

    public boolean isSupportUpsertByInsertOnly() {
        return supportUpsertByInsertOnly;
    }

    public void setSupportUpsertByInsertOnly(boolean supportUpsertByInsertOnly) {
        this.supportUpsertByInsertOnly = supportUpsertByInsertOnly;
    }

    public static JdbcSinkConfig of(ReadonlyConfig config, List<Column> sourceColumns) {
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig();
        jdbcSinkConfig.setJdbcConnectionConfig(JdbcConnectionConfig.of(config));
        jdbcSinkConfig.setExactlyOnce(config.get(JdbcOptions.IS_EXACTLY_ONCE));
        config.getOptional(JdbcOptions.PRIMARY_KEYS).ifPresent(jdbcSinkConfig::setPrimaryKeys);
        config.getOptional(JdbcOptions.DATABASE).ifPresent(jdbcSinkConfig::setDatabase);
        config.getOptional(JdbcOptions.TABLE).ifPresent(jdbcSinkConfig::setTable);
        jdbcSinkConfig.setEnableUpsert(config.get(ENABLE_UPSERT));
        jdbcSinkConfig.setPrimaryKeyUpdated(config.get(IS_PRIMARY_KEY_UPDATED));
        jdbcSinkConfig.setSupportUpsertByInsertOnly(config.get(SUPPORT_UPSERT_BY_INSERT_ONLY));

        SinkType sinkType = config.get(CommonOptions.SINK_TYPE);
        String statementSql = (String) JdbcSinkTypeHandler.handleSinkType(sinkType, config, sourceColumns);
        jdbcSinkConfig.setSimpleSql(statementSql);

        return jdbcSinkConfig;
    }


}

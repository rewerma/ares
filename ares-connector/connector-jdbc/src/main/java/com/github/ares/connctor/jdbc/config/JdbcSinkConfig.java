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
    private String updateSql;
    private String deleteSql;
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

    public String getUpdateSql() {
        return updateSql;
    }

    public void setUpdateSql(String updateSql) {
        this.updateSql = updateSql;
    }

    public String getDeleteSql() {
        return deleteSql;
    }

    public void setDeleteSql(String deleteSql) {
        this.deleteSql = deleteSql;
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

        String sinkType = config.get(CommonOptions.SINK_TYPE);
        if (SinkType.INSERT.name().equalsIgnoreCase(sinkType)) {
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(config.get(JdbcOptions.TABLE_NAME));
            List<String> insertColumns = config.get(CommonOptions.INSERT_COLUMNS);
            if (insertColumns != null && !insertColumns.isEmpty()) {
                sql.append(" (").append(String.join(", ", insertColumns)).append(") ");
            }
            sql.append(" VALUES (");
            StringJoiner sj = new StringJoiner(", ");
            for (int j = 0; j < sourceColumns.size(); j++) {
                sj.add("?");
            }
            sql.append(sj).append(")");
            jdbcSinkConfig.setSimpleSql(sql.toString());
        } else if (SinkType.UPDATE.name().equalsIgnoreCase(sinkType)) {
            StringBuilder sql = new StringBuilder("UPDATE ").append(config.get(JdbcOptions.TABLE_NAME))
                    .append(" SET ");
            List<String> updateColumns = config.get(CommonOptions.UPDATE_COLUMNS);
            CriteriaClause whereClause = config.get(CommonOptions.WHERE_CLAUSE);
            if (updateColumns == null || updateColumns.isEmpty() || whereClause == null || whereClause.getOperator() == null) {
                throw new AresException("Update set or where columns is empty");
            }
            StringJoiner setSj = new StringJoiner(", ");
            for (String updateColumn : updateColumns) {
                setSj.add(updateColumn + "=?");
            }
            sql.append(setSj).append(" WHERE ");
            StringBuilder whereSql = new StringBuilder();
            visitWhereClause(whereClause, whereSql);
            sql.append(whereSql);
            jdbcSinkConfig.setUpdateSql(sql.toString());
        } else if (SinkType.DELETE.name().equalsIgnoreCase(sinkType)) {
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(config.get(JdbcOptions.TABLE_NAME));
            CriteriaClause whereClause = config.get(CommonOptions.WHERE_CLAUSE);
            if (whereClause == null) {
                throw new AresException("Delete where clause is empty");
            }
            sql.append(" WHERE ");
            StringBuilder whereSql = new StringBuilder();
            visitWhereClause(whereClause, whereSql);
            sql.append(whereSql);
            jdbcSinkConfig.setDeleteSql(sql.toString());
        } else if (SinkType.TRUNCATE.name().equalsIgnoreCase(sinkType)) {
            jdbcSinkConfig.setSimpleSql("TRUNCATE TABLE " + config.get(JdbcOptions.TABLE_NAME));
        } else {
            throw new AresException(String.format("Unsupported sink type: %s for JDBC", sinkType));
        }

        return jdbcSinkConfig;
    }

    private static void visitWhereClause(CriteriaClause clause, StringBuilder whereSql) {
        if ("AND".equalsIgnoreCase(clause.getOperator())) {
            whereSql.append(" ( ");
            visitWhereClause(clause.getLeftCriteria(), whereSql);
            whereSql.append(" AND ");
            visitWhereClause(clause.getRightCriteria(), whereSql);
            whereSql.append(" ) ");
        } else if ("OR".equalsIgnoreCase(clause.getOperator())) {
            whereSql.append(" ( ");
            visitWhereClause(clause.getLeftCriteria(), whereSql);
            whereSql.append(" OR ");
            visitWhereClause(clause.getRightCriteria(), whereSql);
            whereSql.append(" ) ");
        } else if (clause.getLeftExpr() != null) {
            if (clause.getRightExpr() != null) {
                whereSql.append(clause.getLeftExpr()).append(" ").append(clause.getOperator()).append(" ? ");
            } else if (clause.getInItems() != null) {
                List<String> placeholders = new ArrayList<>();
                for (int i = 0; i < clause.getInItems().size(); i++) {
                    placeholders.add("?");
                }
                whereSql.append(clause.getLeftExpr()).append(" IN ( ").append(String.join(", ", placeholders)).append(" ) ");
            }
        } else {
            throw new AresException("Unsupported where clause: " + clause);
        }
    }
}

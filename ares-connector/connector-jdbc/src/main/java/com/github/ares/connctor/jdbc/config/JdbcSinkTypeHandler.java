package com.github.ares.connctor.jdbc.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.CriteriaClause;
import com.github.ares.api.common.SinkType;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class JdbcSinkTypeHandler {
    public static Object handleSinkType(SinkType sinkType, ReadonlyConfig config, List<Column> sourceColumns) {
        if (sinkType == null) {
            return null;
        }
        String resultSql;
        switch (sinkType) {
            case INSERT: {
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
                resultSql = sql.toString();
                break;
            }
            case UPDATE: {
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
                resultSql = sql.toString();
                break;
            }
            case DELETE: {
                StringBuilder sql = new StringBuilder("DELETE FROM ").append(config.get(JdbcOptions.TABLE_NAME));
                CriteriaClause whereClause = config.get(CommonOptions.WHERE_CLAUSE);
                if (whereClause == null) {
                    throw new AresException("Delete where clause is empty");
                }
                sql.append(" WHERE ");
                StringBuilder whereSql = new StringBuilder();
                visitWhereClause(whereClause, whereSql);
                sql.append(whereSql);
                resultSql = sql.toString();
                break;
            }
            case TRUNCATE:
                resultSql = "TRUNCATE TABLE " + config.get(JdbcOptions.TABLE_NAME);
                break;
            default:
                throw new AresException(String.format("Unsupported sink type: %s for JDBC", sinkType));
        }
        return resultSql;
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

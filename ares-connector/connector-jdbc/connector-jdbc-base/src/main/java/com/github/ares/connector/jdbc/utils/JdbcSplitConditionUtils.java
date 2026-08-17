package com.github.ares.connector.jdbc.utils;

import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connector.jdbc.internal.dialect.JdbcDialect;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class JdbcSplitConditionUtils {

    private JdbcSplitConditionUtils() {}

    public static AresRowType chunkSplitKey(AresRowType splitKey) {
        if (splitKey == null || splitKey.getTotalFields() == 0) {
            return splitKey;
        }
        return new AresRowType(
                new String[] {splitKey.getFieldNames()[0]},
                new com.github.ares.api.table.type.AresDataType[] {splitKey.getFieldType(0)});
    }

    public static String buildSplitCondition(
            AresRowType splitKey, JdbcDialect dialect, Object[] splitStart, Object[] splitEnd) {
        AresRowType chunkKey = chunkSplitKey(splitKey);
        boolean isFirstSplit = splitStart == null;
        boolean isLastSplit = splitEnd == null;
        if (isFirstSplit && isLastSplit) {
            return null;
        }

        StringBuilder sql = new StringBuilder();
        if (isFirstSplit) {
            appendUpperBoundExcluded(chunkKey, dialect, sql);
        } else if (isLastSplit) {
            appendLowerBoundIncluded(chunkKey, dialect, sql);
        } else {
            appendLowerBoundIncluded(chunkKey, dialect, sql);
            sql.append(" AND NOT (");
            appendEquality(chunkKey, dialect, sql);
            sql.append(")");
            sql.append(" AND ");
            appendUpperBoundIncluded(chunkKey, dialect, sql);
        }
        return sql.toString();
    }

    public static void bindSplitParameters(
            PreparedStatement statement, Object[] splitStart, Object[] splitEnd)
            throws SQLException {
        boolean isFirstSplit = splitStart == null;
        boolean isLastSplit = splitEnd == null;
        if (isFirstSplit && isLastSplit) {
            return;
        }

        int parameterIndex = 1;
        if (isFirstSplit) {
            bindValues(statement, splitEnd, parameterIndex);
        } else if (isLastSplit) {
            bindValues(statement, splitStart, parameterIndex);
        } else {
            parameterIndex = bindValues(statement, splitStart, parameterIndex);
            parameterIndex = bindValues(statement, splitEnd, parameterIndex);
            bindValues(statement, splitEnd, parameterIndex);
        }
    }

    private static int bindValues(PreparedStatement statement, Object[] values, int parameterIndex)
            throws SQLException {
        if (values == null) {
            return parameterIndex;
        }
        for (Object value : values) {
            statement.setObject(parameterIndex++, value);
        }
        return parameterIndex;
    }

    private static void appendLowerBoundIncluded(
            AresRowType rowType, JdbcDialect dialect, StringBuilder sql) {
        appendLexicographicBound(rowType, dialect, sql, ">=", ">");
    }

    private static void appendUpperBoundIncluded(
            AresRowType rowType, JdbcDialect dialect, StringBuilder sql) {
        appendLexicographicBound(rowType, dialect, sql, "<=", "<");
    }

    private static void appendUpperBoundExcluded(
            AresRowType rowType, JdbcDialect dialect, StringBuilder sql) {
        appendLexicographicBound(rowType, dialect, sql, "<", "<=");
    }

    private static void appendEquality(
            AresRowType rowType, JdbcDialect dialect, StringBuilder sql) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(dialect.quoteIdentifier(rowType.getFieldNames()[i])).append(" = ?");
        }
    }

    private static void appendLexicographicBound(
            AresRowType rowType,
            JdbcDialect dialect,
            StringBuilder sql,
            String lastOperator,
            String prefixOperator) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append("(");
            for (int j = 0; j < i; j++) {
                sql.append(dialect.quoteIdentifier(rowType.getFieldNames()[j])).append(" = ?");
                sql.append(" AND ");
            }
            String operator = i == rowType.getTotalFields() - 1 ? lastOperator : prefixOperator;
            sql.append(dialect.quoteIdentifier(rowType.getFieldNames()[i]))
                    .append(" ")
                    .append(operator)
                    .append(" ?");
            sql.append(")");
        }
    }
}

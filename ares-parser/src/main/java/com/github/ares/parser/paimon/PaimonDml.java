package com.github.ares.parser.paimon;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Rewrites Ares UPDATE, DELETE, and MERGE into Spark SQL for a Paimon table. */
public final class PaimonDml {
    public static final String TARGET = "__ARES_PAIMON_TARGET__";

    private PaimonDml() {}

    public static String update(SQLUpdate update) {
        if (hasJoin(update.getJoinTable(), update.getJoinSql())) {
            StringBuilder sql = new StringBuilder();
            sql.append("MERGE INTO ").append(TARGET);
            appendAlias(sql, update.getAlias());
            appendUsing(sql, update.getJoinTable(), update.getJoinSql(), update.getJoinAlias());
            sql.append(" ON (").append(requireWhere(update.getWhereSql())).append(")");
            sql.append(" WHEN MATCHED THEN UPDATE SET ");
            sql.append(assignments(update.getUpdateColumns(), update.getUpdateValues()));
            return sql.toString();
        }
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(TARGET);
        appendAlias(sql, update.getAlias());
        sql.append(" SET ");
        sql.append(assignments(update.getUpdateColumns(), update.getUpdateValues()));
        sql.append(" WHERE ").append(requireWhere(update.getWhereSql()));
        return sql.toString();
    }

    public static String delete(SQLDelete delete) {
        if (hasJoin(delete.getJoinTable(), delete.getJoinSql())) {
            StringBuilder sql = new StringBuilder();
            sql.append("MERGE INTO ").append(TARGET);
            appendAlias(sql, delete.getAlias());
            appendUsing(sql, delete.getJoinTable(), delete.getJoinSql(), delete.getJoinAlias());
            sql.append(" ON (").append(requireWhere(delete.getWhereSql())).append(")");
            sql.append(" WHEN MATCHED THEN DELETE");
            return sql.toString();
        }
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(TARGET);
        appendAlias(sql, delete.getAlias());
        sql.append(" WHERE ").append(requireWhere(delete.getWhereSql()));
        return sql.toString();
    }

    public static String merge(SQLMerge merge) {
        if (merge.getSqlUpdate() == null
                && merge.getSqlInsert() == null
                && !merge.isMatchedDelete()) {
            throw new ParseException("Paimon MERGE requires WHEN MATCHED or WHEN NOT MATCHED");
        }
        StringBuilder sql = new StringBuilder();
        sql.append("MERGE INTO ").append(TARGET);
        appendAlias(sql, merge.getAlias());
        appendUsing(sql, merge.getUsingTable(), merge.getUsingSql(), merge.getUsingAlias());
        sql.append(" ON (").append(requireWhere(merge.getOnSql())).append(")");
        if (merge.isMatchedDelete()) {
            sql.append(" WHEN MATCHED");
            appendAnd(sql, merge.getMatchedConditionSql());
            sql.append(" THEN DELETE");
        } else if (merge.getSqlUpdate() != null) {
            SQLUpdate update = merge.getSqlUpdate();
            sql.append(" WHEN MATCHED");
            appendAnd(sql, combine(merge.getMatchedConditionSql(), update.getWhereSql()));
            sql.append(" THEN UPDATE SET ");
            sql.append(assignments(update.getUpdateColumns(), update.getUpdateValues()));
        }
        if (merge.getSqlInsert() != null) {
            SQLInsert insert = merge.getSqlInsert();
            sql.append(" WHEN NOT MATCHED");
            appendAnd(sql, merge.getNotMatchedConditionSql());
            sql.append(" THEN INSERT (");
            sql.append(columns(insert.getColumns()));
            sql.append(") VALUES (");
            sql.append(String.join(", ", insert.getValuesArray().get(0)));
            sql.append(")");
        }
        return sql.toString();
    }

    private static boolean hasJoin(String table, String query) {
        return StringUtils.isNotBlank(table) || StringUtils.isNotBlank(query);
    }

    private static void appendUsing(StringBuilder sql, String table, String query, String alias) {
        sql.append(" USING ");
        if (StringUtils.isNotBlank(query)) {
            sql.append("(").append(query).append(")");
        } else {
            sql.append(table);
        }
        appendAlias(sql, alias);
    }

    private static void appendAlias(StringBuilder sql, String alias) {
        if (StringUtils.isNotBlank(alias)) {
            sql.append(" ").append(alias.trim());
        }
    }

    private static void appendAnd(StringBuilder sql, String condition) {
        if (StringUtils.isNotBlank(condition)) {
            sql.append(" AND (").append(condition).append(")");
        }
    }

    private static String combine(String left, String right) {
        if (StringUtils.isBlank(left)) {
            return right;
        }
        if (StringUtils.isBlank(right)) {
            return left;
        }
        return "(" + left + ") AND (" + right + ")";
    }

    private static String assignments(List<String> columns, List<String> values) {
        if (columns == null
                || values == null
                || columns.isEmpty()
                || columns.size() != values.size()) {
            throw new ParseException("Paimon UPDATE requires assignment expressions");
        }
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteColumn(columns.get(i))).append(" = ").append(values.get(i));
        }
        return sql.toString();
    }

    private static String columns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new ParseException("Paimon MERGE INSERT requires column names");
        }
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteColumn(columns.get(i)));
        }
        return sql.toString();
    }

    private static String quoteColumn(String column) {
        String name = column == null ? "" : column.trim();
        if (name.length() >= 2 && name.charAt(0) == '`' && name.charAt(name.length() - 1) == '`') {
            return name;
        }
        int dot = name.lastIndexOf('.');
        if (dot >= 0 && dot < name.length() - 1) {
            name = name.substring(dot + 1).trim();
        }
        return "`" + name.replace("`", "``") + "`";
    }

    private static String requireWhere(String whereSql) {
        if (StringUtils.isBlank(whereSql)) {
            throw new ParseException("Paimon DML requires a condition");
        }
        return whereSql;
    }
}

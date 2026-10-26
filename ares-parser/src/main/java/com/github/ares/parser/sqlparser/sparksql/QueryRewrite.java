package com.github.ares.parser.sqlparser.sparksql;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;

/** Keeps Spark SQL {@code WITH} clauses when a statement is rewritten for execution. */
final class QueryRewrite {
    private static final String CTE_WRAPPER_ALIAS = "__ares_cte";

    private QueryRewrite() {}

    static SqlBaseParser.DmlStatementContext requireDml(SqlBaseParser parser, String sql) {
        SqlBaseParser.StatementContext statement = parser.statement();
        if (!(statement instanceof SqlBaseParser.DmlStatementContext)) {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
        }
        SqlBaseParser.DmlStatementContext dml = (SqlBaseParser.DmlStatementContext) statement;
        if (dml.dmlStatementNoWith() == null) {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
        }
        return dml;
    }

    static String cteText(SqlBaseParser.CtesContext ctes) {
        if (ctes == null) {
            return null;
        }
        String cteSql = getFullText(ctes);
        if (cteSql == null || cteSql.trim().isEmpty()) {
            return null;
        }
        return cteSql.trim();
    }

    static String prependCtes(SqlBaseParser.CtesContext ctes, String sql) {
        String cteSql = cteText(ctes);
        if (cteSql == null) {
            return sql;
        }
        if (sql == null || sql.trim().isEmpty()) {
            return cteSql;
        }
        String body = sql.trim();
        if (startsWithWith(body)) {
            return cteSql + " SELECT * FROM (" + body + ") " + CTE_WRAPPER_ALIAS;
        }
        return cteSql + " " + body;
    }

    static String appendOrganization(String sourceSql, SqlBaseParser.QueryContext queryContext) {
        if (sourceSql == null) {
            sourceSql = "";
        }
        if (queryContext == null
                || queryContext.queryOrganization() == null
                || queryContext.queryOrganization().getChildCount() == 0) {
            return sourceSql;
        }
        String organizationSql = getFullText(queryContext.queryOrganization());
        if (organizationSql == null || organizationSql.trim().isEmpty()) {
            return sourceSql;
        }
        return sourceSql.trim() + " " + organizationSql.trim();
    }

    private static boolean startsWithWith(String sql) {
        return sql.length() >= 4
                && sql.regionMatches(true, 0, "WITH", 0, 4)
                && (sql.length() == 4 || Character.isWhitespace(sql.charAt(4)));
    }
}

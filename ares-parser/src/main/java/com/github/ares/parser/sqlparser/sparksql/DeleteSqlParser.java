package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLDelete;
import com.github.ares.parser.sqlparser.model.SQLHint;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.SQL_SELECT_PREFIX;
import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;

public class DeleteSqlParser {
    private DeleteSqlParser() {
    }

    /**
     * Parse delete SQL and return SQLDelete object.
     *
     * @param sql delete SQL
     * @return sqlDelete object
     */
    public static SQLDelete parseDelete(String sql) {
        SQLDelete sqlDelete = new SQLDelete();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.DeleteFromTableContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }

            SqlBaseParser.DeleteFromTableContext deleteFromTableContext = (SqlBaseParser.DeleteFromTableContext) dmlStatementNoWithContext;

            SqlBaseParser.MultipartIdentifierContext mappingTable = deleteFromTableContext.multipartIdentifier(0);
            sqlDelete.setTable(mappingTable.getText());

            if (deleteFromTableContext.source != null || deleteFromTableContext.sourceQuery != null) {
                parseSourceQuery(deleteFromTableContext, sqlDelete, sql);
            } else if (!deleteFromTableContext.tableAlias().isEmpty()) {
                sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
            }

            if (deleteFromTableContext.whereClause() == null) {
                throw new ParseException("delete SQL must have WHERE clause: " + sql);
            }
            CriteriaClause criteriaClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext expressionContext = deleteFromTableContext.whereClause().booleanExpression();
            CriteriaParser.parseWhereClause(expressionContext, criteriaClause, sqlDelete.getAlias());
            sqlDelete.setWhereClause(criteriaClause);

            List<String> selectItems = new ArrayList<>();
            CommonParser.visitCriteriaClause(criteriaClause, selectItems);

            StringBuilder selectSql = new StringBuilder();
            selectSql.append(SQL_SELECT_PREFIX);
            selectSql.append(String.join(", ", selectItems));
            if (StringUtils.isNotBlank(sqlDelete.getJoinTable())) {
                selectSql.append(" FROM ").append(sqlDelete.getJoinTable()).append(" ").append(sqlDelete.getJoinAlias());
            } else if (StringUtils.isNotBlank(sqlDelete.getJoinSql())) {
                selectSql.append(" FROM (").append(sqlDelete.getJoinSql()).append(") ").append(sqlDelete.getJoinAlias());
            }
            sqlDelete.setSourceSql(selectSql.toString());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlDelete;
    }

    private static void parseSourceQuery(SqlBaseParser.DeleteFromTableContext deleteFromTableContext, SQLDelete sqlDelete, String sql) {
        if (deleteFromTableContext.tableAlias().size() != 2) {
            throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
        }
        if (deleteFromTableContext.source != null) {
            sqlDelete.setJoinTable(deleteFromTableContext.source.getText());
            String sourceTableAlias = deleteFromTableContext.tableAlias().get(1).getText();
            sqlDelete.setJoinAlias(sourceTableAlias);
            sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
        } else if (deleteFromTableContext.sourceQuery != null) {
            if (!(deleteFromTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) deleteFromTableContext.sourceQuery.queryTerm()).queryPrimary();
            if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            String sourceTableAlias = deleteFromTableContext.tableAlias().get(1).getText();
            sqlDelete.setJoinAlias(sourceTableAlias);
            sqlDelete.setAlias(deleteFromTableContext.tableAlias().get(0).getText());
            Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                    (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
            sqlDelete.setHints(hintsWithSql.getLeft());
            sqlDelete.setJoinSql(hintsWithSql.getRight());
        }
    }
}

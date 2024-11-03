package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.SQL_SELECT_PREFIX;
import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class UpdateSqlParser {
    private UpdateSqlParser() {
    }

    /**
     * Parse update SQL and return SQLUpdate object.
     *
     * @param sql update SQL
     * @return SQLUpdate object
     */
    public static SQLUpdate parseUpdate(String sql) {
        SQLUpdate sqlUpdate = new SQLUpdate();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.UpdateTableContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }

            SqlBaseParser.UpdateTableContext updateTableContext = (SqlBaseParser.UpdateTableContext) dmlStatementNoWithContext;

            SqlBaseParser.MultipartIdentifierContext mappingTable = updateTableContext.multipartIdentifier(0);
            sqlUpdate.setTable(mappingTable.getText());

            if (updateTableContext.source != null || updateTableContext.sourceQuery != null) {
                parseSourceQuery(updateTableContext, sqlUpdate, sql);
            } else if (!updateTableContext.tableAlias().isEmpty()) {
                sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
            }

            List<SqlBaseParser.AssignmentContext> assignmentContexts = updateTableContext.setClause().assignmentList().assignment();
            for (SqlBaseParser.AssignmentContext assignmentContext : assignmentContexts) {
                List<SqlBaseParser.ErrorCapturingIdentifierContext> identifierContexts = assignmentContext.multipartIdentifier().errorCapturingIdentifier();
                if (StringUtils.isBlank(sqlUpdate.getAlias()) && identifierContexts.size() == 2
                        && identifierContexts.get(0).getText().equalsIgnoreCase(sqlUpdate.getAlias())) {
                    throw new ParseException("column owner must be same as table alias in update statement: " + sql);
                }
                String targetCol = assignmentContext.multipartIdentifier().errorCapturingIdentifier.getText();
                String sourceExpression = getFullText(assignmentContext.expression());
                sqlUpdate.getUpdateColumns().add(targetCol);
                sqlUpdate.getUpdateValues().add(sourceExpression);
            }
            if (updateTableContext.whereClause() == null) {
                throw new ParseException("update SQL must have WHERE clause: " + sql);
            }
            CriteriaClause criteriaClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext expressionContext = updateTableContext.whereClause().booleanExpression();
            CriteriaParser.parseWhereClause(expressionContext, criteriaClause, sqlUpdate.getAlias());
            sqlUpdate.setWhereClause(criteriaClause);

            List<String> selectItems = new ArrayList<>();
            CommonParser.visitCriteriaClause(criteriaClause, selectItems);

            StringBuilder selectSql = new StringBuilder();
            selectSql.append(SQL_SELECT_PREFIX);
            selectSql.append(String.join(", ", sqlUpdate.getUpdateValues()));
            selectSql.append(", ").append(String.join(", ", selectItems));
            if (StringUtils.isNotBlank(sqlUpdate.getJoinTable())) {
                selectSql.append(" FROM ").append(sqlUpdate.getJoinTable()).append(" ").append(sqlUpdate.getJoinAlias());
            } else if (StringUtils.isNotBlank(sqlUpdate.getJoinSql())) {
                selectSql.append(" FROM (").append(sqlUpdate.getJoinSql()).append(") ").append(sqlUpdate.getJoinAlias());
            }
            sqlUpdate.setSourceSql(selectSql.toString());
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlUpdate;
    }

    private static void parseSourceQuery(SqlBaseParser.UpdateTableContext updateTableContext, SQLUpdate sqlUpdate, String sql) {
        if (updateTableContext.tableAlias().size() != 2) {
            throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
        }
        if (updateTableContext.source != null) {
            sqlUpdate.setJoinTable(updateTableContext.source.getText());
            String sourceTableAlias = updateTableContext.tableAlias().get(1).getText();
            sqlUpdate.setJoinAlias(sourceTableAlias);
            sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
        } else if (updateTableContext.sourceQuery != null) {
            if (!(updateTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) updateTableContext.sourceQuery.queryTerm()).queryPrimary();
            if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            String sourceTableAlias = updateTableContext.tableAlias().get(1).getText();
            sqlUpdate.setJoinAlias(sourceTableAlias);
            sqlUpdate.setAlias(updateTableContext.tableAlias().get(0).getText());
            Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                    (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
            sqlUpdate.setHints(hintsWithSql.getLeft());
            sqlUpdate.setJoinSql(hintsWithSql.getRight());
        }
    }
}

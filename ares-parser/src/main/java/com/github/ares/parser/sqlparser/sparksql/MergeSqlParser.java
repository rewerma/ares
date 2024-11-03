package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import com.github.ares.parser.sqlparser.model.SQLMerge;
import com.github.ares.parser.sqlparser.model.SQLUpdate;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.SQL_SELECT_PREFIX;
import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.sqlparser.sparksql.CriteriaParser.parseWhereClause;
import static com.github.ares.parser.sqlparser.sparksql.CriteriaParser.visitOnWhereClause;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class MergeSqlParser {
    private MergeSqlParser() {
    }

    /**
     * Parse merge into SQL and return SQLMerge object.
     *
     * @param sql merge into SQL
     * @return SQLMerge object
     */
    public static SQLMerge parseMerge(String sql) {
        SQLMerge sqlMerge = new SQLMerge();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.MergeIntoTableContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            getFullText(dmlStatementNoWithContext);

            SqlBaseParser.MergeIntoTableContext mergeIntoTableContext = (SqlBaseParser.MergeIntoTableContext) dmlStatementNoWithContext;
            SqlBaseParser.MultipartIdentifierContext mappingTable = mergeIntoTableContext.multipartIdentifier(0);
            sqlMerge.setTable(mappingTable.getText());

            if (mergeIntoTableContext.source != null || mergeIntoTableContext.sourceQuery != null) {
                parseSourceQuery(mergeIntoTableContext, sqlMerge, sql);
            } else if (!mergeIntoTableContext.tableAlias().isEmpty()) {
                sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
            }

            CriteriaClause onClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext onExpressionContext = mergeIntoTableContext.mergeCondition;
            CriteriaParser.parseWhereClause(onExpressionContext, onClause, sqlMerge.getAlias());
            List<String> onSelectItems = new ArrayList<>();
            CommonParser.visitCriteriaClause(onClause, onSelectItems);
            sqlMerge.setOnSelectItems(onSelectItems);

            String usingSQL;
            if (sqlMerge.getUsingTable() != null) {
                usingSQL = "SELECT * FROM " + sqlMerge.getUsingTable();
            } else {
                usingSQL = sqlMerge.getUsingSql();
            }

            StringBuilder conditionSql = new StringBuilder();
            visitOnWhereClause(onClause, conditionSql);

            if (mergeIntoTableContext.matchedClause().size() > 1 || mergeIntoTableContext.notMatchedClause().size() > 1) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            if (!mergeIntoTableContext.notMatchedClause().isEmpty()) {
                parseNotMatchedClause(mergeIntoTableContext, sqlMerge, usingSQL, conditionSql.toString());
            }

            if (!mergeIntoTableContext.matchedClause().isEmpty()) {
                parseMatchedClause(mergeIntoTableContext, sqlMerge, usingSQL, onClause, conditionSql.toString());
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlMerge;
    }

    private static void parseSourceQuery(SqlBaseParser.MergeIntoTableContext mergeIntoTableContext, SQLMerge sqlMerge, String sql) {
        if (mergeIntoTableContext.tableAlias().size() != 2) {
            throw new ParseException(String.format("Alias not defined for source table or target table: %s", sql));
        }
        if (mergeIntoTableContext.source != null) {
            sqlMerge.setUsingTable(mergeIntoTableContext.source.getText());
            String sourceTableAlias = mergeIntoTableContext.tableAlias().get(1).getText();
            sqlMerge.setUsingAlias(sourceTableAlias);
            sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
        } else if (mergeIntoTableContext.sourceQuery != null) {
            if (!(mergeIntoTableContext.sourceQuery.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) mergeIntoTableContext.sourceQuery.queryTerm()).queryPrimary();
            if (!(queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            String sourceTableAlias = mergeIntoTableContext.tableAlias().get(1).getText();
            sqlMerge.setUsingAlias(sourceTableAlias);
            sqlMerge.setAlias(mergeIntoTableContext.tableAlias().get(0).getText());
            Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                    (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
            sqlMerge.setHints(hintsWithSql.getLeft());
            sqlMerge.setUsingSql(hintsWithSql.getRight());
        }
    }

    private static void parseNotMatchedClause(SqlBaseParser.MergeIntoTableContext mergeIntoTableContext, SQLMerge sqlMerge,
                                              String usingSQL, String conditionSql) {
        SQLInsert sqlInsert = new SQLInsert();
        sqlInsert.setTable(sqlMerge.getTable());

        SqlBaseParser.NotMatchedActionContext notMatchedAction = mergeIntoTableContext.notMatchedClause().get(0).notMatchedAction();

        for (SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext : notMatchedAction.multipartIdentifierList().multipartIdentifier()) {
            sqlInsert.getColumns().add(multipartIdentifierContext.errorCapturingIdentifier.identifier().getText());
        }

        List<List<String>> valuesArray = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (SqlBaseParser.ExpressionContext expressionContext : notMatchedAction.expression()) {
            values.add(getFullText(expressionContext));
        }
        valuesArray.add(values);
        sqlInsert.setValuesArray(valuesArray);
        sqlMerge.setSqlInsert(sqlInsert);

        StringBuilder sourceSql = new StringBuilder();
        sourceSql.append(SQL_SELECT_PREFIX);
        sourceSql.append(String.join(", ", sqlInsert.getValuesArray().get(0)));
        sourceSql.append(" FROM (");
        sourceSql.append(usingSQL).append(") ").append(sqlMerge.getUsingAlias()).append(" ");
        sourceSql.append("WHERE NOT EXISTS (SELECT 1 FROM ");
        sourceSql.append(sqlMerge.getTable());
        sourceSql.append(" WHERE ").append(conditionSql).append(" )");
        sqlInsert.setSourceSql(sourceSql.toString());
    }

    private static void parseMatchedClause(SqlBaseParser.MergeIntoTableContext mergeIntoTableContext, SQLMerge sqlMerge,
                                           String usingSQL, CriteriaClause onClause, String conditionSql) {

        SQLUpdate sqlUpdate = new SQLUpdate();
        sqlUpdate.setTable(sqlMerge.getTable());

        sqlUpdate.setAlias(sqlMerge.getAlias());
        sqlUpdate.setJoinAlias(sqlMerge.getUsingAlias());
        if (sqlMerge.getUsingTable() != null) {
            sqlUpdate.setJoinTable(sqlMerge.getUsingTable());
        } else if (sqlMerge.getUsingSql() != null) {
            sqlUpdate.setJoinSql(sqlMerge.getUsingSql());
        }

        SqlBaseParser.MatchedActionContext matchedActionContext = mergeIntoTableContext.matchedClause().get(0).matchedAction();

        for (SqlBaseParser.AssignmentContext assignmentContext : matchedActionContext.assignmentList().assignment()) {
            sqlUpdate.getUpdateColumns().add(assignmentContext.key.errorCapturingIdentifier.identifier().getText());
            sqlUpdate.getUpdateValues().add(getFullText(assignmentContext.value));
        }

        if (matchedActionContext.booleanExpression() != null) {
            CriteriaClause whereClause = new CriteriaClause();
            parseWhereClause(matchedActionContext.booleanExpression(), whereClause, sqlUpdate.getAlias());

            sqlUpdate.setWhereClause(whereClause);
            List<String> selectItems = new ArrayList<>();
            CommonParser.visitCriteriaClause(whereClause, selectItems);
            sqlUpdate.setSelectWhereItems(selectItems);

            CriteriaClause allWhereClause = new CriteriaClause();
            allWhereClause.setLeftCriteria(onClause);
            allWhereClause.setOperator("AND");
            allWhereClause.setRightCriteria(whereClause);
            sqlMerge.setAllWhereClause(allWhereClause);
        }
        sqlMerge.setSqlUpdate(sqlUpdate);

        StringBuilder sourceSql = new StringBuilder();
        sourceSql.append(SQL_SELECT_PREFIX);
        sourceSql.append(String.join(", ", sqlUpdate.getUpdateValues()));
        sourceSql.append(", ").append(String.join(", ", sqlMerge.getOnSelectItems()));
        if (sqlUpdate.getSelectWhereItems() != null) {
            sourceSql.append(", ").append(String.join(", ", sqlUpdate.getSelectWhereItems()));
        }
        sourceSql.append(" FROM (");
        sourceSql.append(usingSQL).append(") ").append(sqlMerge.getUsingAlias()).append(" ");
        sourceSql.append("WHERE EXISTS (SELECT 1 FROM ");
        sourceSql.append(sqlMerge.getTable());
        sourceSql.append(" WHERE ").append(conditionSql).append(" )");

        sqlUpdate.setSourceSql(sourceSql.toString());
    }
}

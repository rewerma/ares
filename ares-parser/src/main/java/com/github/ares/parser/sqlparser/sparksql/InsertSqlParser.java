package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTree;
import com.github.ares.org.antlr.v4.runtime.tree.TerminalNodeImpl;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLInsert;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.SQL_SELECT_PREFIX;
import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class InsertSqlParser {
    private static final String SQL_INTO = "INTO";

    private InsertSqlParser() {
    }

    /**
     * Parse insert sql and return SQLInsert object.
     *
     * @param sql insert sql
     * @return SQLInsert object
     */
    public static SQLInsert parseInsert(String sql) {
        SQLInsert sqlInsert = new SQLInsert();

        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {

            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.DmlStatementNoWithContext dmlStatementNoWithContext = parser.dmlStatementNoWith();

            if (!(dmlStatementNoWithContext instanceof SqlBaseParser.SingleInsertQueryContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.SingleInsertQueryContext singleInsertQueryContext = (SqlBaseParser.SingleInsertQueryContext) dmlStatementNoWithContext;
            SqlBaseParser.InsertIntoTableContext insertIntoContext = (SqlBaseParser.InsertIntoTableContext) singleInsertQueryContext.insertInto();
            if (insertIntoContext.getChildCount() < 3 ||
                    !(insertIntoContext.getChild(1) instanceof TerminalNodeImpl) ||
                    !SQL_INTO.equalsIgnoreCase(insertIntoContext.getChild(1).getText()) ||
                    !(insertIntoContext.getChild(2) instanceof SqlBaseParser.MultipartIdentifierContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.MultipartIdentifierContext multipartIdentifierContext = insertIntoContext.multipartIdentifier();
            sqlInsert.setTable(multipartIdentifierContext.getText());
            if (insertIntoContext.identifierList() != null) {
                SqlBaseParser.IdentifierListContext identifierListContext = insertIntoContext.identifierList();
                if (identifierListContext.identifierSeq() == null) {
                    throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
                }

                SqlBaseParser.IdentifierSeqContext identifierSeqContext = identifierListContext.identifierSeq();
                for (SqlBaseParser.ErrorCapturingIdentifierContext errorCapturingIdentifierContext : identifierSeqContext.errorCapturingIdentifier()) {
                    sqlInsert.getColumns().add(errorCapturingIdentifierContext.getText());
                }
            }

            String selectSql;
            SqlBaseParser.QueryContext queryContext = singleInsertQueryContext.query();
            SqlBaseParser.QueryPrimaryContext queryPrimaryContext = ((SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm()).queryPrimary();
            if (queryPrimaryContext instanceof SqlBaseParser.InlineTableDefault1Context) {
                selectSql = parseInlineTableContext(queryContext, sql, sqlInsert);
            } else if (queryPrimaryContext instanceof SqlBaseParser.QueryPrimaryDefaultContext) {
                Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                        (SqlBaseParser.QueryPrimaryDefaultContext) queryPrimaryContext);
                sqlInsert.setHints(hintsWithSql.getLeft());
                selectSql = hintsWithSql.getRight();
            } else {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            sqlInsert.setSourceSql(selectSql);
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlInsert;
    }

    /**
     * Parse inline table context.
     *
     * @param queryContext query context object
     * @param sql          sql string
     * @param sqlInsert    SQLInsert object
     * @return select sql string
     */
    private static String parseInlineTableContext(SqlBaseParser.QueryContext queryContext, String sql, SQLInsert sqlInsert) {
        List<SqlBaseParser.ExpressionContext> expressionContexts = ((SqlBaseParser.InlineTableDefault1Context)
                ((SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm()).queryPrimary()).inlineTable().expression();
        List<String> valuesExpressions = new ArrayList<>();
        List<List<String>> valuesArray = new ArrayList<>();
        for (SqlBaseParser.ExpressionContext expressionContext : expressionContexts) {
            if (expressionContext.getChildCount() < 1 || !(expressionContext.getChild(0) instanceof SqlBaseParser.PredicatedContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) expressionContext.getChild(0);
            if (predicatedContext.valueExpression().getChildCount() < 1 || !(predicatedContext.valueExpression().getChild(0) instanceof SqlBaseParser.RowConstructorContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            List<String> values = new ArrayList<>();
            SqlBaseParser.RowConstructorContext rowConstructorContext = (SqlBaseParser.RowConstructorContext) predicatedContext.valueExpression().getChild(0);
            for (int i = 0; i < rowConstructorContext.getChildCount(); i++) {
                ParseTree item = rowConstructorContext.getChild(i);
                if (item instanceof TerminalNodeImpl) {
                    continue;
                }
                values.add(getFullText(item));
            }
            valuesArray.add(values);
            sqlInsert.setValuesArray(valuesArray);

            String selectExpression = getFullText(expressionContext);
            if (!selectExpression.startsWith("(") && !selectExpression.endsWith(")")) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            selectExpression = selectExpression.substring(0, selectExpression.length() - 1).substring(1);
            valuesExpressions.add(selectExpression);
        }
        StringJoiner stringJoiner = new StringJoiner(" UNION ALL ");
        valuesExpressions.forEach(valuesExpression -> stringJoiner.add(SQL_SELECT_PREFIX + valuesExpression));

        return stringJoiner.toString();
    }
}

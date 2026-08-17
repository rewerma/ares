package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLHint;
import com.github.ares.parser.sqlparser.model.SQLSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.utils.PLParserUtil.clearParam;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class SelectSqlParser {
    private SelectSqlParser() {
    }

    public static SQLSelect parseSelect(String sql) {
        SQLSelect sqlSelect = new SQLSelect();
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.QueryContext queryContext = parser.query();

            if (!(queryContext.queryTerm() instanceof SqlBaseParser.QueryTermDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.QueryTermDefaultContext queryTermDefaultContext = (SqlBaseParser.QueryTermDefaultContext) queryContext.queryTerm();
            if (!(queryTermDefaultContext.queryPrimary() instanceof SqlBaseParser.QueryPrimaryDefaultContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.QueryPrimaryDefaultContext queryPrimaryDefaultContext = (SqlBaseParser.QueryPrimaryDefaultContext) queryTermDefaultContext.queryPrimary();
            if (!(queryPrimaryDefaultContext.querySpecification() instanceof SqlBaseParser.RegularQuerySpecificationContext)) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
            }
            SqlBaseParser.RegularQuerySpecificationContext regularQuerySpecificationContext = (SqlBaseParser.RegularQuerySpecificationContext) queryPrimaryDefaultContext.querySpecification();
            SqlBaseParser.IntoClauseContext intoClauseContext = regularQuerySpecificationContext.selectClause().intoClause();
            if (intoClauseContext != null) {
                List<String> intoParams = new ArrayList<>();
                intoClauseContext.expression().forEach(expressionContext -> intoParams.add(expressionContext.getText()));

                sqlSelect.setIntoParams(new ArrayList<>());
                for (String intoParam : intoParams) {
                    intoParam = clearParam(intoParam);
                    sqlSelect.getIntoParams().add(intoParam);
                }
            }

            Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                    queryPrimaryDefaultContext);
            sqlSelect.setHints(hintsWithSql.getLeft());
            sqlSelect.setSourceSql(appendQueryOrganization(hintsWithSql.getRight(), queryContext));
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlSelect;
    }

    public static boolean hasOuterLimit(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        try (InputStream in = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))) {
            SqlBaseParser parser = CommonParser.parseSql(in);
            SqlBaseParser.QueryContext queryContext = parser.query();
            return queryContext != null
                    && queryContext.queryOrganization() != null
                    && queryContext.queryOrganization().LIMIT() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private static String appendQueryOrganization(String sourceSql, SqlBaseParser.QueryContext queryContext) {
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
}

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
            if (intoClauseContext == null) {
                Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                        queryPrimaryDefaultContext);
                sqlSelect.setHints(hintsWithSql.getLeft());
                sqlSelect.setSourceSql(hintsWithSql.getRight());
            } else {
                List<String> intoParams = new ArrayList<>();
                intoClauseContext.expression().forEach(expressionContext -> intoParams.add(expressionContext.getText()));

                sqlSelect.setIntoParams(new ArrayList<>());
                for (String intoParam : intoParams) {
                    intoParam = clearParam(intoParam);
                    sqlSelect.getIntoParams().add(intoParam);
                }

                Pair<List<SQLHint>, String> hintsWithSql = HintParser.parseSelectHints(sql,
                        queryPrimaryDefaultContext);
                sqlSelect.setHints(hintsWithSql.getLeft());
                sqlSelect.setSourceSql(hintsWithSql.getRight());
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), e);
        }
        return sqlSelect;
    }
}

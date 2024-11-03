package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.org.antlr.v4.runtime.tree.ParseTree;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;
import com.github.ares.parser.sqlparser.model.SQLHint;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG_WITH_PARAM;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class HintParser {
    private static final String INNER_HINT_MAPJOIN = "mapjoin";
    private static final String INNER_HINT_BROADCAST = "broadcast";

    private HintParser() {
    }

    public static Pair<List<SQLHint>, String> parseSelectHints(String sql, SqlBaseParser.QueryPrimaryDefaultContext queryPrimaryDefaultContext) {
        if (!(queryPrimaryDefaultContext.querySpecification() instanceof SqlBaseParser.RegularQuerySpecificationContext)) {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
        }
        SqlBaseParser.RegularQuerySpecificationContext regularQuerySpecificationContext = (SqlBaseParser.RegularQuerySpecificationContext) queryPrimaryDefaultContext.querySpecification();
        if (regularQuerySpecificationContext.selectClause() == null) {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG_WITH_PARAM, sql));
        }

        List<SQLHint> sqlHints = new ArrayList<>();
        SqlBaseParser.SelectClauseContext selectClauseContext = regularQuerySpecificationContext.selectClause();
        if (!selectClauseContext.hints.isEmpty()) {
            for (int i = 0; i < selectClauseContext.hints.size(); i++) {
                SQLHint sqlHint = new SQLHint();
                SqlBaseParser.HintContext hintContext = selectClauseContext.hint(i);
                String hintName = hintContext.hintStatement.hintName.getText();
                sqlHint.setHintName(hintName);
                for (SqlBaseParser.PrimaryExpressionContext primaryExpressionContext : hintContext.hintStatement.parameters) {
                    String parameter = getFullText(primaryExpressionContext);
                    if (StringUtils.isNotBlank(parameter)) {
                        sqlHint.getArguments().add(parameter);
                    }
                }
                sqlHints.add(sqlHint);
            }
        }

        // filter out hints and into clause from select clause
        StringBuilder selectSql = new StringBuilder();
        for (ParseTree child : regularQuerySpecificationContext.children) {
            if (child instanceof SqlBaseParser.SelectClauseContext) {
                filterHints((SqlBaseParser.SelectClauseContext) child, selectSql);
            } else {
                selectSql.append(getFullText(child)).append(" ");
            }
        }

        return Pair.of(sqlHints, selectSql.toString());
    }

    private static void filterHints(SqlBaseParser.SelectClauseContext selectClauseContext, StringBuilder selectSql) {
        for (ParseTree grandChild : selectClauseContext.children) {
            if (grandChild instanceof SqlBaseParser.IntoClauseContext || grandChild instanceof SqlBaseParser.HintContext) {
                if ((grandChild instanceof SqlBaseParser.HintContext) && ExecutionEngineType.engineType == EngineType.SPARK) {
                    String hint = getFullText(grandChild);
                    String hintLower = hint.toLowerCase();
                    if (hintLower.contains(INNER_HINT_MAPJOIN) || hintLower.contains(INNER_HINT_BROADCAST)) {
                        selectSql.append(hint).append(" ");
                    }
                }
                continue;
            }
            selectSql.append(getFullText(grandChild)).append(" ");
        }
    }
}

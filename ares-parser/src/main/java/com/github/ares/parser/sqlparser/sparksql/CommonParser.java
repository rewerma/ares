package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.org.antlr.v4.runtime.CharStream;
import com.github.ares.org.antlr.v4.runtime.CharStreams;
import com.github.ares.org.antlr.v4.runtime.CommonTokenStream;
import com.github.ares.parser.antlr4.CaseChangingCharStream;
import com.github.ares.parser.antlr4.CustomErrorListener;
import com.github.ares.parser.antlr4.sparksql.SqlBaseLexer;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CommonParser {

    public static final String UNSUPPORTED_EXP_MSG = "unsupported syntax: ";
    public static final String UNSUPPORTED_EXP_MSG_WITH_PARAM = UNSUPPORTED_EXP_MSG + " %s";

    public static final String SQL_SELECT_PREFIX = "SELECT ";

    public static SqlBaseParser parseSql(InputStream in) throws IOException {
        CharStream s = CharStreams.fromStream(in);
        CaseChangingCharStream upper = new CaseChangingCharStream(s, true);

        CustomErrorListener lexerErrorListener = new CustomErrorListener();
        SqlBaseLexer lexer = new SqlBaseLexer(upper);
        lexer.removeErrorListeners();
        lexer.addErrorListener(lexerErrorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokens);
        CustomErrorListener parserErrorListener = new CustomErrorListener();
        parser.removeErrorListeners();
        parser.addErrorListener(parserErrorListener);
        return parser;
    }

    public static void visitCriteriaClause(CriteriaClause criteriaClause, List<String> items) {
        if ("AND".equalsIgnoreCase(criteriaClause.getOperator()) || "OR".equalsIgnoreCase(criteriaClause.getOperator())) {
            visitCriteriaClause(criteriaClause.getLeftCriteria(), items);
            visitCriteriaClause(criteriaClause.getRightCriteria(), items);
        } else {
            if ("IN".equalsIgnoreCase(criteriaClause.getOperator())) {
                if (criteriaClause.getInItems() != null) {
                    items.addAll(criteriaClause.getInItems());
                }
            } else {
                items.add(criteriaClause.getRightExpr());
            }
        }
    }
}

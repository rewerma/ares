package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Singleton;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;

import java.util.List;

@Singleton
public class PlDataTypePrecisionVisitor {
    public Tuple2<Integer, Integer> visit(PlSqlParser.DatatypeContext datatypeContext) {
        Integer precision = null;
        Integer scale = null;
        if (datatypeContext.precision_part() != null &&
                datatypeContext.precision_part().numeric() != null) {
            List<PlSqlParser.NumericContext> numericContexts = datatypeContext.precision_part().numeric();
            if (numericContexts.size() >= 2) {
                precision = Integer.parseInt(numericContexts.get(0).getText());
                scale = Integer.parseInt(numericContexts.get(1).getText());
            } else if (numericContexts.size() == 1) {
                precision = Integer.parseInt(numericContexts.get(0).getText());
            }
        }
        return Tuple2.of(precision, scale);
    }
}

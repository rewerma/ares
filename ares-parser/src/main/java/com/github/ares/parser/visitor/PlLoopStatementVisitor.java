package com.github.ares.parser.visitor;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalWhileLoop;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.ares.common.utils.StringUtils.substring;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class PlLoopStatementVisitor {

    private PlVisitorManager visitorManager;

    public void init(PlVisitorManager visitorManager) {
        this.visitorManager = visitorManager;
    }

    public LogicalOperation loopVisitor(PlBodyVisitor plBodyVisitor, PlSqlParser.Loop_statementContext loop_statementContext,
                                        List<LogicalOperation> baseBody, Map<String, PlType> allParams, List<String> structs) {
        return loopVisitor(plBodyVisitor, loop_statementContext, baseBody, allParams, structs, true);
    }

    public LogicalOperation loopVisitor(PlBodyVisitor plBodyVisitor, PlSqlParser.Loop_statementContext loop_statementContext,
                                        List<LogicalOperation> baseBody, Map<String, PlType> allParams, List<String> structs, boolean withCursor) {
        if (loop_statementContext.FOR() != null) {
            PlSqlParser.Cursor_loop_paramContext loop_paramContext = loop_statementContext.cursor_loop_param();
            if (loop_paramContext.IN() != null && loop_paramContext.DOUBLE_PERIOD() != null) {
                String indexParam = loop_paramContext.index_name().getText();

                LogicalExpression lowerExpression = visitorManager.getExpressionVisitor().visitExpressionContext(loop_paramContext.lower_bound().concatenation(), allParams, structs);
                LogicalExpression upperExpression = visitorManager.getExpressionVisitor().visitExpressionContext(loop_paramContext.upper_bound().concatenation(), allParams, structs);

                Map<String, PlType> allParamsTmp = new LinkedHashMap<>(allParams);
                allParamsTmp.put(indexParam, PlType.of(InternalFieldType.INT));

                LogicalForLoop forLoop = new LogicalForLoop();
                forLoop.setIndexName(indexParam);
                forLoop.setLowerExpr(lowerExpression);
                forLoop.setUpperExpr(upperExpression);
                forLoop.setForBody(plBodyVisitor.visitBodyStatements(loop_statementContext.seq_of_statements(), new LinkedHashMap<>(),
                        new LinkedHashMap<>(), allParamsTmp, baseBody, structs));
                return forLoop;
            } else if (loop_paramContext.IN() != null && loop_paramContext.record_name() != null && loop_paramContext.select_statement() != null) {
                if (!withCursor) {
                    throw new ParseException("Cursor loop is not supported in function body");
                }
                String cursorName = loop_paramContext.record_name().getText();
                String selectSQL = PLParserUtil.getFullSQLWithParams(loop_paramContext.select_statement(), allParams, structs);
                if (structs == null) {
                    structs = new ArrayList<>();
                } else {
                    structs = new ArrayList<>(structs);
                }
                structs.add(cursorName);
                LogicalForCursorLoop forCursorLoop = new LogicalForCursorLoop();
                forCursorLoop.setCursorName(cursorName);
                forCursorLoop.setSelectSQL(selectSQL);
                forCursorLoop.setForBody(plBodyVisitor.visitBodyStatements(loop_statementContext.seq_of_statements(), new LinkedHashMap<>(),
                        new LinkedHashMap<>(), allParams, baseBody, structs));
                return forCursorLoop;
            } else {
                throw new ParseException(String.format("Unsupported syntax: %s", substring(getFullText(loop_statementContext), 0, 100)));
            }
        }
        if (loop_statementContext.WHILE() != null) {
            LogicalExpression conditionExpr = visitorManager.getExpressionVisitor().visitExpressionContext(
                    loop_statementContext.condition().expression(), allParams, structs);
            LogicalWhileLoop whileLoop = new LogicalWhileLoop();
            whileLoop.setCondition(conditionExpr);
            whileLoop.setWhileBody(plBodyVisitor.visitBodyStatements(loop_statementContext.seq_of_statements(), new LinkedHashMap<>(),
                    new LinkedHashMap<>(), allParams, baseBody, structs));
            return whileLoop;
        } else {
            throw new ParseException(String.format("Unsupported syntax: %s", substring(getFullText(loop_statementContext), 0, 100)));
        }
    }
}

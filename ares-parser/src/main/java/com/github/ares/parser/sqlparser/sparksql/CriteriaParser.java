package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.github.ares.parser.sqlparser.sparksql.CommonParser.UNSUPPORTED_EXP_MSG;
import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class CriteriaParser {

    private static final String UNSUPPORTED_EXP_MSG = "unsupported syntax: %s in WHERE clause";

    private static final String OP_AND = "AND";
    private static final String OP_OR = "OR";
    private static final String OP_EQ = "=";
    private static final String OP_NE = "!=";
    private static final String OP_NE2 = "<>";
    private static final String OP_GT = ">";
    private static final String OP_LT = "<";
    private static final String OP_GE = ">=";
    private static final String OP_LE = "<=";
    private static final String OP_IN = "IN";
    private static final String OP_LIKE = "LIKE";
    private static final String OP_NOT_LIKE = "NOT LIKE";

    private CriteriaParser() {
    }

    /**
     * parse where clause
     *
     * @param booleanExpressionContext boolean expression context
     * @param criteriaClause           criteria clause for result
     * @param targetTableAlias         target table alias
     */
    public static void parseWhereClause(SqlBaseParser.BooleanExpressionContext booleanExpressionContext, CriteriaClause criteriaClause, String targetTableAlias) {
        if (booleanExpressionContext instanceof SqlBaseParser.LogicalBinaryContext) {
            parseLogicalBinaryContext((SqlBaseParser.LogicalBinaryContext) booleanExpressionContext, criteriaClause, targetTableAlias);
        } else if (booleanExpressionContext instanceof SqlBaseParser.PredicatedContext) {
            SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) booleanExpressionContext;
            SqlBaseParser.PrimaryExpressionContext primaryExpressionContext;
            if (predicatedContext.valueExpression() instanceof SqlBaseParser.ComparisonContext) {
                primaryExpressionContext = parseComparisonContext((SqlBaseParser.ComparisonContext) predicatedContext.valueExpression(), criteriaClause);
            } else if (predicatedContext.valueExpression() instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext = (SqlBaseParser.ValueExpressionDefaultContext) predicatedContext.valueExpression();
                primaryExpressionContext = valueExpressionDefaultContext.primaryExpression();

                if (primaryExpressionContext instanceof SqlBaseParser.ParenthesizedExpressionContext) {
                    SqlBaseParser.ParenthesizedExpressionContext parenthesizedExpressionContext = (SqlBaseParser.ParenthesizedExpressionContext) primaryExpressionContext;
                    SqlBaseParser.BooleanExpressionContext expressionContext = parenthesizedExpressionContext.expression().booleanExpression();
                    parseWhereClause(expressionContext, criteriaClause, targetTableAlias);
                    return;
                } else {
                    parseOtherConditionContext(predicatedContext, criteriaClause);
                }
            } else {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(booleanExpressionContext)));
            }

            if (primaryExpressionContext instanceof SqlBaseParser.ColumnReferenceContext) {
                criteriaClause.setLeftExpr(primaryExpressionContext.getText());
            } else if (primaryExpressionContext instanceof SqlBaseParser.DereferenceContext) {
                SqlBaseParser.DereferenceContext dereferenceContext = (SqlBaseParser.DereferenceContext) primaryExpressionContext;
                String field = dereferenceContext.fieldName.getText();
                criteriaClause.setLeftExpr(field);
                String alias = dereferenceContext.primaryExpression().getText();
                if (!alias.equalsIgnoreCase(targetTableAlias)) {
                    throw new ParseException(String.format("cannot found alias '%s' of field: '%s' in WHERE clause: %s", alias, field, getFullText(booleanExpressionContext)));
                }
            } else {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(booleanExpressionContext)));
            }
        }
    }

    public static void visitOnWhereClause(CriteriaClause clause, StringBuilder conditionSql) {
        if (OP_AND.equalsIgnoreCase(clause.getOperator())) {
            conditionSql.append(" ( ");
            visitOnWhereClause(clause.getLeftCriteria(), conditionSql);
            conditionSql.append(" AND ");
            visitOnWhereClause(clause.getRightCriteria(), conditionSql);
            conditionSql.append(" ) ");
        } else if (OP_OR.equalsIgnoreCase(clause.getOperator())) {
            conditionSql.append(" ( ");
            visitOnWhereClause(clause.getLeftCriteria(), conditionSql);
            conditionSql.append(" OR ");
            visitOnWhereClause(clause.getRightCriteria(), conditionSql);
            conditionSql.append(" ) ");
        } else if (clause.getLeftExpr() != null) {
            if (clause.getRightExpr() != null) {
                conditionSql.append(clause.getLeftExpr()).append(" ").append(clause.getOperator()).append(" ").append(clause.getRightExpr());
            } else if (clause.getInItems() != null) {
                List<String> placeholders = new ArrayList<>();
                for (int i = 0; i < clause.getInItems().size(); i++) {
                    placeholders.add(clause.getRightExpr());
                }
                conditionSql.append(clause.getLeftExpr()).append(" IN ( ").append(String.join(", ", placeholders)).append(" ) ");
            }
        } else {
            throw new AresException("Unsupported where clause: " + clause);
        }
    }

    private static void parseLogicalBinaryContext(SqlBaseParser.LogicalBinaryContext logicalBinaryContext, CriteriaClause criteriaClause, String targetTableAlias) {
        if (OP_AND.equalsIgnoreCase(logicalBinaryContext.operator.getText())) {
            criteriaClause.setOperator(OP_AND);
        } else if (OP_OR.equalsIgnoreCase(logicalBinaryContext.operator.getText())) {
            criteriaClause.setOperator(OP_OR);
        } else {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(logicalBinaryContext)));
        }

        CriteriaClause leftClause = new CriteriaClause();
        SqlBaseParser.BooleanExpressionContext leftExpressionContext = logicalBinaryContext.left;
        parseWhereClause(leftExpressionContext, leftClause, targetTableAlias);
        criteriaClause.setLeftCriteria(leftClause);

        CriteriaClause rightClause = new CriteriaClause();
        SqlBaseParser.BooleanExpressionContext rightExpressionContext = logicalBinaryContext.right;
        parseWhereClause(rightExpressionContext, rightClause, targetTableAlias);
        criteriaClause.setRightCriteria(rightClause);
    }

    private static SqlBaseParser.PrimaryExpressionContext parseComparisonContext(
            SqlBaseParser.ComparisonContext comparisonContext, CriteriaClause criteriaClause) {
        String operator = comparisonContext.comparisonOperator().getText();
        if (OP_EQ.equals(operator)) {
            criteriaClause.setOperator(OP_EQ);
        } else if (OP_NE.equals(operator) || OP_NE2.equals(operator)) {
            criteriaClause.setOperator(OP_NE);
        } else if (OP_GT.equals(operator)) {
            criteriaClause.setOperator(OP_GT);
        } else if (OP_LT.equals(operator)) {
            criteriaClause.setOperator(OP_LT);
        } else if (OP_GE.equals(operator)) {
            criteriaClause.setOperator(OP_GE);
        } else if (OP_LE.equals(operator)) {
            criteriaClause.setOperator(OP_LE);
        } else {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(comparisonContext)));
        }
        if (!(comparisonContext.left instanceof SqlBaseParser.ValueExpressionDefaultContext)) {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(comparisonContext)));
        }
        criteriaClause.setRightExpr(getFullText(comparisonContext.right));
        return ((SqlBaseParser.ValueExpressionDefaultContext) comparisonContext.left).primaryExpression();
    }

    private static void parseOtherConditionContext(SqlBaseParser.PredicatedContext predicatedContext, CriteriaClause criteriaClause) {
        if (predicatedContext.predicate().IN() != null) {
            criteriaClause.setOperator(OP_IN);
        } else if (predicatedContext.predicate().NOT() != null && predicatedContext.predicate().LIKE() != null) {
            criteriaClause.setOperator(OP_NOT_LIKE);
        } else if (predicatedContext.predicate().LIKE() != null) {
            criteriaClause.setOperator(OP_LIKE);
        } else {
            throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(predicatedContext)));
        }

        if (OP_IN.equalsIgnoreCase(criteriaClause.getOperator())) {
            if (predicatedContext.predicate().expression().isEmpty()) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(predicatedContext)));
            }
            criteriaClause.setInItems(new ArrayList<>());
            predicatedContext.predicate().expression().forEach(expr ->
                    criteriaClause.getInItems().add(getFullText(expr)));
        } else {
            if (predicatedContext.predicate().valueExpression().isEmpty()) {
                throw new ParseException(String.format(UNSUPPORTED_EXP_MSG, getFullText(predicatedContext)));
            }
            StringJoiner joiner = new StringJoiner(" ");
            for (SqlBaseParser.ValueExpressionContext expressionContext : predicatedContext.predicate().valueExpression()) {
                joiner.add(getFullText(expressionContext));
            }
            String rightExpr = joiner.toString();
            criteriaClause.setRightExpr(rightExpr);
        }
    }
}

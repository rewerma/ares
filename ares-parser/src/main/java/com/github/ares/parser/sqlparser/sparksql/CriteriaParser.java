package com.github.ares.parser.sqlparser.sparksql;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.parser.antlr4.sparksql.SqlBaseParser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static com.github.ares.parser.utils.PLParserUtil.getFullText;

public class CriteriaParser {
    public static void parseWhereClause(SqlBaseParser.BooleanExpressionContext booleanExpressionContext, CriteriaClause criteriaClause, String targetTableAlias) {
        if (booleanExpressionContext instanceof SqlBaseParser.LogicalBinaryContext) {
            SqlBaseParser.LogicalBinaryContext logicalBinaryContext = (SqlBaseParser.LogicalBinaryContext) booleanExpressionContext;
            if ("AND".equalsIgnoreCase(logicalBinaryContext.operator.getText())) {
                criteriaClause.setOperator("AND");
            } else if ("OR".equalsIgnoreCase(logicalBinaryContext.operator.getText())) {
                criteriaClause.setOperator("OR");
            } else {
                throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
            }

            CriteriaClause leftClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext leftExpressionContext = logicalBinaryContext.left;
            parseWhereClause(leftExpressionContext, leftClause, targetTableAlias);
            criteriaClause.setLeftCriteria(leftClause);

            CriteriaClause rightClause = new CriteriaClause();
            SqlBaseParser.BooleanExpressionContext rightExpressionContext = logicalBinaryContext.right;
            parseWhereClause(rightExpressionContext, rightClause, targetTableAlias);
            criteriaClause.setRightCriteria(rightClause);
        } else if (booleanExpressionContext instanceof SqlBaseParser.PredicatedContext) {
            SqlBaseParser.PredicatedContext predicatedContext = (SqlBaseParser.PredicatedContext) booleanExpressionContext;
            String operator;
            SqlBaseParser.PrimaryExpressionContext primaryExpressionContext;
            if (predicatedContext.valueExpression() instanceof SqlBaseParser.ComparisonContext) {
                SqlBaseParser.ComparisonContext comparisonContext = (SqlBaseParser.ComparisonContext) predicatedContext.valueExpression();
                operator = comparisonContext.comparisonOperator().getText();
                if ("=".equals(operator)) {
                    criteriaClause.setOperator("=");
                } else if ("!=".equals(operator) || "<>".equals(operator)) {
                    criteriaClause.setOperator("!=");
                } else if (">".equals(operator)) {
                    criteriaClause.setOperator(">");
                } else if ("<".equals(operator)) {
                    criteriaClause.setOperator("<");
                } else if (">=".equals(operator)) {
                    criteriaClause.setOperator(">=");
                } else if ("<=".equals(operator)) {
                    criteriaClause.setOperator("<=");
                } else {
                    throw new ParseException(String.format("unsupported operator %s in WHERE clause: %s: ", operator, getFullText(booleanExpressionContext)));
                }
                if (!(comparisonContext.left instanceof SqlBaseParser.ValueExpressionDefaultContext)) {
                    throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
                }
                primaryExpressionContext = ((SqlBaseParser.ValueExpressionDefaultContext) comparisonContext.left).primaryExpression();
                criteriaClause.setRightExpr(getFullText(comparisonContext.right));
            } else if (predicatedContext.valueExpression() instanceof SqlBaseParser.ValueExpressionDefaultContext) {
                SqlBaseParser.ValueExpressionDefaultContext valueExpressionDefaultContext = (SqlBaseParser.ValueExpressionDefaultContext) predicatedContext.valueExpression();
                primaryExpressionContext = valueExpressionDefaultContext.primaryExpression();

                if (primaryExpressionContext instanceof SqlBaseParser.ParenthesizedExpressionContext) {
                    SqlBaseParser.ParenthesizedExpressionContext parenthesizedExpressionContext = (SqlBaseParser.ParenthesizedExpressionContext) primaryExpressionContext;
                    SqlBaseParser.BooleanExpressionContext expressionContext = parenthesizedExpressionContext.expression().booleanExpression();
                    parseWhereClause(expressionContext, criteriaClause, targetTableAlias);
                    return;
                } else {
                    if (predicatedContext.predicate().IN() != null) {
                        criteriaClause.setOperator("IN");
                    } else if (predicatedContext.predicate().NOT() != null && predicatedContext.predicate().LIKE() != null) {
                        criteriaClause.setOperator("NOT LIKE");
                    } else if (predicatedContext.predicate().LIKE() != null) {
                        criteriaClause.setOperator("LIKE");
                    } else {
                        throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
                    }

                    if ("IN".equalsIgnoreCase(criteriaClause.getOperator())) {
                        if (predicatedContext.predicate().expression().isEmpty()) {
                            throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
                        }
                        criteriaClause.setInItems(new ArrayList<>());
                        predicatedContext.predicate().expression().forEach(expr ->
                                criteriaClause.getInItems().add(getFullText(expr)));
                    } else {
                        if (predicatedContext.predicate().valueExpression().isEmpty()) {
                            throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
                        }
                        StringJoiner joiner = new StringJoiner(" ");
                        for (SqlBaseParser.ValueExpressionContext expressionContext : predicatedContext.predicate().valueExpression()) {
                            joiner.add(getFullText(expressionContext));
                        }
                        String rightExpr = joiner.toString();
                        criteriaClause.setRightExpr(rightExpr);
                    }
                }
            } else {
                throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
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
                throw new ParseException("unsupported syntax: " + getFullText(booleanExpressionContext) + " in WHERE clause");
            }
        }
    }

    public static void visitOnWhereClause(CriteriaClause clause, StringBuilder conditionSql) {
        if ("AND".equalsIgnoreCase(clause.getOperator())) {
            conditionSql.append(" ( ");
            visitOnWhereClause(clause.getLeftCriteria(), conditionSql);
            conditionSql.append(" AND ");
            visitOnWhereClause(clause.getRightCriteria(), conditionSql);
            conditionSql.append(" ) ");
        } else if ("OR".equalsIgnoreCase(clause.getOperator())) {
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
}

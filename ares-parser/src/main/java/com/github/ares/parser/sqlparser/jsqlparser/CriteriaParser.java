package com.github.ares.parser.sqlparser.jsqlparser;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.ParseException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class CriteriaParser {
    public static void parseWhere(String tableAlias, Expression whereExpr, CriteriaClause criteriaClause) {
        try {
            if (whereExpr instanceof Parenthesis) {
                Parenthesis parenthesis = (Parenthesis) whereExpr;
                parseWhere(tableAlias, parenthesis.getExpression(), criteriaClause);
            } else if (whereExpr instanceof AndExpression) {
                AndExpression andExpression = (AndExpression) whereExpr;
                CriteriaClause leftClause = new CriteriaClause();
                CriteriaClause rightClause = new CriteriaClause();
                criteriaClause.setOperator("AND");
                parseWhere(tableAlias, andExpression.getLeftExpression(), leftClause);
                parseWhere(tableAlias, andExpression.getRightExpression(), rightClause);
                criteriaClause.setLeftCriteria(leftClause);
                criteriaClause.setRightCriteria(rightClause);
            } else if (whereExpr instanceof OrExpression) {
                OrExpression orExpression = (OrExpression) whereExpr;
                CriteriaClause leftClause = new CriteriaClause();
                CriteriaClause rightClause = new CriteriaClause();
                criteriaClause.setOperator("OR");
                parseWhere(tableAlias, orExpression.getLeftExpression(), leftClause);
                parseWhere(tableAlias, orExpression.getRightExpression(), rightClause);
                criteriaClause.setLeftCriteria(leftClause);
                criteriaClause.setRightCriteria(rightClause);
            } else if (whereExpr instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) whereExpr;
                if (comparisonOperator instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) comparisonOperator;
                    criteriaClause.setOperator("=");
                    setCriteriaValues(tableAlias, equalsTo, criteriaClause);
                } else if (comparisonOperator instanceof NotEqualsTo) {
                    NotEqualsTo notEqualsTo = (NotEqualsTo) comparisonOperator;
                    criteriaClause.setOperator("!=");
                    setCriteriaValues(tableAlias, notEqualsTo, criteriaClause);
                } else if (comparisonOperator instanceof GreaterThan) {
                    GreaterThan greaterThan = (GreaterThan) comparisonOperator;
                    criteriaClause.setOperator(">");
                    setCriteriaValues(tableAlias, greaterThan, criteriaClause);
                } else if (comparisonOperator instanceof GreaterThanEquals) {
                    GreaterThanEquals greaterThanEquals = (GreaterThanEquals) comparisonOperator;
                    criteriaClause.setOperator(">=");
                    setCriteriaValues(tableAlias, greaterThanEquals, criteriaClause);
                } else if (comparisonOperator instanceof MinorThan) {
                    MinorThan minorThan = (MinorThan) comparisonOperator;
                    criteriaClause.setOperator("<");
                    setCriteriaValues(tableAlias, minorThan, criteriaClause);
                } else if (comparisonOperator instanceof MinorThanEquals) {
                    MinorThanEquals minorThanEquals = (MinorThanEquals) comparisonOperator;
                    criteriaClause.setOperator("<=");
                    setCriteriaValues(tableAlias, minorThanEquals, criteriaClause);
                }
            } else if (whereExpr instanceof LikeExpression) {
                LikeExpression likeExpression = (LikeExpression) whereExpr;
                if (likeExpression.getASTNode().jjtGetFirstToken() != null &&
                        "not".equalsIgnoreCase(likeExpression.getASTNode().jjtGetFirstToken().toString())) {
                    criteriaClause.setOperator("NOT LIKE");
                } else {
                    criteriaClause.setOperator("LIKE");
                }
                setCriteriaValues(tableAlias, likeExpression, criteriaClause);
            } else if (whereExpr instanceof InExpression) {
                InExpression inExpression = (InExpression) whereExpr;
                criteriaClause.setOperator("IN");
                setCriteriaValues(tableAlias, inExpression, criteriaClause);
            } else {
                throw new ParseException("unsupported syntax: " + whereExpr.toString() + " in WHERE clause");
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + whereExpr.toString() + " in WHERE clause");
        }
    }

    public static void setCriteriaValues(String tableAlias, Expression expression, CriteriaClause criteriaClause) {
        Expression leftExpr;
        if (expression instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) expression;
            leftExpr = comparisonOperator.getLeftExpression();
            criteriaClause.setRightExpr(comparisonOperator.getRightExpression().toString());
        } else if (expression instanceof LikeExpression) {
            LikeExpression likeExpression = (LikeExpression) expression;
            leftExpr = likeExpression.getLeftExpression();
            criteriaClause.setRightExpr(likeExpression.getRightExpression().toString());
        } else if (expression instanceof InExpression) {
            InExpression inExpression = (InExpression) expression;
            leftExpr = inExpression.getLeftExpression();
            List<Expression> expressionList = ((ExpressionList) inExpression.getRightItemsList()).getExpressions();
            criteriaClause.setInItems(new ArrayList<>());
            for (Expression inExpr : expressionList) {
                criteriaClause.getInItems().add(inExpr.toString());
            }
        } else {
            throw new ParseException("unsupported syntax: " + expression.toString() + " in WHERE clause");
        }
        if (!(leftExpr instanceof Column)) {
            throw new ParseException("unsupported syntax: " + leftExpr +
                    " in WHERE clause, left expression must be column");
        }
        Column leftColumn = (Column) leftExpr;
        if (leftColumn.getTable() != null && !leftColumn.getTable().toString().equals(tableAlias)) {
            throw new ParseException("unsupported syntax: " + leftColumn.getTable() +
                    " in WHERE clause, left expression must be column of table " + tableAlias);
        }
        criteriaClause.setLeftExpr(leftColumn.getColumnName());
    }

    public static void parseOnExpression(String tableAlias, String tableAlias2, Expression whereExpr, CriteriaClause criteriaClause) {
        try {
            if (whereExpr instanceof Parenthesis) {
                Parenthesis parenthesis = (Parenthesis) whereExpr;
                parseOnExpression(tableAlias, tableAlias2, parenthesis.getExpression(), criteriaClause);
            } else if (whereExpr instanceof AndExpression) {
                AndExpression andExpression = (AndExpression) whereExpr;
                CriteriaClause leftClause = new CriteriaClause();
                CriteriaClause rightClause = new CriteriaClause();
                criteriaClause.setOperator("AND");
                parseOnExpression(tableAlias, tableAlias2, andExpression.getLeftExpression(), leftClause);
                parseOnExpression(tableAlias, tableAlias2, andExpression.getRightExpression(), rightClause);
                criteriaClause.setLeftCriteria(leftClause);
                criteriaClause.setRightCriteria(rightClause);
            } else if (whereExpr instanceof OrExpression) {
                OrExpression orExpression = (OrExpression) whereExpr;
                CriteriaClause leftClause = new CriteriaClause();
                CriteriaClause rightClause = new CriteriaClause();
                criteriaClause.setOperator("OR");
                parseOnExpression(tableAlias, tableAlias2, orExpression.getLeftExpression(), leftClause);
                parseOnExpression(tableAlias, tableAlias2, orExpression.getRightExpression(), rightClause);
                criteriaClause.setLeftCriteria(leftClause);
                criteriaClause.setRightCriteria(rightClause);
            } else if (whereExpr instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) whereExpr;
                if (comparisonOperator instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) comparisonOperator;
                    criteriaClause.setOperator("=");
                    setCriteriaValues(tableAlias, tableAlias2, equalsTo, criteriaClause);
                } else if (comparisonOperator instanceof NotEqualsTo) {
                    NotEqualsTo notEqualsTo = (NotEqualsTo) comparisonOperator;
                    criteriaClause.setOperator("!=");
                    setCriteriaValues(tableAlias, tableAlias2, notEqualsTo, criteriaClause);
                } else if (comparisonOperator instanceof GreaterThan) {
                    GreaterThan greaterThan = (GreaterThan) comparisonOperator;
                    criteriaClause.setOperator(">");
                    setCriteriaValues(tableAlias, tableAlias2, greaterThan, criteriaClause);
                } else if (comparisonOperator instanceof GreaterThanEquals) {
                    GreaterThanEquals greaterThanEquals = (GreaterThanEquals) comparisonOperator;
                    criteriaClause.setOperator(">=");
                    setCriteriaValues(tableAlias, tableAlias2, greaterThanEquals, criteriaClause);
                } else if (comparisonOperator instanceof MinorThan) {
                    MinorThan minorThan = (MinorThan) comparisonOperator;
                    criteriaClause.setOperator("<");
                    setCriteriaValues(tableAlias, tableAlias2, minorThan, criteriaClause);
                } else if (comparisonOperator instanceof MinorThanEquals) {
                    MinorThanEquals minorThanEquals = (MinorThanEquals) comparisonOperator;
                    criteriaClause.setOperator("<=");
                    setCriteriaValues(tableAlias, tableAlias2, minorThanEquals, criteriaClause);
                }
            } else if (whereExpr instanceof LikeExpression) {
                LikeExpression likeExpression = (LikeExpression) whereExpr;
                if (likeExpression.getASTNode().jjtGetFirstToken() != null &&
                        "not".equalsIgnoreCase(likeExpression.getASTNode().jjtGetFirstToken().toString())) {
                    criteriaClause.setOperator("NOT LIKE");
                } else {
                    criteriaClause.setOperator("LIKE");
                }
                setCriteriaValues(tableAlias, tableAlias2, likeExpression, criteriaClause);
            } else if (whereExpr instanceof InExpression) {
                InExpression inExpression = (InExpression) whereExpr;
                criteriaClause.setOperator("IN");
                setCriteriaValues(tableAlias, tableAlias2, inExpression, criteriaClause);
            } else {
                throw new ParseException("unsupported syntax: " + whereExpr.toString() + " in WHERE clause");
            }
        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("unsupported syntax: " + whereExpr.toString() + " in WHERE clause");
        }
    }

    public static void setCriteriaValues(String tableAlias, String tableAlias2, Expression expression, CriteriaClause criteriaClause) {
        Expression leftExpr;
        Expression rightExpr;
        if (expression instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) expression;
            leftExpr = comparisonOperator.getLeftExpression();
            rightExpr = comparisonOperator.getRightExpression();
        } else if (expression instanceof LikeExpression) {
            LikeExpression likeExpression = (LikeExpression) expression;
            leftExpr = likeExpression.getLeftExpression();
            rightExpr = likeExpression.getRightExpression();
        } else {
            throw new ParseException("unsupported syntax: " + expression.toString() + " in WHERE clause");
        }
        if (!(leftExpr instanceof Column)) {
            throw new ParseException("unsupported syntax: " + leftExpr +
                    " in WHERE clause, left expression must be column");
        }
        Column leftColumn = (Column) leftExpr;
        if (leftColumn.getTable() != null && !leftColumn.getTable().toString().equals(tableAlias)) {
            throw new ParseException("unsupported syntax: " + leftColumn.getTable() +
                    " in WHERE clause, left expression must be column of table " + tableAlias);
        }
        criteriaClause.setLeftExpr(leftColumn.getColumnName());

        if (!(rightExpr instanceof Column)) {
            throw new ParseException("unsupported syntax: " + leftExpr +
                    " in WHERE clause, right expression must be column");
        }
        Column rightColumn = (Column) rightExpr;
        if (rightColumn.getTable() != null && !rightColumn.getTable().toString().equals(tableAlias2)) {
            throw new ParseException("unsupported syntax: " + rightColumn.getTable() +
                    " in WHERE clause, right expression must be column of table " + tableAlias2);
        }
        criteriaClause.setRightExpr(tableAlias2 + "." + rightColumn.getColumnName());
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

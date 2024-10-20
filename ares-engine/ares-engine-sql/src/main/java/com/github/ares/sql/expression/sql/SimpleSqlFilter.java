package com.github.ares.sql.expression.sql;

import com.github.ares.sql.expression.exception.ExpressionException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleSqlFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final SimpleSqlFunction simpleSqlFunction;

    public SimpleSqlFilter(SimpleSqlFunction simpleSqlFunction) {
        this.simpleSqlFunction = simpleSqlFunction;
    }

    public boolean executeFilter(Expression whereExpr, Object[] inputFields) {
        if (whereExpr == null) {
            return true;
        }
        if (whereExpr instanceof Function) {
            return functionExpr((Function) whereExpr, inputFields);
        }
        if (whereExpr instanceof IsNullExpression) {
            return isNullExpr((IsNullExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof InExpression) {
            return inExpr((InExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof LikeExpression) {
            boolean isNotLike = ((LikeExpression) whereExpr).isNot();
            // not like SQL parsing
            if (isNotLike) {
                return notLikeExpr((LikeExpression) whereExpr, inputFields);
            } else { // like SQL parsing
                return likeExpr((LikeExpression) whereExpr, inputFields);
            }
        }
        if (whereExpr instanceof ComparisonOperator) {
            Pair<Object, Object> pair =
                    executeComparisonOperator((ComparisonOperator) whereExpr, inputFields);
            if (whereExpr instanceof EqualsTo) {
                return equalsToExpr(pair);
            }
            if (whereExpr instanceof NotEqualsTo) {
                return notEqualsToExpr(pair);
            }
            if (whereExpr instanceof GreaterThan) {
                return greaterThanExpr(pair);
            }
            if (whereExpr instanceof GreaterThanEquals) {
                return greaterThanEqualsExpr(pair);
            }
            if (whereExpr instanceof MinorThan) {
                return minorThanExpr(pair);
            }
            if (whereExpr instanceof MinorThanEquals) {
                return minorThanEqualsExpr(pair);
            }
        }
        if (whereExpr instanceof AndExpression) {
            return andExpr((AndExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof OrExpression) {
            return orExpr((OrExpression) whereExpr, inputFields);
        }
        if (whereExpr instanceof Parenthesis) {
            return parenthesisExpr((Parenthesis) whereExpr, inputFields);
        }
        throw new ExpressionException(
                String.format("Unsupported SQL Expression: %s ", whereExpr));
    }

    private boolean functionExpr(Function function, Object[] inputFields) {
        Boolean result = (Boolean) simpleSqlFunction.computeForValue(function, inputFields);
        if (result == null) {
            return false;
        }
        return result;
    }

    private boolean isNullExpr(IsNullExpression isNullExpression, Object[] inputFields) {
        Expression leftExpr = isNullExpression.getLeftExpression();
        Object leftVal = simpleSqlFunction.computeForValue(leftExpr, inputFields);
        if (isNullExpression.isNot()) {
            return leftVal != null;
        } else {
            return leftVal == null;
        }
    }

    private boolean inExpr(InExpression inExpression, Object[] inputFields) {
        Expression leftExpr = inExpression.getLeftExpression();
        ExpressionList itemsList = (ExpressionList) inExpression.getRightItemsList();
        Object leftValue = simpleSqlFunction.computeForValue(leftExpr, inputFields);
        for (Expression exprItem : itemsList.getExpressions()) {
            Object rightValue = simpleSqlFunction.computeForValue(exprItem, inputFields);
            if (leftValue == null && rightValue == null) {
                return true;
            }
            if (leftValue != null) {
                if (leftValue instanceof Number && rightValue instanceof Number) {
                    if (((Number) leftValue).doubleValue() == ((Number) rightValue).doubleValue()) {
                        return !inExpression.isNot();
                    }
                } else if (leftValue.equals(rightValue)) {
                    return !inExpression.isNot();
                }

            } else {
                return false;
            }
        }
        return inExpression.isNot(); // if all not in return true
    }

    /**
     * Like expression filter
     *
     * @param likeExpression like expression
     * @param inputFields    input fields
     * @return filter result
     */
    private boolean likeExpr(LikeExpression likeExpression, Object[] inputFields) {
        Expression leftExpr = likeExpression.getLeftExpression();
        Object leftVal = simpleSqlFunction.computeForValue(leftExpr, inputFields);
        if (leftVal == null) {
            return false;
        }
        Expression rightExpr = likeExpression.getRightExpression();
        Object rightVal = simpleSqlFunction.computeForValue(rightExpr, inputFields);
        if (rightVal == null) {
            return false;
        }
        String regex = rightVal.toString();
        String likeIdent = "%";
        if (regex.startsWith(likeIdent)) {
            regex = regex.replaceFirst(likeIdent, ".*");
        }
        if (regex.endsWith(likeIdent)) {
            regex = regex.substring(0, regex.length() - 1) + ".*";
        }
        if (regex.startsWith("_")) {
            regex = regex.replaceFirst("_", ".");
        }
        if (regex.endsWith("_")) {
            regex = regex.substring(0, regex.length() - 1) + ".";
        }
        if (regex.length() >= 3 && regex.substring(regex.length() - 3).endsWith("_.*")) {
            regex = regex.substring(0, regex.length() - 3) + "..*";
        }
        if (regex.startsWith("'") && regex.endsWith("'")) {
            regex = regex.substring(0, regex.length() - 1).substring(1);
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(leftVal.toString());

        return matcher.matches();
    }

    /**
     * Not Like expression filter
     *
     * @param likeExpression not like expression
     * @param inputFields    input fields
     * @return filter result
     */
    private boolean notLikeExpr(LikeExpression likeExpression, Object[] inputFields) {
        Expression leftExpr = likeExpression.getLeftExpression();
        Object leftVal = simpleSqlFunction.computeForValue(leftExpr, inputFields);
        if (leftVal == null) {
            return false;
        }
        Expression rightExpr = likeExpression.getRightExpression();
        Object rightVal = simpleSqlFunction.computeForValue(rightExpr, inputFields);
        if (rightVal == null) {
            return false;
        }
        String regex = rightVal.toString();
        String likeIdent = "%";
        if (regex.startsWith(likeIdent)) {
            regex = regex.replaceFirst(likeIdent, ".*");
        }
        if (regex.endsWith(likeIdent)) {
            regex = regex.substring(0, regex.length() - 1) + ".*";
        }
        if (regex.startsWith("_")) {
            regex = regex.replaceFirst("_", ".");
        }
        if (regex.endsWith("_")) {
            regex = regex.substring(0, regex.length() - 1) + ".";
        }
        if (regex.length() >= 3 && regex.substring(regex.length() - 3).endsWith("_.*")) {
            regex = regex.substring(0, regex.length() - 3) + "..*";
        }
        if (regex.startsWith("'") && regex.endsWith("'")) {
            regex = regex.substring(0, regex.length() - 1).substring(1);
        }
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(leftVal.toString());

        return !matcher.matches();
    }

    private Pair<Object, Object> executeComparisonOperator(
            ComparisonOperator comparisonOperator, Object[] inputFields) {
        Expression leftExpr = comparisonOperator.getLeftExpression();
        Expression rightExpr = comparisonOperator.getRightExpression();
        Object leftVal = simpleSqlFunction.computeForValue(leftExpr, inputFields);
        Object rightVal = simpleSqlFunction.computeForValue(rightExpr, inputFields);
        return Pair.of(leftVal, rightVal);
    }

    private boolean equalsToExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() == ((Number) rightVal).doubleValue();
        }
        return leftVal.equals(rightVal);
    }

    private boolean notEqualsToExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null) {
            return rightVal != null;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() != ((Number) rightVal).doubleValue();
        }
        return !leftVal.equals(rightVal);
    }

    private boolean greaterThanExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() > ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) > 0;
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isAfter((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isAfter((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isAfter((LocalTime) rightVal);
        }
        throw new ExpressionException(
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean greaterThanEqualsExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() >= ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) >= 0;
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isAfter((LocalDateTime) rightVal)
                    || ((LocalDateTime) leftVal).isEqual((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isAfter((LocalDate) rightVal)
                    || ((LocalDate) leftVal).isEqual((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isAfter((LocalTime) rightVal) || leftVal.equals(rightVal);
        }
        throw new ExpressionException(
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean minorThanExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isBefore((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isBefore((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isBefore((LocalTime) rightVal);
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() < ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) < 0;
        }
        throw new ExpressionException(
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean minorThanEqualsExpr(Pair<Object, Object> pair) {
        Object leftVal = pair.getKey();
        Object rightVal = pair.getValue();
        if (leftVal == null || rightVal == null) {
            return false;
        }
        if (leftVal instanceof LocalDateTime && rightVal instanceof LocalDateTime) {
            return ((LocalDateTime) leftVal).isBefore((LocalDateTime) rightVal)
                    || ((LocalDateTime) leftVal).isEqual((LocalDateTime) rightVal);
        }
        if (leftVal instanceof LocalDate && rightVal instanceof LocalDate) {
            return ((LocalDate) leftVal).isBefore((LocalDate) rightVal)
                    || ((LocalDate) leftVal).isEqual((LocalDate) rightVal);
        }
        if (leftVal instanceof LocalTime && rightVal instanceof LocalTime) {
            return ((LocalTime) leftVal).isBefore((LocalTime) rightVal) || leftVal.equals(rightVal);
        }
        if (leftVal instanceof Number && rightVal instanceof Number) {
            return ((Number) leftVal).doubleValue() <= ((Number) rightVal).doubleValue();
        }
        if (leftVal instanceof String && rightVal instanceof String) {
            return ((String) leftVal).compareTo((String) rightVal) <= 0;
        }
        throw new ExpressionException(
                String.format(
                        "Filed types not matched, left is: %s, right is: %s ",
                        leftVal.getClass().getSimpleName(), rightVal.getClass().getSimpleName()));
    }

    private boolean andExpr(AndExpression andExpression, Object[] inputFields) {
        Expression leftExpr = andExpression.getLeftExpression();
        boolean leftRes = executeFilter(leftExpr, inputFields);
        Expression rightExpr = andExpression.getRightExpression();
        boolean rightRes = executeFilter(rightExpr, inputFields);
        return leftRes && rightRes;
    }

    private boolean orExpr(OrExpression orExpression, Object[] inputFields) {
        Expression leftExpr = orExpression.getLeftExpression();
        boolean leftRes = executeFilter(leftExpr, inputFields);
        Expression rightExpr = orExpression.getRightExpression();
        boolean rightRes = executeFilter(rightExpr, inputFields);
        return leftRes || rightRes;
    }

    private boolean parenthesisExpr(Parenthesis parenthesis, Object[] inputFields) {
        Expression expression = parenthesis.getExpression();
        return executeFilter(expression, inputFields);
    }
}

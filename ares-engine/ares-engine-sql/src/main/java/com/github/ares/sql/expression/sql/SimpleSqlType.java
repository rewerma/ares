package com.github.ares.sql.expression.sql;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.sql.expression.exception.ExpressionException;
import com.github.ares.sql.function.FunctionInterface;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleSqlType implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DECIMAL = "DECIMAL";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String INT = "INT";
    public static final String INTEGER = "INTEGER";
    public static final String BIGINT = "BIGINT";
    public static final String LONG = "LONG";
    public static final String BYTE = "BYTE";
    public static final String DOUBLE = "DOUBLE";
    public static final String FLOAT = "FLOAT";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String DATETIME = "DATETIME";
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";

    private final Map<String, FunctionInterface> allFunctions;

    public SimpleSqlType(Map<String, FunctionInterface> allFunctions) {
        this.allFunctions = allFunctions;
    }

    public AresDataType<?> getExpressionType(Expression expression) {
        if (expression instanceof NullValue) {
            return BasicType.VOID_TYPE;
        }
        if (expression instanceof SignedExpression) {
            return getExpressionType(((SignedExpression) expression).getExpression());
        }
        if (expression instanceof DoubleValue) {
            return BasicType.DOUBLE_TYPE;
        }
        if (expression instanceof LongValue) {
            long longVal = ((LongValue) expression).getValue();
            if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                return BasicType.INT_TYPE;
            }
            return BasicType.LONG_TYPE;
        }
        if (expression instanceof StringValue) {
            return BasicType.STRING_TYPE;
        }
        if (expression instanceof Function) {
            return getFunctionType((Function) expression);
        }
        if (expression instanceof TimeKeyExpression) {
            return getTimeKeyExprType((TimeKeyExpression) expression);
        }
        if (expression instanceof ExtractExpression) {
            return BasicType.INT_TYPE;
        }
        if (expression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expression;
            return getExpressionType(parenthesis.getExpression());
        }
        if (expression instanceof Concat) {
            return BasicType.STRING_TYPE;
        }
        if (expression instanceof CastExpression) {
            return getCastType((CastExpression) expression);
        }
        if (expression instanceof Column) {
            Column column = (Column) expression;
            if ("true".equalsIgnoreCase(column.getColumnName()) || "false".equalsIgnoreCase(column.getColumnName())) {
                return BasicType.BOOLEAN_TYPE;
            }
        }
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;

            if (binaryExpression instanceof ComparisonOperator || binaryExpression instanceof AndExpression
                    || binaryExpression instanceof OrExpression) {
                return BasicType.BOOLEAN_TYPE;
            }

            if (binaryExpression instanceof Division) {
                return BasicType.DOUBLE_TYPE;
            }

            AresDataType<?> leftType = getExpressionType(binaryExpression.getLeftExpression());
            AresDataType<?> rightType = getExpressionType(binaryExpression.getRightExpression());
            if (leftType.getSqlType() == SqlType.INT && rightType.getSqlType() == SqlType.INT) {
                return BasicType.INT_TYPE;
            }
            if ((leftType.getSqlType() == SqlType.INT || leftType.getSqlType() == SqlType.BIGINT)
                    && (rightType.getSqlType() == SqlType.INT
                    || rightType.getSqlType() == SqlType.BIGINT)) {
                return BasicType.LONG_TYPE;
            }
            if (leftType.getSqlType() == SqlType.DECIMAL
                    || rightType.getSqlType() == SqlType.DECIMAL) {
                int precision = 0;
                int scale = 0;
                if (leftType.getSqlType() == SqlType.DECIMAL) {
                    DecimalType decimalType = (DecimalType) leftType;
                    precision = decimalType.getPrecision();
                    scale = decimalType.getScale();
                }
                if (rightType.getSqlType() == SqlType.DECIMAL) {
                    DecimalType decimalType = (DecimalType) rightType;
                    precision = Math.max(decimalType.getPrecision(), precision);
                    scale = Math.max(decimalType.getScale(), scale);
                }
                return new DecimalType(precision, scale);
            }
            if ((leftType.getSqlType() == SqlType.FLOAT || leftType.getSqlType() == SqlType.DOUBLE)
                    || (rightType.getSqlType() == SqlType.FLOAT
                    || rightType.getSqlType() == SqlType.DOUBLE)) {
                return BasicType.DOUBLE_TYPE;
            }
        }
        throw new ExpressionException(
                String.format("Unsupported SQL Expression: %s ", expression.toString()));
    }

    private AresDataType<?> getCastType(CastExpression castExpression) {
        String dataType = castExpression.getType().getDataType();
        switch (dataType.toUpperCase()) {
            case DECIMAL:
                List<String> ps = castExpression.getType().getArgumentsStringList();
                return new DecimalType(Integer.parseInt(ps.get(0)), Integer.parseInt(ps.get(1)));
            case VARCHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case INT:
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
            case LONG:
                return BasicType.LONG_TYPE;
            case BYTE:
                return BasicType.BYTE_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            default:
                throw new ExpressionException(
                        String.format("Unsupported CAST AS type: %s", dataType));
        }
    }

    private AresDataType<?> getFunctionType(Function function) {
        FunctionInterface functionInterface = allFunctions.get(function.getName().toUpperCase());
        if (functionInterface != null) {
            List<Expression> expressions = function.getParameters().getExpressions();
            List<AresDataType<?>> argTypes = new ArrayList<>();
            if (expressions != null) {
                for (Expression expression : expressions) {
                    argTypes.add(getExpressionType(expression));
                }
            }
            AresDataType<?> resultType = functionInterface.resultType(argTypes);
            if (resultType != null) {
                return resultType;
            }
        }
        throw new ExpressionException(
                String.format("Unsupported function: %s ", function.getName()));
    }

    private AresDataType<?> getTimeKeyExprType(TimeKeyExpression timeKeyExpression) {
        switch (timeKeyExpression.getStringValue().toUpperCase()) {
            case SimpleSqlFunction.CURRENT_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SimpleSqlFunction.CURRENT_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                throw new ExpressionException(
                        String.format("Unsupported TimeKey expression: %s ",
                                timeKeyExpression.getStringValue()));
        }
    }
}

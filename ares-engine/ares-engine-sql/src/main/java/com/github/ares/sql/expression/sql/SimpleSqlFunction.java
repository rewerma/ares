package com.github.ares.sql.expression.sql;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresDataTypeHelper;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.sql.expression.exception.ExpressionException;
import com.github.ares.sql.function.FunctionInterface;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.github.ares.sql.function.utils.Utils.toNumber;


public class SimpleSqlFunction implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String CURRENT_DATE = "CURRENT_DATE";
    public static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    public static final String EXTRACT = "EXTRACT";

    private final SimpleSqlType simpleSqlType;
    private final SimpleSqlFilter simpleSqlFilter;

    private final Map<String, FunctionInterface> allFunctions;

    public SimpleSqlFunction(SimpleSqlType simpleSqlType, Map<String, FunctionInterface> allFunctions) {
        this.simpleSqlType = simpleSqlType;
        this.simpleSqlFilter = new SimpleSqlFilter(this);
        this.allFunctions = allFunctions;
    }

    public Object computeForValue(Expression expression, Object[] inputFields) {
        if (expression instanceof NullValue) {
            return null;
        }
        if (expression instanceof SignedExpression) {
            SignedExpression signedExpression = (SignedExpression) expression;
            if (signedExpression.getSign() == '-') {
                Object value = computeForValue(signedExpression.getExpression(), inputFields);
                if (value instanceof Integer) {
                    return -((Integer) value);
                }
                if (value instanceof Long) {
                    return -((Long) value);
                }
                if (value instanceof Double) {
                    return -((Double) value);
                }
                if (value instanceof Number) {
                    return -((Number) value).doubleValue();
                }
            } else {
                return computeForValue(signedExpression, inputFields);
            }
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        if (expression instanceof LongValue) {
            long longVal = ((LongValue) expression).getValue();
            if (longVal <= Integer.MAX_VALUE && longVal >= Integer.MIN_VALUE) {
                return (int) longVal;
            } else {
                return longVal;
            }
        }
        if (expression instanceof StringValue) {
            String res = ((StringValue) expression).getValue();
            if (res != null) {
                res = res.replace("''", "'");
                res = res.replace("\\'", "'");
            }
            return res;
        }

        if (expression instanceof Column) {
            Column column = (Column) expression;
            if ("true".equalsIgnoreCase(column.getColumnName())) {
                return true;
            } else if ("false".equalsIgnoreCase(column.getColumnName())) {
                return false;
            }
        }

        if (expression instanceof Function) {
            Function function = (Function) expression;
            ExpressionList expressionList = function.getParameters();
            List<Object> functionArgs = new ArrayList<>();
            if (expressionList != null) {
                for (Expression funcArgExpression : expressionList.getExpressions()) {
                    functionArgs.add(computeForValue(funcArgExpression, inputFields));
                }
            }
            return executeFunctionExpr(function.getName(), functionArgs);
        }
        if (expression instanceof TimeKeyExpression) {
            return executeTimeKeyExpr(((TimeKeyExpression) expression).getStringValue());
        }
        if (expression instanceof ExtractExpression) {
            ExtractExpression extract = (ExtractExpression) expression;
            List<Object> functionArgs = new ArrayList<>();
            functionArgs.add(computeForValue(extract.getExpression(), inputFields));
            functionArgs.add(extract.getName());
            return executeFunctionExpr(EXTRACT, functionArgs);
        }
        if (expression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expression;
            return computeForValue(parenthesis.getExpression(), inputFields);
        }
        if (expression instanceof BinaryExpression) {
            return executeBinaryExpr((BinaryExpression) expression, inputFields);
        }
        if (expression instanceof CastExpression) {
            CastExpression castExpression = (CastExpression) expression;
            Expression leftExpr = castExpression.getLeftExpression();
            Object leftValue = computeForValue(leftExpr, inputFields);
            return executeCastExpr(castExpression, leftValue);
        }
        if (expression instanceof NotExpression) {
            Object value = computeForValue(((NotExpression) expression).getExpression(), inputFields);
            if (value == null) {
                return null;
            } else {
                return !((Boolean) value);
            }
        }
        throw new ExpressionException(
                String.format("Unsupported SQL Expression: %s ", expression.toString()));
    }

    public Object executeFunctionExpr(String functionName, List<Object> args) {
        FunctionInterface functionInterface = allFunctions.get(functionName.toUpperCase());
        if (functionInterface != null) {
            return functionInterface.evaluate(args);
        }
        throw new ExpressionException(
                String.format("Unsupported function: %s", functionName));
    }

    public Object executeTimeKeyExpr(String timeKeyExpr) {
        FunctionInterface functionInterface = allFunctions.get(timeKeyExpr.toUpperCase());
        if (functionInterface != null) {
            return functionInterface.evaluate(new ArrayList<>());
        }
        throw new ExpressionException(
                String.format("Unsupported TimeKey expression: %s", timeKeyExpr));
    }

    public Object executeCastExpr(CastExpression castExpression, Object arg) {
        String dataType = castExpression.getType().getDataType();
        List<Object> args = new ArrayList<>(2);
        args.add(arg);
        args.add(dataType.toUpperCase());
        if (dataType.equalsIgnoreCase("DECIMAL")) {
            List<String> ps = castExpression.getType().getArgumentsStringList();
            if (ps == null || ps.isEmpty()) {
                args.add(20);
                args.add(10);
            } else {
                args.add(Integer.parseInt(ps.get(0)));
                if (ps.size() > 1) {
                    args.add(Integer.parseInt(ps.get(1)));
                }
            }
        }
        FunctionInterface functionInterface = allFunctions.get("CAST");
        if (functionInterface != null) {
            return functionInterface.evaluate(args);
        }
        return null;
    }

    private Object executeBinaryExpr(BinaryExpression binaryExpression, Object[] inputFields) {
        if (binaryExpression instanceof Concat) {
            Concat concat = (Concat) binaryExpression;
            Expression leftExpr = concat.getLeftExpression();
            Expression rightExpr = concat.getRightExpression();
            Function function = new Function();
            function.setName("CONCAT");
            ExpressionList expressionList = new ExpressionList();
            expressionList.setExpressions(new ArrayList<>());
            expressionList.getExpressions().add(leftExpr);
            expressionList.getExpressions().add(rightExpr);
            function.setParameters(expressionList);
            return computeForValue(function, inputFields);
        }
        AresDataType<?> resultType = simpleSqlType.getExpressionType(binaryExpression);
        if (resultType == BasicType.BOOLEAN_TYPE) {
            return simpleSqlFilter.executeFilter(binaryExpression, inputFields);
        }
        Number leftValue =
                toNumber(computeForValue(binaryExpression.getLeftExpression(), inputFields));
        Number rightValue =
                toNumber(computeForValue(binaryExpression.getRightExpression(), inputFields));
        if (leftValue == null || rightValue == null) {
            return null;
        }
        AresDataType<?> dataType = getMaxDataType(leftValue, rightValue);
        if (binaryExpression instanceof Addition) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() + rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() + rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() + rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() + rightValue.longValue();
            } else if (dataType == BasicType.FLOAT_TYPE) {
                return leftValue.floatValue() + rightValue.floatValue();
            } else if (dataType == BasicType.DOUBLE_TYPE) {
                return leftValue.doubleValue() + rightValue.doubleValue();
            } else if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                BigDecimal decimal1 = new BigDecimal(leftValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                BigDecimal decimal2 = new BigDecimal(rightValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                return decimal1.add(decimal2);
            }
        }
        if (binaryExpression instanceof Subtraction) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() - rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() - rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() - rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() - rightValue.longValue();
            } else if (dataType == BasicType.FLOAT_TYPE) {
                return leftValue.floatValue() - rightValue.floatValue();
            } else if (dataType == BasicType.DOUBLE_TYPE) {
                return leftValue.doubleValue() - rightValue.doubleValue();
            } else if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                BigDecimal decimal1 = new BigDecimal(leftValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                BigDecimal decimal2 = new BigDecimal(rightValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                return decimal1.subtract(decimal2);
            }
        }
        if (binaryExpression instanceof Multiplication) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() * rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() * rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() * rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() * rightValue.longValue();
            } else if (dataType == BasicType.FLOAT_TYPE) {
                return leftValue.floatValue() * rightValue.floatValue();
            } else if (dataType == BasicType.DOUBLE_TYPE) {
                return leftValue.doubleValue() * rightValue.doubleValue();
            } else if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                BigDecimal decimal1 = new BigDecimal(leftValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                BigDecimal decimal2 = new BigDecimal(rightValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                return decimal1.multiply(decimal2).setScale(12, RoundingMode.HALF_UP);
            }
        }
        if (binaryExpression instanceof Division) {
            if (rightValue.intValue() == 0) {
                return null;
            }
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() / rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() / rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() / rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() / rightValue.longValue();
            } else if (dataType == BasicType.FLOAT_TYPE) {
                return leftValue.floatValue() / rightValue.floatValue();
            } else if (dataType == BasicType.DOUBLE_TYPE) {
                return leftValue.doubleValue() / rightValue.doubleValue();
            } else if (dataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) dataType;
                BigDecimal decimal1 = new BigDecimal(leftValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                BigDecimal decimal2 = new BigDecimal(rightValue.toString()).setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                return decimal1.divide(decimal2, 12, RoundingMode.HALF_UP);
            }
        }
        if (binaryExpression instanceof Modulo) {
            FunctionInterface functionInterface = allFunctions.get("MOD");
            if (functionInterface != null) {
                return functionInterface.evaluate(Arrays.asList(leftValue, rightValue));
            }
        }
        if (binaryExpression instanceof BitwiseAnd) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() & rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() & rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() & rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() & rightValue.longValue();
            } else {
                throw new ExpressionException("BitwiseAnd operation only support for INTEGRAL type.");
            }
        }
        if (binaryExpression instanceof BitwiseOr) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() | rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() | rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() | rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() | rightValue.longValue();
            } else {
                throw new ExpressionException("BitwiseOr operation only support for INTEGRAL type.");
            }
        }
        if (binaryExpression instanceof BitwiseXor) {
            if (dataType == BasicType.BYTE_TYPE) {
                return leftValue.byteValue() ^ rightValue.byteValue();
            } else if (dataType == BasicType.SHORT_TYPE) {
                return leftValue.shortValue() ^ rightValue.shortValue();
            } else if (dataType == BasicType.INT_TYPE) {
                return leftValue.intValue() ^ rightValue.intValue();
            } else if (dataType == BasicType.LONG_TYPE) {
                return leftValue.longValue() ^ rightValue.longValue();
            } else {
                throw new ExpressionException("BitwiseXor operation only support for INTEGRAL type.");
            }
        }
        throw new ExpressionException(
                String.format("Unsupported SQL Expression: %s ", binaryExpression));
    }

    private static AresDataType<?> getMaxDataType(Number arg1, Number arg2) {
        AresDataType<?> type1 = AresDataTypeHelper.getAresDataType(arg1);
        AresDataType<?> type2 = AresDataTypeHelper.getAresDataType(arg2);
        return getMaxDataType(type1, type2);
    }

    private static AresDataType<?> getMaxDataType(AresDataType<?> type1, AresDataType<?> type2) {
        DecimalType decimalType = null;
        if (type1 instanceof DecimalType) {
            decimalType = new DecimalType(((DecimalType) type1).getPrecision(),
                    ((DecimalType) type1).getScale());
        }
        if (type2 instanceof DecimalType) {
            int precision = ((DecimalType) type2).getPrecision();
            int scale = ((DecimalType) type2).getScale();
            if (decimalType != null) {
                precision = Math.max(precision, decimalType.getPrecision());
                scale = Math.max(scale, decimalType.getScale());
                decimalType = new DecimalType(precision, scale);
            }
        }
        if (decimalType != null) {
            return decimalType;
        }
        List<AresDataType<?>> numberTypes = Arrays.asList(BasicType.BYTE_TYPE, BasicType.SHORT_TYPE, BasicType.INT_TYPE, BasicType.LONG_TYPE,
                BasicType.FLOAT_TYPE, BasicType.DOUBLE_TYPE);
        int idx1 = numberTypes.indexOf(type1);
        int idx2 = numberTypes.indexOf(type2);
        int idxMax = Math.max(idx1, idx2);
        return numberTypes.get(idxMax);
    }
}

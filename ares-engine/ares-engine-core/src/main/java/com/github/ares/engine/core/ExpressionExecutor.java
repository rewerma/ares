package com.github.ares.engine.core;

import static com.github.ares.engine.utils.EngineUtil.toEvalParams;

import com.github.ares.common.exceptions.AresException;
import com.github.ares.sql.expression.sql.ExpressionEngine;
import com.github.ares.sql.function.FunctionInterface;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.Getter;

public class ExpressionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    @Getter private ExpressionEngine expressionEngine;

    public void init(ExecutorManager executorManager) {
        expressionEngine = new ExpressionEngine();
        super.init(executorManager);
        expressionEngine.initDynamicFunctions(
                executorManager.getUdfManager().getDynamicFunctions());
    }

    public Serializable execute(String expr) {
        return execute(expr, null);
    }

    public Serializable execute(String expr, PlParams plParams) {
        String simpleSql = String.format("SELECT %s", expr);
        return (Serializable) expressionEngine.evaluate(simpleSql, toEvalParams(plParams));
    }

    public boolean execute4Bool(String expr) {
        return execute4Bool(expr, null);
    }

    public boolean execute4Bool(String expr, PlParams plParams) {
        String simpleSql = String.format("SELECT %s", expr);
        return expressionEngine.evaluateForBool(simpleSql, toEvalParams(plParams));
    }

    public Serializable execute4Hex(String expression) {
        return execute4Hex(expression, null);
    }

    public Serializable execute4Hex(String expression, PlParams plParams) {
        Serializable resVal;
        if (expression.startsWith("'") && expression.endsWith("'")) {
            resVal = execute(expression, plParams);
            resVal = rawToHex(resVal);
        } else {
            try {
                BigDecimal bigDecimalVal = new BigDecimal(expression);
                resVal = rawToHex(bigDecimalVal.longValue());
            } catch (NumberFormatException e) {
                resVal = execute(expression, plParams);
            }
        }
        return resVal;
    }

    public Object invokeFunction(String functionName, List<Object> args) {
        expressionEngine.initDynamicFunctions(
                executorManager.getUdfManager().getDynamicFunctions());
        FunctionInterface function =
                expressionEngine.getAllFunctions().get(functionName.toUpperCase());
        if (function == null) {
            throw new AresException(
                    String.format("Procedure or function undefined: %s", functionName));
        }
        return function.evaluate(args);
    }

    public static String rawToHex(Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof byte[]) {
            int len = ((byte[]) arg).length;
            byte[] bytes = new byte[len * 2];
            char[] hex = "0123456789ABCDEF".toCharArray();
            for (int i = 0, j = 0; i < len; i++) {
                int c = ((byte[]) arg)[i] & 0xff;
                bytes[j++] = (byte) hex[c >> 4];
                bytes[j++] = (byte) hex[c & 0xf];
            }
            return new String(bytes, StandardCharsets.ISO_8859_1);
        }
        String s = arg.toString();
        try {
            BigDecimal bd = new BigDecimal(s);
            return Long.toHexString(bd.longValue());
        } catch (NumberFormatException e) {
            // ignore
        }

        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        StringBuilder buff = new StringBuilder(4 * length);
        for (byte aByte : bytes) {
            String hex = Integer.toHexString(aByte & 0xff);
            for (int j = hex.length(); j < 2; j++) {
                buff.append('0');
            }
            buff.append(hex);
        }
        return buff.toString();
    }
}

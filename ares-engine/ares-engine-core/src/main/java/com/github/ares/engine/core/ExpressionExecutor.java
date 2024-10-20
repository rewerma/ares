package com.github.ares.engine.core;

import com.github.ares.sql.expression.sql.ExpressionEngine;
import lombok.Getter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class ExpressionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter
    private ExpressionEngine expressionEngine;

    public void init(ExecutorManager executorManager) {
        expressionEngine = new ExpressionEngine();
        super.init(executorManager);
    }

    public Serializable execute(String expr) {
        String simpleSql = String.format("SELECT %s", expr);
        return (Serializable) expressionEngine.evaluate(simpleSql);
    }

    public boolean execute4Bool(String expr) {
        String simpleSql = String.format("SELECT %s", expr);
        return expressionEngine.evaluateForBool(simpleSql);
    }

    public Serializable execute4Hex(String expression) {
        Serializable resVal;
        if (expression.startsWith("'")
                && expression.endsWith("'")) {
            resVal = execute(expression);
            resVal = rawToHex(resVal);
        } else {
            try {
                BigDecimal bigDecimalVal = new BigDecimal(expression);
                resVal = rawToHex(bigDecimalVal.longValue());
            } catch (NumberFormatException e) {
                resVal = execute(expression);
            }
        }
        return resVal;
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
            //ignore
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

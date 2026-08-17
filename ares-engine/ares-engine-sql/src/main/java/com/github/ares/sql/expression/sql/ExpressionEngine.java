package com.github.ares.sql.expression.sql;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.sql.expression.exception.ExpressionException;
import com.github.ares.sql.function.DynamicFunction;
import com.github.ares.sql.function.FunctionInterface;
import com.github.ares.sql.function.SparkFuncInterface;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ExpressionEngine implements Serializable {
    private static final long serialVersionUID = -1L;

    private Map<String, FunctionInterface> functions;

    private Map<String, DynamicFunction> dynamicFunctions;

    private final Map<String, FunctionInterface> allFunctions = new LinkedHashMap<>();

    private final SimpleSqlFunction simpleSqlFunction;

    private transient Map<String, PlainSelect> parsedSelects;

    public ExpressionEngine() {
        init();
        SimpleSqlType simpleSqlType = new SimpleSqlType(allFunctions);
        this.simpleSqlFunction = new SimpleSqlFunction(simpleSqlType, allFunctions);
    }

    public void init() {
        if (functions == null) {
            synchronized (ExpressionEngine.class) {
                if (functions == null) {
                    functions = new LinkedHashMap<>();
                    ClassLoader cl = Thread.currentThread().getContextClassLoader();
                    if (ExecutionEngineType.engineType == EngineType.SPARK) {
                        ServiceLoader.load(SparkFuncInterface.class, cl)
                                .forEach(
                                        functionInterface ->
                                                functions.put(
                                                        functionInterface
                                                                .functionName()
                                                                .toUpperCase(),
                                                        functionInterface));
                    }
                    allFunctions.putAll(functions);
                }
            }
        }
    }

    public void initDynamicFunctions(Map<String, DynamicFunction> dynamicFun) {
        if (dynamicFun == null) {
            return;
        }
        synchronized (ExpressionEngine.class) {
            for (DynamicFunction dynamicFunction : dynamicFun.values()) {
                String key = dynamicFunction.getFunctionName().toUpperCase();
                if (!allFunctions.containsKey(key)) {
                    allFunctions.put(
                            key, FunctionInterface.fromUdf(dynamicFunction.toUdfInterface()));
                }
            }
            if (dynamicFunctions == null) {
                dynamicFunctions = dynamicFun;
            }
        }
    }

    public Object evaluate(String sql) {
        return evaluate(sql, null);
    }

    public Object evaluate(String sql, Map<String, Object> params) {
        simpleSqlFunction.setParams(params);
        try {
            Object[] outputFields = project(new Object[] {}, parseSQL(sql));
            if (outputFields.length > 0) {
                return outputFields[0];
            }
            return null;
        } finally {
            simpleSqlFunction.setParams(null);
        }
    }

    public boolean evaluateForBool(String sql) {
        return evaluateForBool(sql, null);
    }

    public boolean evaluateForBool(String sql, Map<String, Object> params) {
        Object res = evaluate(sql, params);
        if (res instanceof Boolean) {
            return (Boolean) res;
        }
        return res instanceof Number && (((Number) res).intValue() > 0);
    }

    private PlainSelect parseSQL(String sql) {
        if (parsedSelects == null) {
            parsedSelects = new HashMap<>();
        }
        PlainSelect cached = parsedSelects.get(sql);
        if (cached != null) {
            return cached;
        }
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            PlainSelect selectBody = (PlainSelect) ((Select) statement).getSelectBody();
            parsedSelects.put(sql, selectBody);
            return selectBody;
        } catch (JSQLParserException e) {
            throw new ExpressionException(
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    private Object[] project(Object[] inputFields, PlainSelect selectBody) {
        List<SelectItem> selectItems = selectBody.getSelectItems();

        Object[] fields = new Object[selectItems.size()];

        int idx = 0;
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
                Expression expression = expressionItem.getExpression();
                fields[idx] = simpleSqlFunction.computeForValue(expression, inputFields);
            }
            idx++;
        }
        return fields;
    }

    public Map<String, FunctionInterface> getAllFunctions() {
        return allFunctions;
    }
}

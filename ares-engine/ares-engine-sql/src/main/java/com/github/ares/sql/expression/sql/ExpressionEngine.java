package com.github.ares.sql.expression.sql;

import com.github.ares.api.common.EngineType;
import com.github.ares.api.common.ExecutionEngineType;
import com.github.ares.sql.expression.exception.ExpressionException;
import com.github.ares.sql.function.DynamicFunction;
import com.github.ares.sql.function.FunctionInterface;
import com.github.ares.sql.function.SparkFuncInterface;
import com.github.ares.sql.function.UdfInterface;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class ExpressionEngine implements Serializable {
    private static final long serialVersionUID = -1L;

    private Map<String, FunctionInterface> functions;

    private Map<String, DynamicFunction> dynamicFunctions;

    private final Map<String, FunctionInterface> allFunctions = new LinkedHashMap<>();

    private final SimpleSqlFunction simpleSqlFunction;

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
                        ServiceLoader.load(SparkFuncInterface.class, cl).forEach(functionInterface ->
                                functions.put(functionInterface.functionName().toUpperCase(), functionInterface));
                    }
                    allFunctions.putAll(functions);
                }
            }
        }
    }

    public void initDynamicFunctions(Map<String, DynamicFunction> dynamicFun) {
        if (dynamicFunctions == null) {
            synchronized (ExpressionEngine.class) {
                if (dynamicFunctions == null) {
                    dynamicFunctions = dynamicFun;
                    List<UdfInterface> dynamicUdfList =
                            dynamicFunctions.values()
                                    .stream().map(DynamicFunction::toUdfInterface)
                                    .collect(Collectors.toList());
                    dynamicUdfList.forEach(udf ->
                            allFunctions.put(udf.functionName().toUpperCase(), FunctionInterface.fromUdf(udf)));
                }
            }
        }
    }

    public Object evaluate(String sql) {
        PlainSelect selectBody = parseSQL(sql);
        Object[] outputFields = project(new Object[]{}, selectBody);
        if (outputFields.length > 0) {
            return outputFields[0];
        }
        return null;
    }

    public boolean evaluateForBool(String sql) {
        PlainSelect selectBody = parseSQL(sql);
        Object[] outputFields = project(new Object[]{}, selectBody);
        if (outputFields.length > 0) {
            Object res = outputFields[0];
            if (res instanceof Boolean) {
                return (Boolean) res;
            }
            return res instanceof Number && (((Number) res).intValue() > 0);
        }
        return false;
    }

    private PlainSelect parseSQL(String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            return (PlainSelect) ((Select) statement).getSelectBody();
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

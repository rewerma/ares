package com.github.ares.engine.core;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.plan.LogicalReturnValue;

import java.io.Serializable;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

public class ReturnFunctionExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String RETURN_PARAM_NAME = "$function_return";

    public void execute(LogicalReturnValue returnValue, PlParams plParams) {
        traceLogger.info("Return: {}; Params: {}", returnValue.getExpr(), plParams);
        String expr = returnValue.getExpr();
        expr = replaceParams(expr, plParams);
        Serializable resVal;
        if (InternalFieldType.BYTES == returnValue.getPlType().getType()) {
            resVal = executorManager.getExpressionExecutor().execute4Hex(expr);
        } else {
            resVal = executorManager.getExpressionExecutor().execute(expr);
        }

        if (resVal == null) {
            plParams.put(RETURN_PARAM_NAME, null, returnValue.getPlType());
        } else {
            PlType fieldType = returnValue.getPlType();
            resVal = DataTypeConvertor.convert(null, fieldType, resVal);
            plParams.put(RETURN_PARAM_NAME, resVal, returnValue.getPlType());
        }
    }
}

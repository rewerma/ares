package com.github.ares.engine.core;

import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalReturnValue;
import java.io.Serializable;

public class ReturnFunctionExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    public static final String RETURN_PARAM_NAME = "$function_return";

    @Override
    public OperationType handledType() {
        return RETURN_VALUE;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalReturnValue) operation, plParams);
        return RETURN_VALUE;
    }

    public void execute(LogicalReturnValue returnValue, PlParams plParams) {
        traceLogger.info("Return: {}; Params: {}", returnValue.getExpr(), plParams);
        String expr = returnValue.getExpr();
        Serializable resVal;
        if (InternalFieldType.BYTES == returnValue.getPlType().getType()) {
            resVal = executorManager.getExpressionExecutor().execute4Hex(expr, plParams);
        } else {
            resVal = executorManager.getExpressionExecutor().execute(expr, plParams);
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

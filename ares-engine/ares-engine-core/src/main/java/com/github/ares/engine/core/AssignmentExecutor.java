package com.github.ares.engine.core;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalAssignment;

import java.io.Serializable;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;
import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

public class AssignmentExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalAssignment assignment, PlParams plParams) {
        if (!plParams.containsKey(assignment.getParam().getName())) {
            throw new AresException(String.format("Parameter is not defined: %s",
                    assignment.getParam().getName()));
        }
        traceLogger.info("Assignment: {} {} := {}",
                assignment.getParam().getName(), getOriginalType(assignment.getParam().getPlType()),
                assignment.getExpr());
        assignment(assignment.getParam(), assignment.getExpr(), plParams);
    }

    public void assignment(Argument argument, String expression, PlParams plParams) {
        Serializable resVal = null;
        if (expression != null) {
            String expr = replaceParams(expression, plParams);
            if (InternalFieldType.BYTES == argument.getPlType().getType()) {
                resVal = executorManager.getExpressionExecutor().execute4Hex(expr);
            } else {
                resVal = executorManager.getExpressionExecutor().execute(expr);
            }
        }
        if (resVal == null) {
            plParams.put(argument.getName(), null, argument.getPlType());
        } else {
            resVal = DataTypeConvertor.convertWithIdentifier(argument.getName(), argument.getPlType(), resVal);
            plParams.put(argument.getName(), resVal, argument.getPlType());
        }
    }

}

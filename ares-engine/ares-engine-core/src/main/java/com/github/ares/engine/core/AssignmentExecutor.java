package com.github.ares.engine.core;

import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalAssignment;
import com.github.ares.parser.plan.LogicalOperation;
import java.io.Serializable;

public class AssignmentExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.ASSIGNMENT;
    }

    @Override
    public Scope scope() {
        return Scope.BODY;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalAssignment) operation, plParams);
        return lastData;
    }

    public void execute(LogicalAssignment assignment, PlParams plParams) {
        if (!plParams.containsKey(assignment.getParam().getName())) {
            throw new AresException(
                    String.format("Parameter is not defined: %s", assignment.getParam().getName()));
        }
        traceLogger.info(
                "Assignment: {} {} := {}",
                assignment.getParam().getName(),
                getOriginalType(assignment.getParam().getPlType()),
                assignment.getExpr());
        assignment(assignment.getParam(), assignment.getExpr(), plParams);
    }

    public void assignment(Argument argument, String expression, PlParams plParams) {
        Serializable resVal = null;
        if (expression != null) {
            if (InternalFieldType.BYTES == argument.getPlType().getType()) {
                resVal = executorManager.getExpressionExecutor().execute4Hex(expression, plParams);
            } else {
                resVal = executorManager.getExpressionExecutor().execute(expression, plParams);
            }
        }
        if (resVal == null) {
            plParams.put(argument.getName(), null, argument.getPlType());
        } else {
            resVal =
                    DataTypeConvertor.convertWithIdentifier(
                            argument.getName(), argument.getPlType(), resVal);
            plParams.put(argument.getName(), resVal, argument.getPlType());
        }
    }
}

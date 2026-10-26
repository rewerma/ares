package com.github.ares.engine.core;

import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalDeclareParams;
import java.io.Serializable;

public class DeclareParamsExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void execute(LogicalDeclareParams declareParams, PlParams plParams) {
        for (Argument argument : declareParams.getDeclareParams()) {
            traceLogger.info(
                    "Declare: {} {} := {}",
                    argument.getName(),
                    getOriginalType(argument.getPlType()),
                    argument.getDefaultVal());
            executorManager
                    .getAssignmentExecutor()
                    .assignment(argument, argument.getDefaultVal(), plParams);
        }
    }
}

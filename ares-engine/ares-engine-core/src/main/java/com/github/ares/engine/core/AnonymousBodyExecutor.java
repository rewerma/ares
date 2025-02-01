package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalAnonymousBody;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;

public class AnonymousBodyExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    @Inject
    private ExceptionMessageHandler exceptionMessageHandler;

    public Object execute(LogicalAnonymousBody anonymousBody) {
        traceLogger.info("Anonymous body: BEGIN");
        PlParams plParams = new PlParams();
        LogicalDeclareParams declareOperation = anonymousBody.getDeclareParams();
        if (declareOperation != null) {
            executorManager.getDeclareParamsExecutor().execute(declareOperation, plParams);
        }
        Object result = null;
        LogicalExceptionHandler exHandler = anonymousBody.getExHandler();
        if (exHandler != null) {
            try {
                result = executorManager.getBodyExecutionExecutor().execute(anonymousBody.getAnonymousBody(), plParams);
            } catch (Exception e) {
                PlParams plParamsCopy = new PlParams(plParams.getAllParams(), plParams.getParamTypes());
                String message = exceptionMessageHandler.getMessage(e);
                message = handleQuoteIdentifier(message);
                plParamsCopy.put("ex.message", message, PlType.of(InternalFieldType.VARCHAR));
                executorManager.getBodyExecutionExecutor().execute(exHandler.getExHandlerBody(), plParamsCopy);
                if (exHandler.getWithRaise() != null && exHandler.getWithRaise()) {
                    throw new AresException(e);
                }
            }
        } else {
            result = executorManager.getBodyExecutionExecutor().execute(anonymousBody.getAnonymousBody(), plParams);
        }
        traceLogger.info("Anonymous body: END");
        return result;
    }
}

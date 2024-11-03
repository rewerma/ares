package com.github.ares.engine.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCreateProcedure;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;

public class CreateProcedureExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    @Inject
    private ExceptionMessageHandler exceptionMessageHandler;

    public void execute(LogicalCreateProcedure createProcedure) {
        Pair<String, CreateProcedureFunc> procedureFunc = createProcedure(createProcedure, executorManager.getBodyExecutionExecutor()::execute);
        String procedureName = procedureFunc.getKey().toLowerCase();
        if (executorManager.getProcedures().containsKey(procedureName) || executorManager.getFunctions().containsKey(procedureName)) {
            throw new AresException(String.format("Procedure or function name exists: %s", createProcedure.getProcedureName()));
        }
        executorManager.getProcedures().put(procedureName, procedureFunc.getValue());
    }

    public Pair<String, CreateProcedureFunc> createProcedure(LogicalCreateProcedure createProcedure, BodyCallback body) {
        traceLogger.info("Create procedure: {} ( {} )", createProcedure.getProcedureName(), createProcedure.getArgsString());
        Map<String, Argument> prodInArgs = new LinkedHashMap<>();
        createProcedure.getInArgs().forEach(arg -> prodInArgs.put(arg.getName(), arg));
        CreateProcedureFunc procedureFunc = new CreateProcedureFunc() {
            private static final long serialVersionUID = 1;

            private List<Serializable> evaluateOrigin(PlParams plParams) {
                body.invoke(createProcedure.getProcedureBody(), plParams);
                List<Serializable> outArgs = new ArrayList<>();
                createProcedure.getOutArgs().forEach(outArg -> {
                    Serializable value = plParams.get(outArg.getName());
                    outArgs.add(value);
                });
                return outArgs;
            }

            @Override
            public List<Serializable> evaluate(List<Serializable> args) {
                traceLogger.info("Procedure: {} ( {} ) BEGIN",
                        createProcedure.getProcedureName(), createProcedure.getArgsString());
                if (args.size() != prodInArgs.size()) {
                    throw new AresException(String.format(
                            "The number of parameters of the procedure '%s' does not match",
                            createProcedure.getProcedureName()));
                }
                PlParams plParams = new PlParams();
                // handle procedure declare args
                LogicalDeclareParams declareOperation = createProcedure.getDeclareParams();
                if (declareOperation != null) {
                    executorManager.declareParamsExecutor.execute(declareOperation, plParams);
                }

                // handle procedure in args
                PlParams prodArgValues = new PlParams(new LinkedHashMap<>(plParams.getAllParams()), new LinkedHashMap<>(plParams.getParamTypes()));
                int i = 0;
                for (Map.Entry<String, Argument> entry : prodInArgs.entrySet()) {
                    String argName = entry.getKey();
                    Argument arg = entry.getValue();
                    Serializable value = args.get(i);
                    value = DataTypeConvertor.convertWithIdentifier(arg.getName(), arg.getPlType(), value);
                    prodArgValues.put(argName, value, arg.getPlType());
                    i++;
                }
                createProcedure.getOutArgs().forEach(outArg -> prodArgValues.put(outArg.getName(), null, outArg.getPlType()));

                List<Serializable> result;
                LogicalExceptionHandler exHandler = createProcedure.getExHandler();
                if (exHandler != null) {
                    try {
                        result = evaluateOrigin(prodArgValues);
                    } catch (Exception e) {
                        String message = exceptionMessageHandler.getMessage(e);
                        message = handleQuoteIdentifier(message);
                        prodArgValues.put("ex.message", message, PlType.of(InternalFieldType.VARCHAR));
                        body.invoke(exHandler.getExHandlerBody(), prodArgValues);
                        if (exHandler.getWithRaise() != null && exHandler.getWithRaise()) {
                            throw new AresException(e);
                        }
                        List<Serializable> outArgs = new ArrayList<>();
                        createProcedure.getOutArgs().forEach(outArg -> {
                            Serializable value = prodArgValues.get(outArg.getName());
                            outArgs.add(value);
                        });
                        result = outArgs;
                    }
                } else {
                    result = evaluateOrigin(prodArgValues);
                }
                traceLogger.info("Procedure: {} END", createProcedure.getProcedureName());
                return result;
            }

            @Override
            public LogicalCreateProcedure getCreateMethod() {
                return createProcedure;
            }
        };

        return Pair.of(createProcedure.getProcedureName().toLowerCase(), procedureFunc);
    }
}

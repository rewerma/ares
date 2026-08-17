package com.github.ares.engine.core;

import static com.github.ares.api.table.type.AresDataTypeHelper.getAresDataType;
import static com.github.ares.engine.core.ReturnFunctionExecutor.RETURN_PARAM_NAME;
import static com.github.ares.parser.utils.PLParserUtil.getOriginalType;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.utils.DataTypeConvertor;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalCreateFunction;
import com.github.ares.parser.plan.LogicalDeclareParams;
import com.github.ares.parser.plan.LogicalExceptionHandler;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.sql.function.DynamicFunction;
import com.github.ares.sql.function.Function;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public abstract class CreateFunctionExecutor extends AbstractBaseExecutor
        implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Inject private ExceptionMessageHandler exceptionMessageHandler;

    @Override
    public OperationType handledType() {
        return OperationType.CREATE_FUNCTION;
    }

    @Override
    public Scope scope() {
        return Scope.PROJECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        execute((LogicalCreateFunction) operation);
        return lastData;
    }

    public void execute(LogicalCreateFunction createFunction) {
        Pair<String, CreateProcedureFunc> procedureFunc =
                createFunction(createFunction, executorManager.getBodyExecutionExecutor()::execute);
        String functionName = procedureFunc.getKey().toLowerCase();
        if (executorManager.getFunctions().containsKey(functionName)
                || executorManager.getProcedures().containsKey(functionName)) {
            throw new AresException(
                    String.format(
                            "Procedure or function name exists: %s",
                            createFunction.getFunctionName()));
        }
        executorManager.getFunctions().put(functionName, procedureFunc.getValue());
    }

    public Pair<String, CreateProcedureFunc> createFunction(
            LogicalCreateFunction createFunction, BodyCallback body) {
        traceLogger.info(
                "Create function: {} ( {} ) RETURN {}",
                createFunction.getFunctionName(),
                createFunction.getArgsString(),
                getOriginalType(createFunction.getReturnType()));
        Map<String, Argument> prodInArgs = new LinkedHashMap<>();
        createFunction.getInArgs().forEach(arg -> prodInArgs.put(arg.getName(), arg));
        CreateProcedureFunc procedureFunc =
                new CreateProcedureFunc() {
                    private static final long serialVersionUID = 1;

                    @Override
                    public List<Serializable> evaluate(List<Serializable> args) {
                        traceLogger.info(
                                "Function: {} ( {} ) BEGIN",
                                createFunction.getFunctionName(),
                                createFunction.getArgsString());
                        if (args.size() != prodInArgs.size()) {
                            throw new AresException(
                                    String.format(
                                            "The number of parameters of the procedure '%s' does not match",
                                            createFunction.getFunctionName()));
                        }
                        PlParams plParams = new PlParams();

                        // handle procedure declare args
                        LogicalDeclareParams declareOperation = createFunction.getDeclareParams();
                        if (declareOperation != null) {
                            executorManager
                                    .getDeclareParamsExecutor()
                                    .execute(declareOperation, plParams);
                        }

                        // handle procedure in args
                        PlParams prodArgValues =
                                new PlParams(
                                        new LinkedHashMap<>(plParams.getAllParams()),
                                        new LinkedHashMap<>(plParams.getParamTypes()));
                        int i = 0;
                        for (Map.Entry<String, Argument> entry : prodInArgs.entrySet()) {
                            String argName = entry.getKey();
                            Argument arg = entry.getValue();
                            Serializable value = args.get(i);
                            value =
                                    DataTypeConvertor.convertWithIdentifier(
                                            arg.getName(), arg.getPlType(), value);
                            prodArgValues.put(argName, value, arg.getPlType());
                            i++;
                        }

                        List<Serializable> outArgs = new ArrayList<>();
                        LogicalExceptionHandler exHandler = createFunction.getExHandler();
                        outArgs.add(
                                PlExceptionHandler.run(
                                        executorManager,
                                        exceptionMessageHandler,
                                        exHandler,
                                        prodArgValues,
                                        body,
                                        () -> {
                                            body.invoke(
                                                    createFunction.getFunctionBody(),
                                                    prodArgValues);
                                            return (Serializable)
                                                    prodArgValues.remove(RETURN_PARAM_NAME);
                                        },
                                        () ->
                                                (Serializable)
                                                        prodArgValues.remove(RETURN_PARAM_NAME)));
                        traceLogger.info(
                                "Function: {} END, RETURN {}",
                                createFunction.getFunctionName(),
                                outArgs.get(0));
                        return outArgs;
                    }

                    @Override
                    public LogicalCreateFunction getCreateMethod() {
                        return createFunction;
                    }
                };

        // Register function
        DynamicFunction dynamicFunction = new DynamicFunction();
        dynamicFunction.setFunctionName(createFunction.getFunctionName());
        dynamicFunction.setResultType(getAresDataType(createFunction.getReturnType()));
        List<AresDataType<?>> inTypes = new ArrayList<>();
        if (createFunction.getInArgs() != null) {
            for (Argument arg : createFunction.getInArgs()) {
                inTypes.add(getAresDataType(arg.getPlType()));
            }
        }
        dynamicFunction.setArgTypes(inTypes);
        Function function =
                args -> {
                    List<Serializable> arguments =
                            args.stream()
                                    .map(arg -> (Serializable) arg)
                                    .collect(Collectors.toList());

                    return procedureFunc.evaluate(arguments).get(0);
                };
        dynamicFunction.setFunction(function);
        executorManager.getUdfManager().registerDynamicFunction(dynamicFunction);

        registerFunction(dynamicFunction);

        return Pair.of(createFunction.getFunctionName().toLowerCase(), procedureFunc);
    }

    public abstract void registerFunction(DynamicFunction dynamicFunction);
}

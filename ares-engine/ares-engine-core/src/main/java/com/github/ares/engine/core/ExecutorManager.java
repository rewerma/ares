package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.parser.config.PlProperties;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class ExecutorManager implements Serializable {
    private static final long serialVersionUID = -1L;

    protected final Map<String, CreateProcedureFunc> functions = new LinkedHashMap<>();

    protected final Map<String, CreateProcedureFunc> procedures = new LinkedHashMap<>();

    protected final Map<String, Optional<? extends Factory>> sinkPlugins = new LinkedHashMap<>();

    protected final Map<String, SourceTableInfo> sourceTables = new LinkedHashMap<>();

    protected final OperationDispatcher operationDispatcher = new OperationDispatcher();

    private PlProperties plProperties;

    @Inject protected TraceLogger traceLogger;

    @Inject protected UdfManager udfManager;

    @Inject protected ProjectExecutor projectExecutor;

    @Inject protected SinkPluginManager sinkPluginManager;

    @Inject protected BodyExecutionExecutor bodyExecutionExecutor;
    @Inject protected DirectExecutionExecutor directExecutionExecutor;
    @Inject protected AnonymousBodyExecutor anonymousBodyExecutor;
    @Inject protected ExpressionExecutor expressionExecutor;
    @Inject protected AssignmentExecutor assignmentExecutor;
    @Inject protected IfElseExecutor ifElseExecutor;
    @Inject protected WhileLoopExecutor whileLoopExecutor;
    @Inject protected ForLoopExecutor forLoopExecutor;
    @Inject protected ForCursorLoopExecutor forCursorLoopExecutor;
    @Inject protected SelectIntoSqlExecutor selectIntoSqlExecutor;
    @Inject protected ReturnFunctionExecutor returnFunctionExecutor;
    @Inject protected CallFunctionExecutor callFunctionExecutor;
    @Inject protected CallProcedureExecutor callProcedureExecutor;
    @Inject protected CreateFunctionExecutor createFunctionExecutor;
    @Inject protected CreateProcedureExecutor createProcedureExecutor;
    @Inject protected CreateSinkTableExecutor createSinkTableExecutor;
    @Inject protected CreateSourceTableExecutor createSourceTableExecutor;
    @Inject protected CreateTableAsSqlExecutor createTableAsSqlExecutor;
    @Inject protected DeclareParamsExecutor declareParamsExecutor;

    @Inject protected InsertSelectSqlExecutor insertSelectSqlExecutor;
    @Inject protected UpdateSelectSqlExecutor updateSelectSqlExecutor;
    @Inject protected DeleteSelectSqlExecutor deleteSelectSqlExecutor;
    @Inject protected MergeIntoSqlExecutor mergeIntoSqlExecutor;
    @Inject protected SelectSqlExecutor selectSqlExecutor;
    @Inject protected TruncateSqlExecutor truncateSqlExecutor;

    @Inject protected ReloadFunctionExecutor reloadFunctionExecutor;

    protected final PlTransactionManager transactionManager = new PlTransactionManager();

    @Inject protected StartTransactionExecutor startTransactionExecutor;
    @Inject protected CommitExecutor commitExecutor;
    @Inject protected RollbackExecutor rollbackExecutor;

    public void init(PlProperties plProperties) {
        this.plProperties = plProperties;
        traceLogger.init(plProperties);
        initInjectedExecutors();
        registerOperationHandlers();
    }

    private void initInjectedExecutors() {
        forEachInjected(
                value -> {
                    if (value instanceof AbstractBaseExecutor) {
                        ((AbstractBaseExecutor) value).init(this);
                    }
                });
    }

    private void registerOperationHandlers() {
        forEachInjected(
                value -> {
                    if (value instanceof OperationHandler) {
                        operationDispatcher.register((OperationHandler) value);
                    }
                });
        operationDispatcher.registerBuiltins(traceLogger);
    }

    private void forEachInjected(java.util.function.Consumer<Object> consumer) {
        Class<?> type = getClass();
        while (type != null && ExecutorManager.class.isAssignableFrom(type)) {
            for (Field field : type.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                Object value;
                try {
                    value = field.get(this);
                } catch (IllegalAccessException e) {
                    throw new AresException(e);
                }
                if (value != null) {
                    consumer.accept(value);
                }
            }
            type = type.getSuperclass();
        }
    }
}

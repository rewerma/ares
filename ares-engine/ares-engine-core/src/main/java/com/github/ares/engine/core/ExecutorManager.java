package com.github.ares.engine.core;

import com.github.ares.api.source.SourceTableInfo;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.parser.config.PlProperties;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Getter
public class ExecutorManager implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Map<String, CreateProcedureFunc> functions = new LinkedHashMap<>();

    protected final Map<String, CreateProcedureFunc> procedures = new LinkedHashMap<>();

    protected final Map<String, Optional<? extends Factory>> sinkPlugins = new LinkedHashMap<>();

    protected final Map<String, SourceTableInfo> sourceTables = new LinkedHashMap<>();

    private PlProperties plProperties;

    @Inject
    protected TraceLogger traceLogger;

    @Inject
    protected UdfManager udfManager;

    @Inject
    protected ProjectExecutor projectExecutor;

    @Inject
    protected SinkPluginManager sinkPluginManager;

    @Inject
    protected BodyExecutionExecutor bodyExecutionExecutor;
    @Inject
    protected DirectExecutionExecutor directExecutionExecutor;
    @Inject
    protected AnonymousBodyExecutor anonymousBodyExecutor;
    @Inject
    protected ExpressionExecutor expressionExecutor;
    @Inject
    protected AssignmentExecutor assignmentExecutor;
    @Inject
    protected IfElseExecutor ifElseExecutor;
    @Inject
    protected WhileLoopExecutor whileLoopExecutor;
    @Inject
    protected ForLoopExecutor forLoopExecutor;
    @Inject
    protected ForCursorLoopExecutor forCursorLoopExecutor;
    @Inject
    protected SelectIntoSqlExecutor selectIntoSqlExecutor;
    @Inject
    protected ReturnFunctionExecutor returnFunctionExecutor;
    @Inject
    protected CallFunctionExecutor callFunctionExecutor;
    @Inject
    protected CallProcedureExecutor callProcedureExecutor;
    @Inject
    protected CreateFunctionExecutor createFunctionExecutor;
    @Inject
    protected CreateProcedureExecutor createProcedureExecutor;
    @Inject
    protected CreateSinkTableExecutor createSinkTableExecutor;
    @Inject
    protected CreateSourceTableExecutor createSourceTableExecutor;
    @Inject
    protected CreateTableAsSqlExecutor createTableAsSqlExecutor;
    @Inject
    protected DeclareParamsExecutor declareParamsExecutor;

    @Inject
    protected InsertSelectSqlExecutor insertSelectSqlExecutor;
    @Inject
    protected UpdateSelectSqlExecutor updateSelectSqlExecutor;
    @Inject
    protected DeleteSelectSqlExecutor deleteSelectSqlExecutor;
    @Inject
    protected MergeIntoSqlExecutor mergeIntoSqlExecutor;
    @Inject
    protected SelectSqlExecutor selectSqlExecutor;
    @Inject
    protected TruncateSqlExecutor truncateSqlExecutor;

    @Inject
    protected ReloadFunctionExecutor reloadFunctionExecutor;

    public void init(PlProperties plProperties) {
        this.plProperties = plProperties;
        traceLogger.init(plProperties);
        udfManager.init(this);

        projectExecutor.init(this);
        sinkPluginManager.init(this);
        expressionExecutor.init(this);
        bodyExecutionExecutor.init(this);
        directExecutionExecutor.init(this);
        anonymousBodyExecutor.init(this);
        assignmentExecutor.init(this);
        ifElseExecutor.init(this);
        insertSelectSqlExecutor.init(this);
        whileLoopExecutor.init(this);
        forLoopExecutor.init(this);
        forCursorLoopExecutor.init(this);
        selectIntoSqlExecutor.init(this);
        returnFunctionExecutor.init(this);
        callFunctionExecutor.init(this);
        callProcedureExecutor.init(this);
        createFunctionExecutor.init(this);
        createProcedureExecutor.init(this);
        createSinkTableExecutor.init(this);
        createSourceTableExecutor.init(this);
        createTableAsSqlExecutor.init(this);
        declareParamsExecutor.init(this);
        updateSelectSqlExecutor.init(this);
        deleteSelectSqlExecutor.init(this);
        mergeIntoSqlExecutor.init(this);
        selectSqlExecutor.init(this);
        truncateSqlExecutor.init(this);

        reloadFunctionExecutor.init(this);
    }

}

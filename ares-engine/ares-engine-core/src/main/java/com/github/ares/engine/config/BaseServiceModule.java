package com.github.ares.engine.config;

import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.engine.core.AnonymousBodyExecutor;
import com.github.ares.engine.core.AresSinkFactory;
import com.github.ares.engine.core.AssignmentExecutor;
import com.github.ares.engine.core.BodyExecutionExecutor;
import com.github.ares.engine.core.CallFunctionExecutor;
import com.github.ares.engine.core.CallProcedureExecutor;
import com.github.ares.engine.core.CreateProcedureExecutor;
import com.github.ares.engine.core.CreateSinkTableExecutor;
import com.github.ares.engine.core.DeclareParamsExecutor;
import com.github.ares.engine.core.DirectExecutionExecutor;
import com.github.ares.engine.core.ExpressionExecutor;
import com.github.ares.engine.core.ForLoopExecutor;
import com.github.ares.engine.core.IfElseExecutor;
import com.github.ares.engine.core.ProjectExecutor;
import com.github.ares.engine.core.ReloadFunctionExecutor;
import com.github.ares.engine.core.ReturnFunctionExecutor;
import com.github.ares.engine.core.SinkPluginManager;
import com.github.ares.engine.core.TraceLogger;
import com.github.ares.engine.core.TruncateSqlExecutor;
import com.github.ares.engine.core.WhileLoopExecutor;

public class BaseServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(TraceLogger.class).in(Singleton.class);
        bind(ProjectExecutor.class).in(Singleton.class);
        bind(SinkPluginManager.class).in(Singleton.class);
        bind(BodyExecutionExecutor.class).in(Singleton.class);
        bind(DirectExecutionExecutor.class).in(Singleton.class);
        bind(AnonymousBodyExecutor.class).in(Singleton.class);
        bind(ExpressionExecutor.class).in(Singleton.class);
        bind(AssignmentExecutor.class).in(Singleton.class);
        bind(IfElseExecutor.class).in(Singleton.class);
        bind(WhileLoopExecutor.class).in(Singleton.class);
        bind(ForLoopExecutor.class).in(Singleton.class);
        bind(ReturnFunctionExecutor.class).in(Singleton.class);
        bind(CallFunctionExecutor.class).in(Singleton.class);
        bind(CallProcedureExecutor.class).in(Singleton.class);
        bind(CreateProcedureExecutor.class).in(Singleton.class);
        bind(CreateSinkTableExecutor.class).in(Singleton.class);
        bind(DeclareParamsExecutor.class).in(Singleton.class);
        bind(TruncateSqlExecutor.class).in(Singleton.class);
        bind(ReloadFunctionExecutor.class).in(Singleton.class);
        bind(AresSinkFactory.class).in(Singleton.class);
    }
}

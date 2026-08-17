package com.github.ares.engine.spark.config;

import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.engine.core.AbstractRootExecutor;
import com.github.ares.engine.core.CreateFunctionExecutor;
import com.github.ares.engine.core.CreateSourceTableExecutor;
import com.github.ares.engine.core.CreateTableAsSqlExecutor;
import com.github.ares.engine.core.DeleteSelectSqlExecutor;
import com.github.ares.engine.core.ExceptionMessageHandler;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.ForCursorLoopExecutor;
import com.github.ares.engine.core.InsertSelectSqlExecutor;
import com.github.ares.engine.core.MergeIntoSqlExecutor;
import com.github.ares.engine.core.SelectIntoSqlExecutor;
import com.github.ares.engine.core.SelectSqlExecutor;
import com.github.ares.engine.core.UdfManager;
import com.github.ares.engine.core.UpdateSelectSqlExecutor;
import com.github.ares.engine.spark.core.MainExecutor;
import com.github.ares.engine.spark.core.SparkCommonExecutor;
import com.github.ares.engine.spark.core.SparkCreateFunctionExecutor;
import com.github.ares.engine.spark.core.SparkCreateSourceTableExecutor;
import com.github.ares.engine.spark.core.SparkCreateTableAsSqlExecutor;
import com.github.ares.engine.spark.core.SparkDeleteSelectSqlExecutor;
import com.github.ares.engine.spark.core.SparkExceptionMessageHandler;
import com.github.ares.engine.spark.core.SparkExecutorManager;
import com.github.ares.engine.spark.core.SparkForCursorLoopExecutor;
import com.github.ares.engine.spark.core.SparkInsertSelectSqlExecutor;
import com.github.ares.engine.spark.core.SparkMergeIntoSqlExecutor;
import com.github.ares.engine.spark.core.SparkRootExecutor;
import com.github.ares.engine.spark.core.SparkSelectIntoSqlExecutor;
import com.github.ares.engine.spark.core.SparkSelectSqlExecutor;
import com.github.ares.engine.spark.core.SparkSessionManager;
import com.github.ares.engine.spark.core.SparkSinkExecutor;
import com.github.ares.engine.spark.core.SparkUdfManager;
import com.github.ares.engine.spark.core.SparkUpdateSelectSqlManager;

public class SparkServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AbstractRootExecutor.class).to(SparkRootExecutor.class).in(Singleton.class);
        bind(ExecutorManager.class).to(SparkExecutorManager.class).in(Singleton.class);

        bind(CreateSourceTableExecutor.class)
                .to(SparkCreateSourceTableExecutor.class)
                .in(Singleton.class);
        bind(SelectSqlExecutor.class).to(SparkSelectSqlExecutor.class).in(Singleton.class);
        bind(CreateTableAsSqlExecutor.class)
                .to(SparkCreateTableAsSqlExecutor.class)
                .in(Singleton.class);
        bind(SelectIntoSqlExecutor.class).to(SparkSelectIntoSqlExecutor.class).in(Singleton.class);
        bind(InsertSelectSqlExecutor.class)
                .to(SparkInsertSelectSqlExecutor.class)
                .in(Singleton.class);
        bind(UpdateSelectSqlExecutor.class)
                .to(SparkUpdateSelectSqlManager.class)
                .in(Singleton.class);
        bind(DeleteSelectSqlExecutor.class)
                .to(SparkDeleteSelectSqlExecutor.class)
                .in(Singleton.class);
        bind(MergeIntoSqlExecutor.class).to(SparkMergeIntoSqlExecutor.class).in(Singleton.class);
        bind(ForCursorLoopExecutor.class).to(SparkForCursorLoopExecutor.class).in(Singleton.class);
        bind(UdfManager.class).to(SparkUdfManager.class).in(Singleton.class);
        bind(CreateFunctionExecutor.class)
                .to(SparkCreateFunctionExecutor.class)
                .in(Singleton.class);
        bind(ExceptionMessageHandler.class)
                .to(SparkExceptionMessageHandler.class)
                .in(Singleton.class);

        bind(MainExecutor.class).in(Singleton.class);
        bind(SparkSessionManager.class).in(Singleton.class);
        bind(SparkSinkExecutor.class).in(Singleton.class);
        bind(SparkCommonExecutor.class).in(Singleton.class);
    }
}

package com.github.ares.parser.config;

import com.github.ares.com.google.inject.AbstractModule;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.parser.PlParser;
import com.github.ares.parser.model.SourceSinkTable;
import com.github.ares.parser.visitor.PlAssignmentVisitor;
import com.github.ares.parser.visitor.PlBaseVisitor;
import com.github.ares.parser.visitor.PlBodyVisitor;
import com.github.ares.parser.visitor.PlCallStatementVisitor;
import com.github.ares.parser.visitor.PlCreateAsSQLVisitor;
import com.github.ares.parser.visitor.PlCreateFunctionVisitor;
import com.github.ares.parser.visitor.PlCreateProcedureVisitor;
import com.github.ares.parser.visitor.PlCreateSinkTableVisitor;
import com.github.ares.parser.visitor.PlCreateSourceTableVisitor;
import com.github.ares.parser.visitor.PlCreateTableWithVisitor;
import com.github.ares.parser.visitor.PlDataTypePrecisionVisitor;
import com.github.ares.parser.visitor.PlDeclareParamsVisitor;
import com.github.ares.parser.visitor.PlDeleteSQLVisitor;
import com.github.ares.parser.visitor.PlExceptionHandlerVisitor;
import com.github.ares.parser.visitor.PlExpressionVisitor;
import com.github.ares.parser.visitor.PlFunctionBodyVisitor;
import com.github.ares.parser.visitor.PlIfStatementVisitor;
import com.github.ares.parser.visitor.PlInsertSQLVisitor;
import com.github.ares.parser.visitor.PlLoopStatementVisitor;
import com.github.ares.parser.visitor.PlMergeSQLVisitor;
import com.github.ares.parser.visitor.PlReturnStatementVisitor;
import com.github.ares.parser.visitor.PlSelectSQLVisitor;
import com.github.ares.parser.visitor.PlStatementVisitor;
import com.github.ares.parser.visitor.PlTruncateSQLVisitor;
import com.github.ares.parser.visitor.PlUpdateSQLVisitor;
import com.github.ares.parser.visitor.PlVisitorManager;

public class ParserServiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PlParser.class).in(Singleton.class);
        bind(PlVisitorManager.class).in(Singleton.class);
        bind(SourceSinkTable.class).in(Singleton.class);
        bind(PlProperties.class).in(Singleton.class);

        bind(PlStatementVisitor.class).in(Singleton.class);
        bind(PlBaseVisitor.class).in(Singleton.class);
        bind(PlBodyVisitor.class).in(Singleton.class);
        bind(PlFunctionBodyVisitor.class).in(Singleton.class);
        bind(PlCreateProcedureVisitor.class).in(Singleton.class);
        bind(PlCreateFunctionVisitor.class).in(Singleton.class);
        bind(PlDeclareParamsVisitor.class).in(Singleton.class);
        bind(PlCreateTableWithVisitor.class).in(Singleton.class);
        bind(PlCreateSourceTableVisitor.class).in(Singleton.class);
        bind(PlCreateSinkTableVisitor.class).in(Singleton.class);
        bind(PlCallStatementVisitor.class).in(Singleton.class);
        bind(PlAssignmentVisitor.class).in(Singleton.class);
        bind(PlExpressionVisitor.class).in(Singleton.class);
        bind(PlSelectSQLVisitor.class).in(Singleton.class);
        bind(PlInsertSQLVisitor.class).in(Singleton.class);
        bind(PlUpdateSQLVisitor.class).in(Singleton.class);
        bind(PlDeleteSQLVisitor.class).in(Singleton.class);
        bind(PlMergeSQLVisitor.class).in(Singleton.class);
        bind(PlCreateAsSQLVisitor.class).in(Singleton.class);
        bind(PlTruncateSQLVisitor.class).in(Singleton.class);
        bind(PlIfStatementVisitor.class).in(Singleton.class);
        bind(PlLoopStatementVisitor.class).in(Singleton.class);
        bind(PlReturnStatementVisitor.class).in(Singleton.class);
        bind(PlExceptionHandlerVisitor.class).in(Singleton.class);
        bind(PlDataTypePrecisionVisitor.class).in(Singleton.class);
    }
}

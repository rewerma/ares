package com.github.ares.parser.visitor;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.parser.config.PlProperties;
import com.github.ares.parser.model.SourceSinkTable;
import lombok.Getter;

@Getter
public class PlVisitorManager {
    private PlProperties plProperties;

    @Inject
    private SourceSinkTable sourceSinkTable;

    @Inject
    private PlStatementVisitor statementVisitor;
    @Inject
    private PlBaseVisitor baseVisitor;
    @Inject
    private PlBodyVisitor bodyVisitor;
    @Inject
    private PlFunctionBodyVisitor functionBodyVisitor;
    @Inject
    private PlCreateProcedureVisitor createProcedureVisitor;
    @Inject
    private PlCreateFunctionVisitor createFunctionVisitor;
    @Inject
    private PlDeclareParamsVisitor declareParamsVisitor;
    @Inject
    private PlCreateTableWithVisitor createTableWithVisitor;
    @Inject
    private PlCallStatementVisitor callStatementVisitor;
    @Inject
    private PlAssignmentVisitor assignmentVisitor;
    @Inject
    private PlExpressionVisitor expressionVisitor;
    @Inject
    private PlSelectSQLVisitor selectSQLVisitor;
    @Inject
    private PlInsertSQLVisitor insertSQLVisitor;
    @Inject
    private PlUpdateSQLVisitor updateSQLVisitor;
    @Inject
    private PlDeleteSQLVisitor deleteSQLVisitor;
    @Inject
    private PlMergeSQLVisitor mergeSQLVisitor;
    @Inject
    private PlCreateAsSQLVisitor createAsSQLVisitor;
    @Inject
    private PlTruncateSQLVisitor truncateSQLVisitor;
    @Inject
    private PlIfStatementVisitor ifStatementVisitor;
    @Inject
    private PlLoopStatementVisitor loopStatementVisitor;
    @Inject
    private PlReturnStatementVisitor returnStatementVisitor;
    @Inject
    private PlExceptionHandlerVisitor exceptionHandlerVisitor;

    public void init() {
        statementVisitor.init(this);
        baseVisitor.init(this);
        bodyVisitor.init(this);
        functionBodyVisitor.init(this);
        createProcedureVisitor.init(this);
        createFunctionVisitor.init(this);

        createTableWithVisitor.init(this);
        callStatementVisitor.init(this);
        assignmentVisitor.init(this);

        selectSQLVisitor.init();
        insertSQLVisitor.init(this);
        updateSQLVisitor.init(this);
        deleteSQLVisitor.init(this);
        mergeSQLVisitor.init(this);

        createAsSQLVisitor.init(this);
        truncateSQLVisitor.init(this);

        exceptionHandlerVisitor.init(this);
        returnStatementVisitor.init(this);
        loopStatementVisitor.init(this);
        ifStatementVisitor.init(this);
    }
}

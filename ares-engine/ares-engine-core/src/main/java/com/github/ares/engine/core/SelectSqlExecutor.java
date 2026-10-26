package com.github.ares.engine.core;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.plan.LogicalSelectSQL;

public abstract class SelectSqlExecutor extends AbstractBaseExecutor implements OperationHandler {
    private static final long serialVersionUID = -1L;

    @Override
    public OperationType handledType() {
        return OperationType.SELECT_SQL;
    }

    @Override
    public Scope scope() {
        return Scope.DIRECT;
    }

    @Override
    public Object handle(
            LogicalOperation operation, PlParams plParams, Object lastData, BodyCallback body) {
        return execute((LogicalSelectSQL) operation, plParams, lastData);
    }

    public Object execute(LogicalSelectSQL selectSql, PlParams plParams, Object lastData) {
        traceLogger.info("SQL: {}; Params: {}", selectSql.getOriginSQL(), plParams);
        String sql = selectSql.getSql();
        sql = replaceParams(sql, plParams);
        return executeSelectSql(sql, lastData);
    }

    protected abstract Object executeSelectSql(String sql, Object lastData);
}

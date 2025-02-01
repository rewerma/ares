package com.github.ares.engine.core;

import com.github.ares.parser.plan.LogicalSelectSQL;

import java.io.Serializable;

import static com.github.ares.engine.utils.EngineUtil.replaceParams;

public abstract class SelectSqlExecutor extends AbstractBaseExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    public void commonFunction() {
    }

    public Object execute(LogicalSelectSQL selectSql, PlParams plParams, Object lastData) {
        traceLogger.info("SQL: {}; Params: {}", selectSql.getOriginSQL(), plParams);
        String sql = selectSql.getSql();
        commonFunction();
        sql = replaceParams(sql, plParams);
        return executeSelectSql(sql, lastData);
    }

    protected abstract Object executeSelectSql(String sql, Object lastData);
}

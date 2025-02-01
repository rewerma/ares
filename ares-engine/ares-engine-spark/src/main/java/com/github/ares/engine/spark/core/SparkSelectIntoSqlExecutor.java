package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Inject;
import com.github.ares.com.google.inject.Singleton;
import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.PlParams;
import com.github.ares.engine.core.SelectIntoSqlExecutor;
import com.github.ares.parser.model.Argument;
import com.github.ares.parser.plan.LogicalSelectIntoSQL;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.ares.engine.core.ExpressionExecutor.rawToHex;
import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;
import static com.github.ares.engine.utils.EngineUtil.replaceParams;


public class SparkSelectIntoSqlExecutor extends SelectIntoSqlExecutor {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    @Override
    public void commonFunction() {

    }

    @Override
    public void execute(LogicalSelectIntoSQL selectIntoSQL, PlParams plParams) {
        traceLogger.info("SQL: {}", selectIntoSQL.getOriginSQL());
        Map<String, Serializable> intoValues = new LinkedHashMap<>();
        Map<String, PlType> intoTypes = new LinkedHashMap<>();
        String sql = selectIntoSQL.getSql();
        commonFunction();
        sql = replaceParams(sql, plParams);
        Dataset<Row> resultDf = sparkExecutorManager.getSparkSessionManager().getSparkSession().sql(sql);
        Row[] resultRow = (Row[]) resultDf.limit(1).collect();
        if (resultRow.length == 0) {
            selectIntoSQL.getIntoParams().forEach(argument -> intoValues.put(argument.getName(), null));
        } else {
            Row row = resultRow[0];
            for (int i = 0; i < selectIntoSQL.getIntoParams().size(); i++) {
                Argument argument = selectIntoSQL.getIntoParams().get(i);
                Serializable obj = (Serializable) row.get(i);
                InternalFieldType type = argument.getPlType().getType();
                if (InternalFieldType.VARCHAR == type
                        || InternalFieldType.DATE == type
                        || InternalFieldType.TIMESTAMP == type) {
                    obj = handleQuoteIdentifier(obj);
                } else if (InternalFieldType.BYTES == type) {
                    obj = rawToHex(obj);
                    obj = handleQuoteIdentifier(obj);
                }
                intoValues.put(argument.getName(), obj);
                intoTypes.put(argument.getName(), argument.getPlType());
            }
        }
        plParams.putAll(intoValues);
        plParams.putAllTypes(intoTypes);

        traceLogger.info("SELECT INTO values: {}", intoValues);
    }
}

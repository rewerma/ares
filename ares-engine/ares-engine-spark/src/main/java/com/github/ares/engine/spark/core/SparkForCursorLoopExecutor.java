package com.github.ares.engine.spark.core;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.engine.core.BodyCallback;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.ForCursorLoopExecutor;
import com.github.ares.engine.core.PlParams;
import com.github.ares.parser.plan.LogicalForCursorLoop;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

import static com.github.ares.engine.core.ExpressionExecutor.rawToHex;
import static com.github.ares.engine.utils.EngineUtil.appendQuoteIdentifier;
import static com.github.ares.engine.utils.EngineUtil.convertQuoteIdentifier;
import static com.github.ares.engine.utils.EngineUtil.handleQuoteIdentifier;
import static com.github.ares.engine.utils.EngineUtil.replaceParams;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;

public class SparkForCursorLoopExecutor extends ForCursorLoopExecutor implements Serializable {
    private static final long serialVersionUID = -1L;

    private SparkExecutorManager sparkExecutorManager;

    public void init(ExecutorManager executorManager) {
        this.sparkExecutorManager = (SparkExecutorManager) executorManager;
        super.init(executorManager);
    }

    public void commonFunction() {
    }

    public Object execute(LogicalForCursorLoop forCursorLoop, PlParams plParams, BodyCallback bodyCallback) {
        traceLogger.info("For cursor loop: {} BEGIN", forCursorLoop.getSelectSQL());
        SparkSession sparkSession = sparkExecutorManager.getSparkSessionManager().getSparkSession();
        commonFunction();
        String selectSql = replaceParams(forCursorLoop.getSelectSQL(), plParams);
        Dataset<Row> resultDf = sparkSession.sql(selectSql);

        Object res = null;
        PlParams paramsWithStruct = plParams.copy();
        List<Row> rows = resultDf.toJavaRDD().collect();
        for (Row row : rows) {
            StructType schema = row.schema();
            int columnCount = schema.names().length;
            for (int i = 0; i < columnCount; i++) {
                String columnName = schema.names()[i];
                Serializable value = (Serializable) row.get(i);
                DataType dataType = schema.fields()[i].dataType();
                if (value != null && (DataTypes.StringType == dataType || DataTypes.DateType == dataType
                        || DataTypes.TimestampType == dataType)) {
                    if (DataTypes.StringType == dataType) {
                        value = convertQuoteIdentifier((String) value);
                    }
                    value = appendQuoteIdentifier(value);
                } else if (value instanceof byte[]) {
                    value = rawToHex(value);
                    value = handleQuoteIdentifier(value);
                }
                InternalFieldType fieldType = InternalFieldType.VARCHAR;
                if (DataTypes.IntegerType == dataType) {
                    fieldType = InternalFieldType.INT;
                } else if (DataTypes.LongType == dataType) {
                    fieldType = InternalFieldType.LONG;
                } else if (DataTypes.ShortType == dataType) {
                    fieldType = InternalFieldType.SMALLINT;
                } else if (DataTypes.ByteType == dataType) {
                    fieldType = InternalFieldType.BYTE;
                } else if (DataTypes.BooleanType == dataType) {
                    fieldType = InternalFieldType.BOOLEAN;
                } else if (DataTypes.DoubleType == dataType) {
                    fieldType = InternalFieldType.DOUBLE;
                } else if (DataTypes.FloatType == dataType) {
                    fieldType = InternalFieldType.FLOAT;
                } else if (dataType instanceof DecimalType) {
                    fieldType = InternalFieldType.NUMERIC;
                } else if (DataTypes.DateType == dataType) {
                    fieldType = InternalFieldType.DATE;
                } else if (DataTypes.TimestampType == dataType) {
                    fieldType = InternalFieldType.TIMESTAMP;
                } else if (DataTypes.BinaryType == dataType) {
                    fieldType = InternalFieldType.BYTES;
                }

                paramsWithStruct.put(forCursorLoop.getCursorName() + "." + columnName, value, PlType.of(fieldType));
            }
            res = bodyCallback.invoke(forCursorLoop.getForBody(), paramsWithStruct);
            paramsWithStruct.entrySet().removeIf(entry -> entry.getKey() != null && entry.getKey().startsWith(forCursorLoop.getCursorName() + "."));
            if (EXIT_LOOP == res) {
                break;
            }
        }
        traceLogger.info("For cursor loop: {} END", forCursorLoop.getSelectSQL());
        return res;
    }
}

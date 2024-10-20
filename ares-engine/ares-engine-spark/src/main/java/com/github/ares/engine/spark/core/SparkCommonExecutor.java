package com.github.ares.engine.spark.core;

import com.github.ares.com.google.inject.Singleton;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SparkCommonExecutor implements Serializable {
    private static final long serialVersionUID = 1L;

    protected static final Logger logger = LoggerFactory.getLogger("[SQLExecution]");

    public Dataset<Row> repartition(Dataset<Row> dataset, Integer repartitionNums, List<String> repartitionColumns) {
        logger.info("Repartition by : {}, numbers: {};", repartitionColumns,
                repartitionNums);
        if (repartitionColumns != null && !repartitionColumns.isEmpty()) {
            List<Column> columns = new ArrayList<>();
            repartitionColumns.forEach(colName -> columns.add(new Column(colName)));
            return dataset.repartition(repartitionNums, columns.toArray(new Column[]{}));
        } else {
            return dataset.repartition(repartitionNums);
        }
    }

    public Dataset<Row> cache(Dataset<Row> dataset,String tableName) {
        logger.info("Cache table: {};", tableName);
        return dataset.cache();
    }

    /* public Iterator<String> resultHandle(SqlOperationResult result, Boolean withEx, Logger logger) {
        if (result.getEx() == null) {
            return Collections.singletonList("{\"code\":200}").iterator();
        } else {
            Map<String, Object> dataMap = null;
            if (withEx != null) {
                dataMap = new LinkedHashMap<>();
                Row firstRow = (Row) result.getFirstRow();
                if (firstRow != null) {
                    for (int i = 0; i < firstRow.size(); i++) {
                        String colName = firstRow.schema().fields()[i].name();
                        Object colVal = firstRow.get(i);
                        dataMap.put(colName, colVal);
                    }
                }
            }
            String errorJson = exceptionHandler(result.getEx(), logger, dataMap);
            return Collections.singletonList(errorJson).iterator();
        }
    } */
}

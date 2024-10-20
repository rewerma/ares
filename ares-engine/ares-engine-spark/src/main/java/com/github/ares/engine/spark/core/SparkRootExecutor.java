package com.github.ares.engine.spark.core;

import com.github.ares.engine.core.AbstractRootExecutor;
import com.github.ares.parser.plan.LogicalProject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SparkRootExecutor extends AbstractRootExecutor {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(LogicalProject baseBody) {
        super.execute(baseBody);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Map<String, Object>> lastDataHandler(Object lastRes) {
        List<Map<String, Object>> lines = new ArrayList<>();
        Dataset<Row> lastDf = (Dataset<Row>) lastRes;
        String[] columns = lastDf.columns();
        Row[] rows = (Row[]) lastDf.collect();
        for (Row row : rows) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int i = 0; i < columns.length; i++) {
                rowMap.put(columns[i], row.get(i));
            }
            lines.add(rowMap);
        }
        lastDf.unpersist();
        return lines;
    }
}

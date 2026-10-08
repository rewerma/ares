package com.github.ares.parser.visitor;

import static com.github.ares.api.common.CommonOptions.CONNECTOR;

import com.github.ares.parser.plan.LogicalCreateSinkTable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class PlCreateSinkTableVisitor {

    public LogicalCreateSinkTable visitCreateSinkTable(
            String tableName, Map<String, Object> withOptions) {
        Map<String, Object> options = new LinkedHashMap<>(withOptions);
        String connector = (String) options.get(CONNECTOR.key());
        LogicalCreateSinkTable sinkTable = new LogicalCreateSinkTable(connector);
        sinkTable.setConnector(connector);
        sinkTable.setTableName(tableName.toLowerCase(Locale.ROOT));
        sinkTable.setOptions(options);
        return sinkTable;
    }
}

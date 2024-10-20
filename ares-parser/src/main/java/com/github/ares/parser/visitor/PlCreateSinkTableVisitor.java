package com.github.ares.parser.visitor;

import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.ares.api.common.CommonOptions.CONNECTOR;

public class PlCreateSinkTableVisitor {

    public LogicalCreateSinkTable visitCreateSinkTable(PlSqlParser.Create_tableContext createTableContext, Map<String, String> withOptions) {
        Map<String, Object> withOptionsTmp = new LinkedHashMap<>(withOptions);
        String connector = (String) withOptionsTmp.get(CONNECTOR.key());
        withOptionsTmp.remove("type");
        LogicalCreateSinkTable sinkTable = new LogicalCreateSinkTable(connector);
        sinkTable.setConnector(connector);
        sinkTable.setTableName(createTableContext.table_name().getText());
        sinkTable.setOptions(withOptionsTmp);

        return sinkTable;
    }
}

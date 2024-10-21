package com.github.ares.parser.visitor;

import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresDataTypeHelper;
import com.github.ares.com.google.inject.Inject;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.ParseException;
import com.github.ares.common.utils.Tuple2;
import com.github.ares.parser.antlr4.plsql.PlSqlParser;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import com.github.ares.parser.plan.LogicalOperation;
import com.github.ares.parser.utils.PLParserUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PlCreateSourceTableVisitor {
    @Inject
    private PlDataTypePrecisionVisitor plDataTypePrecisionVisitor;

    private Map<String, LogicalCreateSourceTable> sourceTables;

    public void init(Map<String, LogicalCreateSourceTable> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public LogicalOperation visitCreateSourceTable(PlSqlParser.Create_tableContext create_tableContext,
                                                   Map<String, Object> withOptions) {
        String createTableName = create_tableContext.table_name().getText();
        if (sourceTables.containsKey(createTableName.toLowerCase())) {
            throw new ParseException(String.format("Source table name exists: %s", createTableName));
        }
        LogicalCreateSourceTable sourceTable = new LogicalCreateSourceTable();
        sourceTable.setTableName(createTableName.toLowerCase(Locale.ROOT));
        sourceTable.getOptions().putAll(withOptions);
        String connector = (String) sourceTable.getOptions().get("connector");
        sourceTable.setConnector(connector);

        List<PlSqlParser.Relational_propertyContext> columnsContext = create_tableContext.relational_table().relational_property();
        List<PhysicalColumn> columns = new ArrayList<>();
        for (PlSqlParser.Relational_propertyContext columnContext : columnsContext) {
            String columnName = columnContext.column_definition().column_name().getText();
            if (columnContext.column_definition().datatype() != null) {
                PlSqlParser.Native_datatype_elementContext native_datatype_elementContext = columnContext.column_definition().datatype().native_datatype_element();
                Tuple2<Integer, Integer> precisionAndScale = plDataTypePrecisionVisitor.visit(columnContext.column_definition().datatype());
                String columnType = native_datatype_elementContext.getText();
                PlType plType = PLParserUtil.getTargetType(columnType, precisionAndScale._1(), precisionAndScale._2());
                AresDataType<?> aresDataType = AresDataTypeHelper.getAresDataType(plType);
                PhysicalColumn physicalColumn = new PhysicalColumn(columnName, aresDataType,
                        precisionAndScale._1() == null ? null : precisionAndScale._1().longValue(), precisionAndScale._2());
                columns.add(physicalColumn);
            } else {
                throw new ParseException(String.format("Unsupported column definition: %s", columnContext.getText()));
            }
        }
        sourceTable.setColumns(columns);

        sourceTables.put(createTableName.toLowerCase(), sourceTable);

        return sourceTable;
    }

}

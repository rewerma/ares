
package com.github.ares.api.table.type;

import lombok.Getter;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultipleRowType
        implements AresDataType<AresRow>, Iterable<Map.Entry<String, AresRowType>> {
    private final Map<String, AresRowType> rowTypeMap;
    @Getter private String[] tableIds;

    public MultipleRowType(String[] tableIds, AresRowType[] rowTypes) {
        Map<String, AresRowType> rowTypeMap = new LinkedHashMap<>();
        for (int i = 0; i < tableIds.length; i++) {
            rowTypeMap.put(tableIds[i], rowTypes[i]);
        }
        this.tableIds = tableIds;
        this.rowTypeMap = rowTypeMap;
    }

    public MultipleRowType(Map<String, AresRowType> rowTypeMap) {
        this.tableIds = rowTypeMap.keySet().toArray(new String[0]);
        this.rowTypeMap = rowTypeMap;
    }

    public AresRowType getRowType(String tableId) {
        return rowTypeMap.get(tableId);
    }

    @Override
    public Class<AresRow> getTypeClass() {
        return AresRow.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MULTIPLE_ROW;
    }

    @Override
    public Iterator<Map.Entry<String, AresRowType>> iterator() {
        return rowTypeMap.entrySet().iterator();
    }
}

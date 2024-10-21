package com.github.ares.parser.plan;

import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.TableWith;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class LogicalCreateSourceTable extends TableWith implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<PhysicalColumn> columns = new ArrayList<>();

    private Boolean withCache;

    private Integer withShow;

    public LogicalCreateSourceTable() {
        super(OperationType.CREATE_SOURCE_TABLE);
    }

    public Map<String, Object> getSourceTableConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, Object> schema = new LinkedHashMap<>();
        Map<String, Object> fields = new LinkedHashMap<>();
        for (PhysicalColumn column : columns) {
            String type = column.getDataType().getSqlType().toString();
            AresDataType<?> aresDataType = column.getDataType();
            if (aresDataType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) aresDataType;
                type = type + "(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
            }
            fields.put(column.getName(), type);
        }
        schema.put("fields", fields);
        if (!fields.isEmpty()) {
            config.put("schema", schema);
        }
        config.putAll(super.getOptions());
        return config;
    }
}

package com.github.ares.api.table.catalog;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PrimaryKey implements Serializable {
    private static final long serialVersionUID = 1L;

    // This field is not used now
    private final String primaryKey;

    private final List<String> columnNames;

    public PrimaryKey(String primaryKey, List<String> columnNames) {
        this.primaryKey = primaryKey;
        this.columnNames = columnNames;
    }

    public static PrimaryKey of(String primaryKey, List<String> columnNames) {
        return new PrimaryKey(primaryKey, columnNames);
    }

    public PrimaryKey copy() {
        return PrimaryKey.of(primaryKey, new ArrayList<>(columnNames));
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }
}

package com.github.ares.parser.sqlparser.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SQLInsert implements Serializable {
    private String table;

    private String sourceSql;

    private ArrayList<String> columns = new ArrayList<>();

    private List<List<String>> valuesArray;

    private List<SQLHint> hints;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public List<List<String>> getValuesArray() {
        return valuesArray;
    }

    public void setValuesArray(List<List<String>> valuesArray) {
        this.valuesArray = valuesArray;
    }

    public List<SQLHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }
}

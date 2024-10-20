package com.github.ares.parser.sqlparser.model;

import java.io.Serializable;
import java.util.List;

public class SQLSelect implements Serializable {
    private String sourceSql;

    private List<String> intoParams;

    private List<SQLHint> hints;

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public List<String> getIntoParams() {
        return intoParams;
    }

    public void setIntoParams(List<String> intoParams) {
        this.intoParams = intoParams;
    }

    public List<SQLHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }
}

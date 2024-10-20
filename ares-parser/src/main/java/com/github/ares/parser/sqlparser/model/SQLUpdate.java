package com.github.ares.parser.sqlparser.model;

import com.github.ares.api.common.CriteriaClause;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SQLUpdate implements Serializable {
    private String table;

    private String alias;

    private String joinTable;

    private String joinSql;

    private String joinAlias;

    private List<String> updateColumns = new ArrayList<>();

    private List<String> updateValues = new ArrayList<>();

    private CriteriaClause whereClause;

    private List<String> selectWhereItems;

    private String sourceSql;

    private List<SQLHint> hints;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getJoinTable() {
        return joinTable;
    }

    public void setJoinTable(String joinTable) {
        this.joinTable = joinTable;
    }

    public String getJoinSql() {
        return joinSql;
    }

    public void setJoinSql(String joinSql) {
        this.joinSql = joinSql;
    }

    public String getJoinAlias() {
        return joinAlias;
    }

    public void setJoinAlias(String joinAlias) {
        this.joinAlias = joinAlias;
    }

    public List<String> getUpdateColumns() {
        return updateColumns;
    }

    public void setUpdateColumns(List<String> updateColumns) {
        this.updateColumns = updateColumns;
    }

    public List<String> getUpdateValues() {
        return updateValues;
    }

    public void setUpdateValues(List<String> updateValues) {
        this.updateValues = updateValues;
    }

    public CriteriaClause getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(CriteriaClause whereClause) {
        this.whereClause = whereClause;
    }

    public List<String> getSelectWhereItems() {
        return selectWhereItems;
    }

    public void setSelectWhereItems(List<String> selectWhereItems) {
        this.selectWhereItems = selectWhereItems;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public List<SQLHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }
}

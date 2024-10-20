package com.github.ares.parser.sqlparser.model;

import com.github.ares.api.common.CriteriaClause;

import java.io.Serializable;
import java.util.List;

public class SQLMerge implements Serializable {
    private String table;

    private String alias;

    private String usingTable;

    private String usingSql;

    private String usingAlias;

    private List<String> onSelectItems;

    private SQLInsert sqlInsert;

    private SQLUpdate sqlUpdate;

    private CriteriaClause allWhereClause;

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

    public String getUsingTable() {
        return usingTable;
    }

    public void setUsingTable(String usingTable) {
        this.usingTable = usingTable;
    }

    public String getUsingSql() {
        return usingSql;
    }

    public void setUsingSql(String usingSql) {
        this.usingSql = usingSql;
    }

    public String getUsingAlias() {
        return usingAlias;
    }

    public void setUsingAlias(String usingAlias) {
        this.usingAlias = usingAlias;
    }

    public List<String> getOnSelectItems() {
        return onSelectItems;
    }

    public void setOnSelectItems(List<String> onSelectItems) {
        this.onSelectItems = onSelectItems;
    }

    public SQLInsert getSqlInsert() {
        return sqlInsert;
    }

    public void setSqlInsert(SQLInsert sqlInsert) {
        this.sqlInsert = sqlInsert;
    }

    public SQLUpdate getSqlUpdate() {
        return sqlUpdate;
    }

    public void setSqlUpdate(SQLUpdate sqlUpdate) {
        this.sqlUpdate = sqlUpdate;
    }

    public CriteriaClause getAllWhereClause() {
        return allWhereClause;
    }

    public void setAllWhereClause(CriteriaClause allWhereClause) {
        this.allWhereClause = allWhereClause;
    }

    public List<SQLHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }
}

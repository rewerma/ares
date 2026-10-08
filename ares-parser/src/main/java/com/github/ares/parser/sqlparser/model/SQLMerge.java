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

    private String onSql;

    private boolean matchedDelete;

    private String matchedConditionSql;

    private String notMatchedConditionSql;

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

    public String getOnSql() {
        return onSql;
    }

    public void setOnSql(String onSql) {
        this.onSql = onSql;
    }

    public boolean isMatchedDelete() {
        return matchedDelete;
    }

    public void setMatchedDelete(boolean matchedDelete) {
        this.matchedDelete = matchedDelete;
    }

    public String getMatchedConditionSql() {
        return matchedConditionSql;
    }

    public void setMatchedConditionSql(String matchedConditionSql) {
        this.matchedConditionSql = matchedConditionSql;
    }

    public String getNotMatchedConditionSql() {
        return notMatchedConditionSql;
    }

    public void setNotMatchedConditionSql(String notMatchedConditionSql) {
        this.notMatchedConditionSql = notMatchedConditionSql;
    }

    public List<SQLHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }
}

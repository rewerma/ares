package com.github.ares.parser.model;

import com.github.ares.parser.enums.OperationType;

import java.io.Serializable;
import java.util.List;

public abstract class BaseSqlOption extends BaseOption implements Serializable {
    private static final long serialVersionUID = 1L;

    private String originSQL;
    protected Integer repartitionNums;
    protected List<String> repartitionColumns;

    public BaseSqlOption(OperationType plainType) {
        super(plainType);
    }

    public String getOriginSQL() {
        return originSQL;
    }

    public void setOriginSQL(String originSQL) {
        this.originSQL = originSQL;
    }

    public Integer getRepartitionNums() {
        return repartitionNums;
    }

    public void setRepartitionNums(Integer repartitionNums) {
        this.repartitionNums = repartitionNums;
    }

    public List<String> getRepartitionColumns() {
        return repartitionColumns;
    }

    public void setRepartitionColumns(List<String> repartitionColumns) {
        this.repartitionColumns = repartitionColumns;
    }
}

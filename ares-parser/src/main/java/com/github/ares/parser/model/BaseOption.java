package com.github.ares.parser.model;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.plan.LogicalOperation;

import java.io.Serializable;

public class BaseOption extends LogicalOperation implements Serializable {
    private Integer withShow;

    public BaseOption(OperationType plainType) {
        super(plainType);
    }

    public Integer getWithShow() {
        return withShow;
    }

    public void setWithShow(Integer withShow) {
        this.withShow = withShow;
    }
}

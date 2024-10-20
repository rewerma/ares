package com.github.ares.parser.plan;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.BaseSqlOption;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class LogicalDeleteSelectSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = 1L;

    private LogicalCreateSinkTable sinkTable;
    private String deleteSQL;
    private String selectSQL;
    private List<String> params;
    private int paramsSize;
    private Boolean withEx;

    private CriteriaClause whereClause;

    public LogicalDeleteSelectSQL() {
        super(OperationType.DELETE_SELECT_SQL);
    }
}

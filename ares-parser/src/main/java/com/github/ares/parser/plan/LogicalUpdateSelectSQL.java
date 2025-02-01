package com.github.ares.parser.plan;

import com.github.ares.api.common.CriteriaClause;
import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.BaseSqlOption;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class LogicalUpdateSelectSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalCreateSinkTable sinkTable;
    private String selectSQL;
    private String subSQL;
    private Boolean withEx;

    private List<String> updateItems = new ArrayList<>();

    private CriteriaClause whereClause;

    public LogicalUpdateSelectSQL() {
        super(OperationType.UPDATE_SELECT_SQL);
    }
}

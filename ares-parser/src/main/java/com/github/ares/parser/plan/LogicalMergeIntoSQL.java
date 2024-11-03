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
public class LogicalMergeIntoSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = -1L;

    private LogicalCreateSinkTable sinkTable;

    private ArrayList<String> insertColumns;

    private String insertSourceSql;

    private List<String> updateColumns;

    private List<String> updateValues;

    private String updateSourceSql;

    private CriteriaClause updateWhereClause;

    private List<String> updateWhereSelectItems;

    private LogicalInsertSelectSQL insertSelectSQL;

    private CriteriaClause allWaitCriteria;

    private Boolean withEx;

    public LogicalMergeIntoSQL() {
        super(OperationType.MERGE_INTO_SQL);
    }
}

package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.BaseSqlOption;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class LogicalCreateTableAsSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = -1L;

    private String selectSQL;
    private String tableName;

    private Boolean withCache;

    public LogicalCreateTableAsSQL() {
        super(OperationType.CREATE_TABLE_AS_SQL);
    }
}

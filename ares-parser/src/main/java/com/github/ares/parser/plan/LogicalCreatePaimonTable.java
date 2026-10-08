package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** Paimon CREATE TABLE executed by Spark SQL against a Paimon catalog. */
@Getter
@Setter
public class LogicalCreatePaimonTable extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String originSQL;
    private String sql;
    private String tableName;
    private Map<String, Object> properties;

    public LogicalCreatePaimonTable() {
        super(OperationType.CREATE_PAIMON_TABLE);
    }
}

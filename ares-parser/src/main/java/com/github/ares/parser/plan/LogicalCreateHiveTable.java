package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/** Hive CREATE TABLE executed by Spark SQL against the Hive catalog. */
@Getter
@Setter
public class LogicalCreateHiveTable extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String originSQL;
    private String sql;
    private String tableName;

    public LogicalCreateHiveTable() {
        super(OperationType.CREATE_HIVE_TABLE);
    }
}

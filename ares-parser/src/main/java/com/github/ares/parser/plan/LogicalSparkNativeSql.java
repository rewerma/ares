package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

/** Spark SQL statement executed verbatim via {@code SparkSession.sql}. */
@Getter
@Setter
public class LogicalSparkNativeSql extends LogicalOperation implements Serializable {
    private static final long serialVersionUID = -1L;

    private String originSQL;
    private String sql;

    public LogicalSparkNativeSql() {
        super(OperationType.SPARK_NATIVE_SQL);
    }
}

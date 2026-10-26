package com.github.ares.parser.plan;

import com.github.ares.parser.enums.OperationType;
import com.github.ares.parser.model.BaseSqlOption;
import java.io.Serializable;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalCreateTableAsSQL extends BaseSqlOption implements Serializable {
    private static final long serialVersionUID = -1L;

    private String selectSQL;
    private String tableName;

    private Boolean withCache;

    /** Connector options carried by catalog views such as Paimon filesystem. */
    private Map<String, Object> properties;

    public LogicalCreateTableAsSQL() {
        super(OperationType.CREATE_TABLE_AS_SQL);
    }
}

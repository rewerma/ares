package com.github.ares.connctor.jdbc.source;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.TablePath;

import java.io.Serializable;
import java.math.BigDecimal;

public class JdbcSourceTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String query;
    private final String partitionColumn;
    private final Integer partitionNumber;
    private final BigDecimal partitionStart;
    private final BigDecimal partitionEnd;
    private final CatalogTable catalogTable;

    public JdbcSourceTable(TablePath tablePath, String query, String partitionColumn, Integer partitionNumber,
                           BigDecimal partitionStart, BigDecimal partitionEnd, CatalogTable catalogTable) {
        this.tablePath = tablePath;
        this.query = query;
        this.partitionColumn = partitionColumn;
        this.partitionNumber = partitionNumber;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.catalogTable = catalogTable;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getQuery() {
        return query;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public Integer getPartitionNumber() {
        return partitionNumber;
    }

    public BigDecimal getPartitionStart() {
        return partitionStart;
    }

    public BigDecimal getPartitionEnd() {
        return partitionEnd;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }
}

package com.github.ares.connctor.jdbc.config;

import com.github.ares.common.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

//@Data
//@Builder
//@JsonIgnoreProperties(ignoreUnknown = true)
public class JdbcSourceTableConfig implements Serializable {
    private static final int DEFAULT_PARTITION_NUMBER = 10;

    //    @JsonProperty("table_path")
    private String tablePath;

    //    @JsonProperty("query")
    private String query;

    //    @JsonProperty("partition_column")
    private String partitionColumn;

    //    @JsonProperty("partition_num")
    private Integer partitionNumber;

    //    @JsonProperty("partition_lower_bound")
    private BigDecimal partitionStart;

    //    @JsonProperty("partition_upper_bound")
    private BigDecimal partitionEnd;

    public JdbcSourceTableConfig() {
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public Integer getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(Integer partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public BigDecimal getPartitionStart() {
        return partitionStart;
    }

    public void setPartitionStart(BigDecimal partitionStart) {
        this.partitionStart = partitionStart;
    }

    public BigDecimal getPartitionEnd() {
        return partitionEnd;
    }

    public void setPartitionEnd(BigDecimal partitionEnd) {
        this.partitionEnd = partitionEnd;
    }


    public static List<JdbcSourceTableConfig> of(ReadonlyConfig connectorConfig) {
        List<JdbcSourceTableConfig> tableList;
        if (connectorConfig.getOptional(JdbcSourceOptions.TABLE_LIST).isPresent()) {
            if (connectorConfig.getOptional(JdbcOptions.QUERY).isPresent()
                    || connectorConfig.getOptional(JdbcSourceOptions.TABLE_NAME).isPresent()) {
                throw new IllegalArgumentException(
                        "Please configure either `table_list` or `table_path`/`query`, not both");
            }
            tableList = connectorConfig.get(JdbcSourceOptions.TABLE_LIST);
        } else {
            JdbcSourceTableConfig tableProperty =
                    new JdbcSourceTableConfig();
            tableProperty.setTablePath(connectorConfig.get(JdbcSourceOptions.TABLE_NAME));
            tableProperty.setQuery(connectorConfig.get(JdbcOptions.QUERY));
            if (tableProperty.getQuery() == null && tableProperty.getTablePath() != null) {
                tableProperty.setQuery("SELECT * FROM " + tableProperty.getTablePath());
                tableProperty.setTablePath(null);
            }
            tableProperty.setPartitionColumn(connectorConfig.get(JdbcOptions.PARTITION_COLUMN));
            tableProperty.setPartitionNumber(connectorConfig.get(JdbcOptions.PARTITION_NUM));
            tableProperty.setPartitionStart(connectorConfig.get(JdbcOptions.PARTITION_LOWER_BOUND));
            tableProperty.setPartitionEnd(connectorConfig.get(JdbcOptions.PARTITION_UPPER_BOUND));
            tableList = Collections.singletonList(tableProperty);
        }

        tableList.forEach(
                tableConfig -> {
                    if (tableConfig.getPartitionNumber() == null) {
                        tableConfig.setPartitionNumber(DEFAULT_PARTITION_NUMBER);
                    }
                });

        if (tableList.size() > 1) {
            List<String> tableIds =
                    tableList.stream().map(e -> e.getTablePath()).collect(Collectors.toList());
            Set<String> tableIdSet = new HashSet<>(tableIds);
            if (tableIdSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_path`, not allow null/duplicate table path: "
                                + tableIds);
            }
        }
        return tableList;
    }
}

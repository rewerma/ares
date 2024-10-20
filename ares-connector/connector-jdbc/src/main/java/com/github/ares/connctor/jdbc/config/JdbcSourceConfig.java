package com.github.ares.connctor.jdbc.config;

import com.github.ares.common.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.List;

//@Builder(builderClassName = "Builder")
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private List<JdbcSourceTableConfig> tableConfigList;
    private String whereConditionClause;
    public String compatibleMode;
    private int fetchSize;

    private boolean useDynamicSplitter;
    private int splitSize;
    private double splitEvenDistributionFactorUpperBound;
    private double splitEvenDistributionFactorLowerBound;
    private int splitSampleShardingThreshold;
    private int splitInverseSamplingRate;

    public JdbcConnectionConfig getJdbcConnectionConfig() {
        return jdbcConnectionConfig;
    }

    public void setJdbcConnectionConfig(JdbcConnectionConfig jdbcConnectionConfig) {
        this.jdbcConnectionConfig = jdbcConnectionConfig;
    }

    public List<JdbcSourceTableConfig> getTableConfigList() {
        return tableConfigList;
    }

    public void setTableConfigList(List<JdbcSourceTableConfig> tableConfigList) {
        this.tableConfigList = tableConfigList;
    }

    public String getWhereConditionClause() {
        return whereConditionClause;
    }

    public void setWhereConditionClause(String whereConditionClause) {
        this.whereConditionClause = whereConditionClause;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public void setCompatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public boolean isUseDynamicSplitter() {
        return useDynamicSplitter;
    }

    public void setUseDynamicSplitter(boolean useDynamicSplitter) {
        this.useDynamicSplitter = useDynamicSplitter;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }

    public double getSplitEvenDistributionFactorUpperBound() {
        return splitEvenDistributionFactorUpperBound;
    }

    public void setSplitEvenDistributionFactorUpperBound(double splitEvenDistributionFactorUpperBound) {
        this.splitEvenDistributionFactorUpperBound = splitEvenDistributionFactorUpperBound;
    }

    public double getSplitEvenDistributionFactorLowerBound() {
        return splitEvenDistributionFactorLowerBound;
    }

    public void setSplitEvenDistributionFactorLowerBound(double splitEvenDistributionFactorLowerBound) {
        this.splitEvenDistributionFactorLowerBound = splitEvenDistributionFactorLowerBound;
    }

    public int getSplitSampleShardingThreshold() {
        return splitSampleShardingThreshold;
    }

    public void setSplitSampleShardingThreshold(int splitSampleShardingThreshold) {
        this.splitSampleShardingThreshold = splitSampleShardingThreshold;
    }

    public int getSplitInverseSamplingRate() {
        return splitInverseSamplingRate;
    }

    public void setSplitInverseSamplingRate(int splitInverseSamplingRate) {
        this.splitInverseSamplingRate = splitInverseSamplingRate;
    }

    public static JdbcSourceConfig of(ReadonlyConfig config) {
        JdbcSourceConfig jdbcSourceConfig = new JdbcSourceConfig();
        jdbcSourceConfig.setJdbcConnectionConfig(JdbcConnectionConfig.of(config));
        jdbcSourceConfig.setTableConfigList(JdbcSourceTableConfig.of(config));
        jdbcSourceConfig.setFetchSize(config.get(JdbcOptions.FETCH_SIZE));
        config.getOptional(JdbcOptions.COMPATIBLE_MODE).ifPresent(jdbcSourceConfig::setCompatibleMode);

        boolean isOldVersion =
                config.getOptional(JdbcOptions.QUERY).isPresent()
                        && config.getOptional(JdbcOptions.PARTITION_COLUMN).isPresent()
                        && config.getOptional(JdbcOptions.PARTITION_NUM).isPresent();
        jdbcSourceConfig.setUseDynamicSplitter(isOldVersion ? false : true);

        jdbcSourceConfig.setSplitSize(config.get(JdbcSourceOptions.SPLIT_SIZE));
        jdbcSourceConfig.setSplitEvenDistributionFactorUpperBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND));
        jdbcSourceConfig.setSplitEvenDistributionFactorUpperBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND));
        jdbcSourceConfig.setSplitSampleShardingThreshold(
                config.get(JdbcSourceOptions.SPLIT_SAMPLE_SHARDING_THRESHOLD));
        jdbcSourceConfig.setSplitInverseSamplingRate(
                config.get(JdbcSourceOptions.SPLIT_INVERSE_SAMPLING_RATE));

        config.getOptional(JdbcSourceOptions.WHERE_CONDITION)
                .ifPresent(
                        whereConditionClause -> {
                            if (!whereConditionClause.toLowerCase().startsWith("where")) {
                                throw new IllegalArgumentException(
                                        "The where condition clause must start with 'where'. value: "
                                                + whereConditionClause);
                            }
                            jdbcSourceConfig.setWhereConditionClause(whereConditionClause);
                        });

        return jdbcSourceConfig;
    }
}

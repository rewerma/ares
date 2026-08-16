package com.github.ares.connector.jdbc.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.common.configuration.ReadonlyConfig;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
//@Builder(builderClassName = "Builder")
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private String dbType;
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

    public static JdbcSourceConfig of(ReadonlyConfig config) {
        JdbcSourceConfig jdbcSourceConfig = new JdbcSourceConfig();
        jdbcSourceConfig.setJdbcConnectionConfig(JdbcConnectionConfig.of(config));
        jdbcSourceConfig.setDbType(config.get(CommonOptions.CONNECTOR));
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

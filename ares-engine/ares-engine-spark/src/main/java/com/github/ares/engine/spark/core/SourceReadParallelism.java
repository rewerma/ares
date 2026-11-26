package com.github.ares.engine.spark.core;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.env.EnvCommonOptions;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

final class SourceReadParallelism {
    private SourceReadParallelism() {}

    static int resolve(LogicalCreateSourceTable sourceTable, Properties envProperties) {
        if (sourceTable != null) {
            Object tableParallelism = sourceTable.getOptions().get(CommonOptions.PARALLELISM.key());
            if (tableParallelism != null) {
                return Integer.parseInt(String.valueOf(tableParallelism));
            }
        }
        if (envProperties != null) {
            String envParallelism = envProperties.getProperty(EnvCommonOptions.PARALLELISM.key());
            if (StringUtils.isNotBlank(envParallelism)) {
                return Integer.parseInt(envParallelism.trim());
            }
        }
        return CommonOptions.PARALLELISM.defaultValue();
    }
}

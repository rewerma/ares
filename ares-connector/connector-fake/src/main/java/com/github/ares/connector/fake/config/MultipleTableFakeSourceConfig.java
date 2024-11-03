package com.github.ares.connector.fake.config;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MultipleTableFakeSourceConfig implements Serializable {

    private static final long serialVersionUID = -1L;

    @Getter private List<FakeConfig> fakeConfigs;

    public MultipleTableFakeSourceConfig(ReadonlyConfig fakeSourceRootConfig) {
        if (fakeSourceRootConfig.getOptional(FakeOption.TABLES_CONFIGS).isPresent()) {
            parseFromConfigs(fakeSourceRootConfig);
        } else {
            parseFromConfig(fakeSourceRootConfig);
        }
        // validate
        if (fakeConfigs.size() > 1) {
            List<String> tableNames =
                    fakeConfigs.stream()
                            .map(FakeConfig::getCatalogTable)
                            .map(catalogTable -> catalogTable.getTableId().toTablePath().toString())
                            .collect(Collectors.toList());
            if (CollectionUtils.size(tableNames) != new HashSet<>(tableNames).size()) {
                throw new IllegalArgumentException("table name: " + tableNames + " must be unique");
            }
        }
    }

    private void parseFromConfigs(ReadonlyConfig readonlyConfig) {
        List<ReadonlyConfig> readonlyConfigs =
                readonlyConfig.getOptional(FakeOption.TABLES_CONFIGS).get().stream()
                        .map(ReadonlyConfig::fromMap)
                        .collect(Collectors.toList());
        // Use the config outside if it's not set in sub config
        fakeConfigs =
                readonlyConfigs.stream()
                        .map(FakeConfig::buildWithConfig)
                        .collect(Collectors.toList());
    }

    private void parseFromConfig(ReadonlyConfig readonlyConfig) {
        FakeConfig fakeConfig = FakeConfig.buildWithConfig(readonlyConfig);
        fakeConfigs = Lists.newArrayList(fakeConfig);
    }
}

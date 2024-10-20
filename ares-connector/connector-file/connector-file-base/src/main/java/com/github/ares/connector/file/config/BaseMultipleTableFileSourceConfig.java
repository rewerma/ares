package com.github.ares.connector.file.config;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseMultipleTableFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private List<BaseFileSourceConfig> fileSourceConfigs;

    public BaseMultipleTableFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        if (fileSourceRootConfig.getOptional(BaseSourceConfigOptions.TABLE_CONFIGS).isPresent()) {
            parseFromFileSourceConfigs(fileSourceRootConfig);
        } else {
            parseFromFileSourceConfig(fileSourceRootConfig);
        }
    }

    private void parseFromFileSourceConfigs(ReadonlyConfig fileSourceRootConfig) {
        this.fileSourceConfigs =
                fileSourceRootConfig.get(BaseSourceConfigOptions.TABLE_CONFIGS).stream()
                        .map(ReadonlyConfig::fromMap)
                        .map(this::getBaseSourceConfig)
                        .collect(Collectors.toList());
    }

    public abstract BaseFileSourceConfig getBaseSourceConfig(ReadonlyConfig readonlyConfig);

    private void parseFromFileSourceConfig(ReadonlyConfig fileSourceRootConfig) {
        this.fileSourceConfigs = Lists.newArrayList(getBaseSourceConfig(fileSourceRootConfig));
    }
}

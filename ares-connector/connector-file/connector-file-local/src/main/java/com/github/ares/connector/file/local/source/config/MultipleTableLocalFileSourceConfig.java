package com.github.ares.connector.file.local.source.config;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.config.BaseFileSourceConfig;
import com.github.ares.connector.file.config.BaseMultipleTableFileSourceConfig;

public class MultipleTableLocalFileSourceConfig extends BaseMultipleTableFileSourceConfig {

    public MultipleTableLocalFileSourceConfig(ReadonlyConfig localFileSourceRootConfig) {
        super(localFileSourceRootConfig);
    }

    @Override
    public BaseFileSourceConfig getBaseSourceConfig(ReadonlyConfig readonlyConfig) {
        return new LocalFileSourceConfig(readonlyConfig);
    }
}

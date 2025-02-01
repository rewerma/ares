package com.github.ares.connector.file.local.source.config;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.config.BaseFileSourceConfig;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.local.config.LocalFileHadoopConf;
import lombok.Getter;

@Getter
public class LocalFileSourceConfig extends BaseFileSourceConfig {

    private static final long serialVersionUID = -1L;

    @Override
    public HadoopConf getHadoopConfig() {
        return new LocalFileHadoopConf();
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    public LocalFileSourceConfig(ReadonlyConfig readonlyConfig) {
        super(readonlyConfig);
    }
}

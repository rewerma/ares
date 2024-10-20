package com.github.ares.connector.file.local.source;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.local.source.config.MultipleTableLocalFileSourceConfig;
import com.github.ares.connector.file.source.BaseMultipleTableFileSource;

public class LocalFileSource extends BaseMultipleTableFileSource {

    public LocalFileSource(ReadonlyConfig readonlyConfig) {
        super(new MultipleTableLocalFileSourceConfig(readonlyConfig));
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }
}

package com.github.ares.connector.file.local.sink;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.local.config.LocalFileHadoopConf;
import com.github.ares.connector.file.sink.BaseMultipleTableFileSink;

public class LocalFileSink extends BaseMultipleTableFileSink {

    public LocalFileSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(new LocalFileHadoopConf(), readonlyConfig, catalogTable);
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }
}

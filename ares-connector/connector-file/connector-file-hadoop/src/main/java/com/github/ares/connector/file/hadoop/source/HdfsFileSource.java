package com.github.ares.connector.file.hadoop.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.FileSystemType;
import com.google.auto.service.AutoService;

@AutoService(AresSource.class)
public class HdfsFileSource extends BaseHdfsFileSource {

    @Override
    public String getPluginName() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) {
        super.prepare(pluginConfig);
    }
}

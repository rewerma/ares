package com.github.ares.connector.file.hadoop.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.FileSystemType;
import com.google.auto.service.AutoService;

@AutoService(AresSink.class)
public class HdfsFileSink extends BaseHdfsFileSink {

    @Override
    public String getPluginName() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) {
        super.prepare(pluginConfig);
    }
}

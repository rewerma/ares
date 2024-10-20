package com.github.ares.connector.file.local.config;

import com.github.ares.connector.file.config.HadoopConf;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

public class LocalFileHadoopConf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
    private static final String SCHEMA = "file";

    public LocalFileHadoopConf() {
        super(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
    }

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }
}

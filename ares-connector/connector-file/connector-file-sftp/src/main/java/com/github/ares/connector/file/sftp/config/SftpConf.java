package com.github.ares.connector.file.sftp.config;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.HadoopConf;

import java.util.HashMap;

public class SftpConf extends HadoopConf {
    private static final String HDFS_IMPL =
            "com.github.ares.connector.file.sftp.system.SFTPFileSystem";
    private static final String SCHEMA = "sftp";

    private SftpConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public static HadoopConf buildWithConfig(Config config) {
        String host = config.getString(SftpConfigOptions.SFTP_HOST.key());
        int port = config.getInt(SftpConfigOptions.SFTP_PORT.key());
        String defaultFS = String.format("sftp://%s:%s", host, port);
        HadoopConf hadoopConf = new SftpConf(defaultFS);
        HashMap<String, String> sftpOptions = new HashMap<>();
        sftpOptions.put(
                "fs.sftp.user." + host, config.getString(SftpConfigOptions.SFTP_USER.key()));
        sftpOptions.put(
                "fs.sftp.password."
                        + host
                        + "."
                        + config.getString(SftpConfigOptions.SFTP_USER.key()),
                config.getString(SftpConfigOptions.SFTP_PASSWORD.key()));
        hadoopConf.setExtraOptions(sftpOptions);
        return hadoopConf;
    }
}

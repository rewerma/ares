package com.github.ares.connector.file.ftp.config;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.HadoopConf;

import java.util.HashMap;

public class FtpConf extends HadoopConf {
    private static final String HDFS_IMPL =
            "com.github.ares.connector.file.ftp.system.AresFTPFileSystem";
    private static final String SCHEMA = "ftp";

    private FtpConf(String hdfsNameKey) {
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
        String host = config.getString(FtpConfigOptions.FTP_HOST.key());
        int port = config.getInt(FtpConfigOptions.FTP_PORT.key());
        String defaultFS = String.format("ftp://%s:%s", host, port);
        HadoopConf hadoopConf = new FtpConf(defaultFS);
        HashMap<String, String> ftpOptions = new HashMap<>();
        ftpOptions.put(
                "fs.ftp.user." + host, config.getString(FtpConfigOptions.FTP_USERNAME.key()));
        ftpOptions.put(
                "fs.ftp.password." + host, config.getString(FtpConfigOptions.FTP_PASSWORD.key()));
        if (config.hasPath(FtpConfigOptions.FTP_CONNECTION_MODE.key())) {
            ftpOptions.put(
                    "fs.ftp.connection.mode",
                    config.getString(FtpConfigOptions.FTP_CONNECTION_MODE.key()));
        }
        hadoopConf.setExtraOptions(ftpOptions);
        return hadoopConf;
    }
}

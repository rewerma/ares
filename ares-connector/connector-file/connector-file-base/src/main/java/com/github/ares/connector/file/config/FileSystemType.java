package com.github.ares.connector.file.config;

import java.io.Serializable;

public enum FileSystemType implements Serializable {
    HDFS("FileHadoop"),
    LOCAL("FileLocal"),
    OSS("FileOss"),
    OSS_JINDO("FileOssJindo"),
    COS("FileCos"),
    FTP("FileFtp"),
    SFTP("FileSftp"),
    S3("FileS3");

    private final String fileSystemPluginName;

    FileSystemType(String fileSystemPluginName) {
        this.fileSystemPluginName = fileSystemPluginName;
    }

    public String getFileSystemPluginName() {
        return fileSystemPluginName;
    }
}

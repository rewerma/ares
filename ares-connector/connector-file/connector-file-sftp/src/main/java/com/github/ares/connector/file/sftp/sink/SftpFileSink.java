package com.github.ares.connector.file.sftp.sink;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sftp.config.SftpConf;
import com.github.ares.connector.file.sftp.config.SftpConfigOptions;
import com.github.ares.connector.file.sink.BaseFileSink;
import com.google.auto.service.AutoService;

@AutoService(AresSink.class)
public class SftpFileSink extends BaseFileSink {
    @Override
    public String getPluginName() {
        return FileSystemType.SFTP.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        SftpConfigOptions.SFTP_HOST.key(),
                        SftpConfigOptions.SFTP_PORT.key(),
                        SftpConfigOptions.SFTP_USER.key(),
                        SftpConfigOptions.SFTP_PASSWORD.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        super.prepare(pluginConfig);
        hadoopConf = SftpConf.buildWithConfig(pluginConfig);
    }
}

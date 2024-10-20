package com.github.ares.connector.file.ftp.sink;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.ftp.config.FtpConf;
import com.github.ares.connector.file.ftp.config.FtpConfigOptions;
import com.github.ares.connector.file.sink.BaseFileSink;
import com.google.auto.service.AutoService;

@AutoService(AresSink.class)
public class FtpFileSink extends BaseFileSink {
    @Override
    public String getPluginName() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        FtpConfigOptions.FTP_HOST.key(),
                        FtpConfigOptions.FTP_PORT.key(),
                        FtpConfigOptions.FTP_USERNAME.key(),
                        FtpConfigOptions.FTP_PASSWORD.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        super.prepare(pluginConfig);
        hadoopConf = FtpConf.buildWithConfig(pluginConfig);
    }
}

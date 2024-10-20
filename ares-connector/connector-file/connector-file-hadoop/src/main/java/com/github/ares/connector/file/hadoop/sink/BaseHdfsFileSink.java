package com.github.ares.connector.file.hadoop.sink;

import com.github.ares.api.common.PluginType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.connector.file.config.BaseSinkConfig;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.BaseFileSink;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

public abstract class BaseHdfsFileSink extends BaseFileSink {

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, FS_DEFAULT_NAME_KEY);
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        super.prepare(pluginConfig);
        hadoopConf = new HadoopConf(pluginConfig.getString(FS_DEFAULT_NAME_KEY));
        if (pluginConfig.hasPath(BaseSinkConfig.HDFS_SITE_PATH.key())) {
            hadoopConf.setHdfsSitePath(pluginConfig.getString(BaseSinkConfig.HDFS_SITE_PATH.key()));
        }

        if (pluginConfig.hasPath(BaseSinkConfig.REMOTE_USER.key())) {
            hadoopConf.setRemoteUser(pluginConfig.getString(BaseSinkConfig.REMOTE_USER.key()));
        }

        if (pluginConfig.hasPath(BaseSinkConfig.KRB5_PATH.key())) {
            hadoopConf.setKrb5Path(pluginConfig.getString(BaseSinkConfig.KRB5_PATH.key()));
        }

        if (pluginConfig.hasPath(BaseSinkConfig.KERBEROS_PRINCIPAL.key())) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.getString(BaseSinkConfig.KERBEROS_PRINCIPAL.key()));
        }
        if (pluginConfig.hasPath(BaseSinkConfig.KERBEROS_KEYTAB_PATH.key())) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.getString(BaseSinkConfig.KERBEROS_KEYTAB_PATH.key()));
        }
    }
}

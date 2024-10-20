package com.github.ares.connector.file.hadoop.source;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.table.catalog.CatalogTableUtil;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.hadoop.source.config.HdfsSourceConfigOptions;
import com.github.ares.connector.file.source.BaseFileSource;
import com.github.ares.connector.file.source.reader.ReadStrategyFactory;

import java.io.IOException;

public abstract class BaseHdfsFileSource extends BaseFileSource {

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        HdfsSourceConfigOptions.FILE_PATH.key(),
                        HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key(),
                        HdfsSourceConfigOptions.DEFAULT_FS.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        String path = pluginConfig.getString(HdfsSourceConfigOptions.FILE_PATH.key());
        hadoopConf =
                new HadoopConf(pluginConfig.getString(HdfsSourceConfigOptions.DEFAULT_FS.key()));
        if (pluginConfig.hasPath(HdfsSourceConfigOptions.HDFS_SITE_PATH.key())) {
            hadoopConf.setHdfsSitePath(
                    pluginConfig.getString(HdfsSourceConfigOptions.HDFS_SITE_PATH.key()));
        }

        if (pluginConfig.hasPath(HdfsSourceConfigOptions.REMOTE_USER.key())) {
            hadoopConf.setRemoteUser(
                    pluginConfig.getString(HdfsSourceConfigOptions.REMOTE_USER.key()));
        }

        if (pluginConfig.hasPath(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL.key())) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.getString(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL.key()));
        }
        if (pluginConfig.hasPath(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH.key())) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.getString(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH.key()));
        }
        readStrategy =
                ReadStrategyFactory.of(
                        pluginConfig.getString(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(pluginConfig);
        readStrategy.init(hadoopConf);
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }

        // support user-defined schema
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig
                                .getString(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key())
                                .toUpperCase());
        // only json text csv type support user-defined schema now
        if (pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                case XML:
                    AresRowType userDefinedSchema =
                            CatalogTableUtil.buildWithConfig(pluginConfig).getAresRowType();
                    readStrategy.setAresRowTypeInfo(userDefinedSchema);
                    rowType = readStrategy.getActualAresRowTypeInfo();
                    break;
                case ORC:
                case PARQUET:
                    throw new FileConnectorException(
                            CommonErrorCode.UNSUPPORTED_OPERATION,
                            "Ares does not support user-defined schema for [parquet, orc] files");
                default:
                    // never got in there
                    throw new FileConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            "Ares does not supported this file format");
            }
        } else {
            if (filePaths.isEmpty()) {
                // When the directory is empty, distribute default behavior schema
                rowType = CatalogTableUtil.buildSimpleTextSchema();
                return;
            }
            try {
                rowType = readStrategy.getAresRowTypeInfo(filePaths.get(0));
            } catch (FileConnectorException e) {
                String errorMsg =
                        String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(
                        CommonErrorCode.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }

    @Override
    public void refreshFilePaths() {
        if (pluginConfig == null) {
            return;
        }
        String path = pluginConfig.getString(HdfsSourceConfigOptions.FILE_PATH.key());
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
    }
}

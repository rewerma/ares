package com.github.ares.connector.file.ftp.source;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.catalog.CatalogTableUtil;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.ftp.config.FtpConf;
import com.github.ares.connector.file.ftp.config.FtpConfigOptions;
import com.github.ares.connector.file.source.BaseFileSource;
import com.github.ares.connector.file.source.reader.ReadStrategyFactory;
import com.google.auto.service.AutoService;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPInputStream;

import java.io.IOException;

@AutoService(AresSource.class)
public class FtpFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        FtpConfigOptions.FILE_PATH.key(),
                        FtpConfigOptions.FILE_FORMAT_TYPE.key(),
                        FtpConfigOptions.FTP_HOST.key(),
                        FtpConfigOptions.FTP_PORT.key(),
                        FtpConfigOptions.FTP_USERNAME.key(),
                        FtpConfigOptions.FTP_PASSWORD.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig
                                .getString(FtpConfigOptions.FILE_FORMAT_TYPE.key())
                                .toUpperCase());
        if (fileFormat == FileFormat.ORC || fileFormat == FileFormat.PARQUET) {
            throw new FileConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Ftp file source connector only support read [text, csv, json] files");
        }
        String path = pluginConfig.getString(FtpConfigOptions.FILE_PATH.key());
        hadoopConf = FtpConf.buildWithConfig(pluginConfig);
        readStrategy =
                ReadStrategyFactory.of(
                        pluginConfig.getString(FtpConfigOptions.FILE_FORMAT_TYPE.key()));
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
        // only json type support user-defined schema now
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
}

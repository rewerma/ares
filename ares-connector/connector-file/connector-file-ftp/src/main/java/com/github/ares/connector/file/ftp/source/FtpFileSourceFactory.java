package com.github.ares.connector.file.ftp.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.ftp.config.FtpConfigOptions;
import com.google.auto.service.AutoService;
import org.apache.hadoop.fs.ftp.FTPInputStream;

import java.util.Arrays;

@AutoService(Factory.class)
public class FtpFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.FTP.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(FtpConfigOptions.FILE_PATH)
                .required(FtpConfigOptions.FTP_HOST)
                .required(FtpConfigOptions.FTP_PORT)
                .required(FtpConfigOptions.FTP_USERNAME)
                .required(FtpConfigOptions.FTP_PASSWORD)
                .required(FtpConfigOptions.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfigOptions.FIELD_DELIMITER)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        BaseSourceConfigOptions.XML_ROW_TAG,
                        BaseSourceConfigOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        BaseSourceConfigOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        TableSchemaOptions.SCHEMA)
                .optional(BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfigOptions.DATE_FORMAT)
                .optional(BaseSourceConfigOptions.DATETIME_FORMAT)
                .optional(BaseSourceConfigOptions.TIME_FORMAT)
                .optional(BaseSourceConfigOptions.FILE_FILTER_PATTERN)
                .optional(BaseSourceConfigOptions.COMPRESS_CODEC)
                .optional(FtpConfigOptions.FTP_CONNECTION_MODE)
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return FtpFileSource.class;
    }
}

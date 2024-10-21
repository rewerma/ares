package com.github.ares.connector.file.sftp.source;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.sftp.config.SftpConfigOptions;
import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class SftpFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.SFTP.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SftpConfigOptions.FILE_PATH)
                .required(SftpConfigOptions.SFTP_HOST)
                .required(SftpConfigOptions.SFTP_PORT)
                .required(SftpConfigOptions.SFTP_USER)
                .required(SftpConfigOptions.SFTP_PASSWORD)
                .required(BaseSourceConfigOptions.FILE_FORMAT_TYPE)
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
                .build();
    }

    @Override
    public Class<? extends AresSource> getSourceClass() {
        return SftpFileSource.class;
    }
}

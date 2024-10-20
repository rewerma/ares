package com.github.ares.connector.file.ftp.sink;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.common.configuration.utils.OptionRule;
import com.github.ares.connector.file.config.BaseSinkConfig;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.FileSystemType;
import com.github.ares.connector.file.ftp.config.FtpConfigOptions;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class FtpFileSinkFactory implements TableSinkFactory {
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
                .optional(BaseSinkConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSinkConfig.ROW_DELIMITER,
                        BaseSinkConfig.FIELD_DELIMITER,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        BaseSinkConfig.TXT_COMPRESS,
                        BaseSinkConfig.ENABLE_HEADER_WRITE)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        BaseSinkConfig.TXT_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        BaseSinkConfig.ORC_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        BaseSinkConfig.PARQUET_COMPRESS)
                .conditional(
                        BaseSinkConfig.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        BaseSinkConfig.XML_USE_ATTR_FORMAT)
                .optional(BaseSinkConfig.CUSTOM_FILENAME)
                .conditional(
                        BaseSinkConfig.CUSTOM_FILENAME,
                        true,
                        BaseSinkConfig.FILE_NAME_EXPRESSION,
                        BaseSinkConfig.FILENAME_TIME_FORMAT)
                .optional(BaseSinkConfig.HAVE_PARTITION)
                .conditional(
                        BaseSinkConfig.HAVE_PARTITION,
                        true,
                        BaseSinkConfig.PARTITION_BY,
                        BaseSinkConfig.PARTITION_DIR_EXPRESSION,
                        BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .optional(BaseSinkConfig.SINK_COLUMNS)
                .optional(BaseSinkConfig.IS_ENABLE_TRANSACTION)
                .optional(BaseSinkConfig.DATE_FORMAT)
                .optional(BaseSinkConfig.DATETIME_FORMAT)
                .optional(BaseSinkConfig.TIME_FORMAT)
                .optional(FtpConfigOptions.FTP_CONNECTION_MODE)
                .build();
    }
}

package com.github.ares.connector.file.sink.writer;

import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriteStrategyFactory {

    private WriteStrategyFactory() {}

    public static WriteStrategy of(String fileType, FileSinkConfig fileSinkConfig) {
        try {
            FileFormat fileFormat = FileFormat.valueOf(fileType.toUpperCase());
            return fileFormat.getWriteStrategy(fileSinkConfig);
        } catch (IllegalArgumentException e) {
            String errorMsg =
                    String.format(
                            "File sink connector not support this file type [%s], please check your config",
                            fileType);
            throw new FileConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    public static WriteStrategy of(FileFormat fileFormat, FileSinkConfig fileSinkConfig) {
        return fileFormat.getWriteStrategy(fileSinkConfig);
    }
}

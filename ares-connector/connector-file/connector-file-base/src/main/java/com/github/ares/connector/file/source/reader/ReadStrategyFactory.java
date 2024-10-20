package com.github.ares.connector.file.source.reader;

import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadStrategyFactory {

    private ReadStrategyFactory() {}

    public static ReadStrategy of(ReadonlyConfig readonlyConfig, HadoopConf hadoopConf) {
        ReadStrategy readStrategy =
                of(readonlyConfig.get(BaseSourceConfigOptions.FILE_FORMAT_TYPE).name());
        readStrategy.setPluginConfig(readonlyConfig.toConfig());
        readStrategy.init(hadoopConf);
        return readStrategy;
    }

    public static ReadStrategy of(String fileType) {
        try {
            FileFormat fileFormat = FileFormat.valueOf(fileType.toUpperCase());
            return fileFormat.getReadStrategy();
        } catch (IllegalArgumentException e) {
            String errorMsg =
                    String.format(
                            "File source connector not support this file type [%s], please check your config",
                            fileType);
            throw new FileConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
        }
    }
}

package com.github.ares.connector.file.config;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.common.SinkType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.TimeUtils;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.Serializable;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class BaseFileSinkConfig implements DelimiterConfig, Serializable {
    private static final long serialVersionUID = 1L;
    protected CompressFormat compressFormat = BaseSinkConfig.COMPRESS_CODEC.defaultValue();
    protected String fieldDelimiter = BaseSinkConfig.FIELD_DELIMITER.defaultValue();
    protected String rowDelimiter = BaseSinkConfig.ROW_DELIMITER.defaultValue();
    protected int batchSize = BaseSinkConfig.BATCH_SIZE.defaultValue();
    protected String path;
    protected String fileNameExpression = BaseSinkConfig.FILE_NAME_EXPRESSION.defaultValue();
    protected FileFormat fileFormat = FileFormat.TEXT;
    protected DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;
    protected DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    protected TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;
    protected Boolean enableHeaderWriter = false;

    public BaseFileSinkConfig(@NonNull Config config) {
        SinkType sinkType = config.getEnum(SinkType.class, CommonOptions.SINK_TYPE.key());
        if (SinkType.INSERT != sinkType && SinkType.TRUNCATE != sinkType) {
            throw new AresException(String.format("Unsupported sink type: %s for file sink", sinkType));
        }
        if (config.hasPath(BaseSinkConfig.COMPRESS_CODEC.key())) {
            String compressCodec = config.getString(BaseSinkConfig.COMPRESS_CODEC.key());
            this.compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        if (config.hasPath(BaseSinkConfig.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(BaseSinkConfig.BATCH_SIZE.key());
        }
        if (config.hasPath(BaseSinkConfig.FIELD_DELIMITER.key())
                && StringUtils.isNotEmpty(config.getString(BaseSinkConfig.FIELD_DELIMITER.key()))) {
            this.fieldDelimiter = config.getString(BaseSinkConfig.FIELD_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.ROW_DELIMITER.key())) {
            this.rowDelimiter = config.getString(BaseSinkConfig.ROW_DELIMITER.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_PATH.key())
                && !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_PATH.key()))) {
            this.path = config.getString(BaseSinkConfig.FILE_PATH.key());
        }
        checkNotNull(path);

        if (path.equals(File.separator)) {
            this.path = "";
        }

        if (config.hasPath(BaseSinkConfig.FILE_NAME_EXPRESSION.key())
                && !StringUtils.isBlank(
                config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key()))) {
            this.fileNameExpression = config.getString(BaseSinkConfig.FILE_NAME_EXPRESSION.key());
        }

        if (config.hasPath(BaseSinkConfig.FILE_FORMAT_TYPE.key())
                && !StringUtils.isBlank(config.getString(BaseSinkConfig.FILE_FORMAT_TYPE.key()))) {
            this.fileFormat =
                    FileFormat.valueOf(
                            config.getString(BaseSinkConfig.FILE_FORMAT_TYPE.key())
                                    .toUpperCase(Locale.ROOT));
        }

        if (config.hasPath(BaseSinkConfig.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(config.getString(BaseSinkConfig.DATE_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            config.getString(BaseSinkConfig.DATETIME_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(config.getString(BaseSinkConfig.TIME_FORMAT.key()));
        }

        if (config.hasPath(BaseSinkConfig.ENABLE_HEADER_WRITE.key())) {
            enableHeaderWriter = config.getBoolean(BaseSinkConfig.ENABLE_HEADER_WRITE.key());
        }
    }

    public BaseFileSinkConfig() {
    }
}

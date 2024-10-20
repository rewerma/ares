package com.github.ares.connector.file.source.reader;

import com.github.ares.api.serialization.DeserializationSchema;
import com.github.ares.api.source.Collector;
import com.github.ares.api.table.catalog.CatalogTableUtil;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.CompressFormat;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.format.text.TextDeserializationSchema;
import com.github.ares.format.text.constant.TextFormatConstant;
import com.github.ares.format.text.splitor.CsvLineSplitor;
import com.github.ares.format.text.splitor.DefaultTextLineSplitor;
import com.github.ares.format.text.splitor.TextLineSplitor;
import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TextReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<AresRow> deserializationSchema;
    private String fieldDelimiter = BaseSourceConfigOptions.FIELD_DELIMITER.defaultValue();
    private DateUtils.Formatter dateFormat = BaseSourceConfigOptions.DATE_FORMAT.defaultValue();
    private DateTimeUtils.Formatter datetimeFormat =
            BaseSourceConfigOptions.DATETIME_FORMAT.defaultValue();
    private TimeUtils.Formatter timeFormat = BaseSourceConfigOptions.TIME_FORMAT.defaultValue();
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();
    private TextLineSplitor textLineSplitor;
    private int[] indexes;
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();

    @Override
    public void read(String path, String tableId, Collector<AresRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        InputStream inputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(hadoopFileSystemProxy.getInputStream(path));
                break;
            case NONE:
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
            default:
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
        }

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(inputStream, encoding))) {
            reader.lines()
                    .skip(skipHeaderNumber)
                    .forEach(
                            line -> {
                                try {
                                    AresRow aresRow =
                                            deserializationSchema.deserialize(
                                                    line.getBytes(StandardCharsets.UTF_8));
                                    if (!readColumns.isEmpty()) {
                                        // need column projection
                                        Object[] fields;
                                        if (isMergePartition) {
                                            fields =
                                                    new Object
                                                            [readColumns.size()
                                                            + partitionsMap.size()];
                                        } else {
                                            fields = new Object[readColumns.size()];
                                        }
                                        for (int i = 0; i < indexes.length; i++) {
                                            fields[i] = aresRow.getField(indexes[i]);
                                        }
                                        aresRow = new AresRow(fields);
                                    }
                                    if (isMergePartition) {
                                        int index = aresRowType.getTotalFields();
                                        for (String value : partitionsMap.values()) {
                                            aresRow.setField(index++, value);
                                        }
                                    }
                                    aresRow.setTableId(tableId);
                                    output.collect(aresRow);
                                } catch (IOException e) {
                                    String errorMsg =
                                            String.format(
                                                    "Deserialize this data [%s] failed, please check the origin data",
                                                    line);
                                    throw new FileConnectorException(
                                            FileConnectorErrorCode.DATA_DESERIALIZE_FAILED,
                                            errorMsg,
                                            e);
                                }
                            });
        }
    }

    @Override
    public AresRowType getAresRowTypeInfo(String path) {
        this.aresRowType = CatalogTableUtil.buildSimpleTextSchema();
        this.aresRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), aresRowType);
        initFormatter();
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "When reading json/text/csv files, if user has not specified schema information, "
                            + "Ares will not support column projection");
        }
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(TextFormatConstant.PLACEHOLDER)
                        .textLineSplitor(textLineSplitor);
        if (isMergePartition) {
            deserializationSchema =
                    builder.aresRowType(this.aresRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.aresRowType(this.aresRowType).build();
        }
        return getActualAresRowTypeInfo();
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        AresRowType userDefinedRowTypeWithPartition;
        if (fileNames.isEmpty()) {
            userDefinedRowTypeWithPartition = aresRowType;
        } else {
            userDefinedRowTypeWithPartition = mergePartitionTypes(fileNames.get(0), aresRowType);
        }
        Optional<String> fieldDelimiterOptional =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.FIELD_DELIMITER);
        encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        if (fieldDelimiterOptional.isPresent()) {
            fieldDelimiter = fieldDelimiterOptional.get();
        } else {
            FileFormat fileFormat =
                    FileFormat.valueOf(
                            pluginConfig
                                    .getString(BaseSourceConfigOptions.FILE_FORMAT_TYPE.key())
                                    .toUpperCase());
            if (fileFormat == FileFormat.CSV) {
                fieldDelimiter = ",";
            }
        }
        initFormatter();
        TextDeserializationSchema.Builder builder =
                TextDeserializationSchema.builder()
                        .delimiter(fieldDelimiter)
                        .textLineSplitor(textLineSplitor);
        if (isMergePartition) {
            deserializationSchema =
                    builder.aresRowType(userDefinedRowTypeWithPartition).build();
        } else {
            deserializationSchema = builder.aresRowType(aresRowType).build();
        }
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            AresDataType<?>[] types = new AresDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = aresRowType.indexOf(readColumns.get(i));
                fields[i] = aresRowType.getFieldName(indexes[i]);
                types[i] = aresRowType.getFieldType(indexes[i]);
            }
            this.aresRowType = new AresRowType(fields, types);
            this.aresRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.aresRowType);
        } else {
            this.aresRowType = aresRowType;
            this.aresRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    private void initFormatter() {
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATE_FORMAT.key())) {
            dateFormat =
                    DateUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATE_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.DATETIME_FORMAT.key())) {
            datetimeFormat =
                    DateTimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.DATETIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.TIME_FORMAT.key())) {
            timeFormat =
                    TimeUtils.Formatter.parse(
                            pluginConfig.getString(BaseSourceConfigOptions.TIME_FORMAT.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        if (FileFormat.CSV.equals(
                FileFormat.valueOf(
                        pluginConfig
                                .getString(BaseSourceConfigOptions.FILE_FORMAT_TYPE.key())
                                .toUpperCase()))) {
            textLineSplitor = new CsvLineSplitor();
        } else {
            textLineSplitor = new DefaultTextLineSplitor();
        }
    }
}

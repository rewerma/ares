package com.github.ares.connector.file.source.reader;

import com.github.ares.api.serialization.DeserializationSchema;
import com.github.ares.api.source.Collector;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.CompressFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.format.json.JsonDeserializationSchema;
import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class JsonReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<AresRow> deserializationSchema;
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        super.setAresRowTypeInfo(aresRowType);
        if (isMergePartition) {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.aresRowTypeWithPartition);
        } else {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.aresRowType);
        }
    }

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
                    .forEach(
                            line -> {
                                try {
                                    AresRow aresRow =
                                            deserializationSchema.deserialize(
                                                    line.getBytes(StandardCharsets.UTF_8));
                                    if (isMergePartition) {
                                        int index = aresRowType.getTotalFields();
                                        for (String value : partitionsMap.values()) {
                                            aresRow.setField(index++, value);
                                        }
                                    }
                                    aresRow.setTableId(tableId);
                                    output.collect(aresRow);
                                } catch (IOException e) {
                                    throw CommonError.fileOperationFailed(
                                            "JsonFile", "read", path, e);
                                }
                            });
        }
    }

    @Override
    public AresRowType getAresRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }
}

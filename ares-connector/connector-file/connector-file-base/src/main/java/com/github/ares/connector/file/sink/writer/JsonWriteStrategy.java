package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.serialization.SerializationSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.utils.EncodingUtils;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.format.json.JsonSerializationSchema;
import io.airlift.compress.lzo.LzopCodec;
import lombok.NonNull;
import org.apache.hadoop.fs.FSDataOutputStream; 

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.ares.common.exceptions.CommonErrorCode;

public class JsonWriteStrategy extends AbstractWriteStrategy {
    private final byte[] rowDelimiter;
    private SerializationSchema serializationSchema;
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final Map<String, Boolean> isFirstWrite;
    private final Charset charset;

    public JsonWriteStrategy(FileSinkConfig textFileSinkConfig) {
        super(textFileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.isFirstWrite = new HashMap<>();
        this.charset = EncodingUtils.tryParseCharset(textFileSinkConfig.getEncoding());
        this.rowDelimiter = textFileSinkConfig.getRowDelimiter().getBytes(charset);
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        super.setAresRowTypeInfo(aresRowType);
        this.serializationSchema =
                new JsonSerializationSchema(
                        buildSchemaWithRowType(aresRowType, sinkColumnsIndexInRow), charset);
    }

    @Override
    public void write(@NonNull AresRow aresRow) {
        super.write(aresRow);
        String filePath = getOrCreateFilePathBeingWritten(aresRow);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        try {
            byte[] rowBytes =
                    serializationSchema.serialize(
                            aresRow.copy(
                                    sinkColumnsIndexInRow.stream()
                                            .mapToInt(Integer::intValue)
                                            .toArray()));
            if (isFirstWrite.get(filePath)) {
                isFirstWrite.put(filePath, false);
            } else {
                fsDataOutputStream.write(rowDelimiter);
            }
            fsDataOutputStream.write(rowBytes);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("JsonFile", "write", filePath, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        beingWrittenOutputStream.forEach(
                (key, value) -> {
                    try {
                        value.flush();
                    } catch (IOException e) {
                        throw new FileConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    } finally {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.warn("Close file output stream {} failed", key, e);
                        }
                    }
                    needMoveFiles.put(key, getTargetLocation(key));
                });
        beingWrittenOutputStream.clear();
        isFirstWrite.clear();
    }

    private FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                switch (compressFormat) {
                    case LZO:
                        LzopCodec lzo = new LzopCodec();
                        OutputStream out =
                                lzo.createOutputStream(
                                        hadoopFileSystemProxy.getOutputStream(filePath));
                        fsDataOutputStream = new FSDataOutputStream(out, null);
                        break;
                    case NONE:
                        fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                        break;
                    default:
                        log.warn(
                                "Json file does not support this compress type: {}",
                                compressFormat.getCompressCodec());
                        fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                        break;
                }
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (IOException e) {
                throw CommonError.fileOperationFailed("JsonFile", "open", filePath, e);
            }
        }
        return fsDataOutputStream;
    }
}

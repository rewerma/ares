package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.serialization.SerializationSchema;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.EncodingUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.format.text.TextSerializationSchema;
import io.airlift.compress.lzo.LzopCodec;
import lombok.NonNull;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TextWriteStrategy extends AbstractWriteStrategy {
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private final Map<String, Boolean> isFirstWrite;
    private final String fieldDelimiter;
    private final String rowDelimiter;
    private final DateUtils.Formatter dateFormat;
    private final DateTimeUtils.Formatter dateTimeFormat;
    private final TimeUtils.Formatter timeFormat;
    private final FileFormat fileFormat;
    private final Boolean enableHeaderWriter;
    private final Charset charset;
    private SerializationSchema serializationSchema;

    public TextWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenOutputStream = new LinkedHashMap<>();
        this.isFirstWrite = new HashMap<>();
        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();
        this.rowDelimiter = fileSinkConfig.getRowDelimiter();
        this.dateFormat = fileSinkConfig.getDateFormat();
        this.dateTimeFormat = fileSinkConfig.getDatetimeFormat();
        this.timeFormat = fileSinkConfig.getTimeFormat();
        this.fileFormat = fileSinkConfig.getFileFormat();
        this.enableHeaderWriter = fileSinkConfig.getEnableHeaderWriter();
        this.charset = EncodingUtils.tryParseCharset(fileSinkConfig.getEncoding());
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        super.setAresRowTypeInfo(aresRowType);
        this.serializationSchema =
                TextSerializationSchema.builder()
                        .aresRowType(
                                buildSchemaWithRowType(aresRowType, sinkColumnsIndexInRow))
                        .delimiter(fieldDelimiter)
                        .dateFormatter(dateFormat)
                        .dateTimeFormatter(dateTimeFormat)
                        .timeFormatter(timeFormat)
                        .charset(charset)
                        .build();
    }

    @Override
    public void write(@NonNull AresRow aresRow) {
        super.write(aresRow);
        String filePath = getOrCreateFilePathBeingWritten(aresRow);
        FSDataOutputStream fsDataOutputStream = getOrCreateOutputStream(filePath);
        try {
            if (isFirstWrite.get(filePath)) {
                isFirstWrite.put(filePath, false);
            } else {
                fsDataOutputStream.write(rowDelimiter.getBytes(charset));
            }
            fsDataOutputStream.write(
                    serializationSchema.serialize(
                            aresRow.copy(
                                    sinkColumnsIndexInRow.stream()
                                            .mapToInt(Integer::intValue)
                                            .toArray())));
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("TextFile", "write", filePath, e);
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
                            log.error("error when close output stream {}", key, e);
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
                        enableWriteHeader(fsDataOutputStream);
                        break;
                    case NONE:
                        fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                        enableWriteHeader(fsDataOutputStream);
                        break;
                    default:
                        log.warn(
                                "Text file does not support this compress type: {}",
                                compressFormat.getCompressCodec());
                        fsDataOutputStream = hadoopFileSystemProxy.getOutputStream(filePath);
                        enableWriteHeader(fsDataOutputStream);
                        break;
                }
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (IOException e) {
                throw CommonError.fileOperationFailed("TextFile", "open", filePath, e);
            }
        }
        return fsDataOutputStream;
    }

    private void enableWriteHeader(FSDataOutputStream fsDataOutputStream) throws IOException {
        if (enableHeaderWriter) {
            fsDataOutputStream.write(
                    String.join(
                                    FileFormat.CSV.equals(fileFormat) ? "," : fieldDelimiter,
                                    aresRowType.getFieldNames())
                            .getBytes());
            fsDataOutputStream.write(rowDelimiter.getBytes());
        }
    }
}

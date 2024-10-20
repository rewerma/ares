package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.connector.file.sink.util.ExcelGenerator;
import lombok.NonNull;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.LinkedHashMap;

public class ExcelWriteStrategy extends AbstractWriteStrategy {
    private final LinkedHashMap<String, ExcelGenerator> beingWrittenWriter;

    public ExcelWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(AresRow aresRow) {
        super.write(aresRow);
        String filePath = getOrCreateFilePathBeingWritten(aresRow);
        ExcelGenerator excelGenerator = getOrCreateExcelGenerator(filePath);
        excelGenerator.writeData(aresRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        hadoopFileSystemProxy.createFile(k);
                        FSDataOutputStream fileOutputStream =
                                hadoopFileSystemProxy.getOutputStream(k);
                        v.flushAndCloseExcel(fileOutputStream);
                        fileOutputStream.close();
                    } catch (IOException e) {
                        throw CommonError.fileOperationFailed("ExcelFile", "write", k, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        beingWrittenWriter.clear();
    }

    private ExcelGenerator getOrCreateExcelGenerator(@NonNull String filePath) {
        ExcelGenerator excelGenerator = this.beingWrittenWriter.get(filePath);
        if (excelGenerator == null) {
            excelGenerator =
                    new ExcelGenerator(sinkColumnsIndexInRow, aresRowType, fileSinkConfig);
            this.beingWrittenWriter.put(filePath, excelGenerator);
        }
        return excelGenerator;
    }
}

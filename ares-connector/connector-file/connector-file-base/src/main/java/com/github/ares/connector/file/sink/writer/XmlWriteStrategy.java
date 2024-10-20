package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.connector.file.sink.util.XmlWriter;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * An implementation of the AbstractWriteStrategy class that writes data in XML format.
 *
 * <p>This strategy stores multiple XmlWriter instances for different files being written and
 * ensures that each file is written to only once. It writes the data by passing the data row to the
 * corresponding XmlWriter instance.
 */
public class XmlWriteStrategy extends AbstractWriteStrategy {

    private final LinkedHashMap<String, XmlWriter> beingWrittenWriter;

    public XmlWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(AresRow aresRow) throws FileConnectorException {
        super.write(aresRow);
        String filePath = getOrCreateFilePathBeingWritten(aresRow);
        XmlWriter xmlDocWriter = getOrCreateXmlWriter(filePath);
        xmlDocWriter.writeData(aresRow);
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        hadoopFileSystemProxy.createFile(k);
                        FSDataOutputStream fileOutputStream =
                                hadoopFileSystemProxy.getOutputStream(k);
                        v.flushAndCloseXmlWriter(fileOutputStream);
                        fileOutputStream.close();
                    } catch (IOException e) {
                        throw CommonError.fileOperationFailed("XmlFile", "write", k, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        this.beingWrittenWriter.clear();
    }

    private XmlWriter getOrCreateXmlWriter(String filePath) {
        return beingWrittenWriter.computeIfAbsent(
                filePath,
                k -> new XmlWriter(fileSinkConfig, sinkColumnsIndexInRow, aresRowType));
    }
}

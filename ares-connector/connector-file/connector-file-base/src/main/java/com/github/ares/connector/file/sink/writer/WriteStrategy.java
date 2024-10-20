package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

public interface WriteStrategy extends Transaction, Serializable, Closeable {
    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex);

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param conf hadoop conf
     * @return Configuration
     */
    Configuration getConfiguration(HadoopConf conf);

    /**
     * write aresRow to target datasource
     *
     * @param aresRow aresRow
     * @throws FileConnectorException Exceptions
     */
    void write(AresRow aresRow) throws FileConnectorException;

    /**
     * set aresRowTypeInfo in writer
     *
     * @param aresRowType aresRowType
     */
    void setAresRowTypeInfo(AresRowType aresRowType);

    /**
     * use aresRow generate partition directory
     *
     * @param aresRow aresRow
     * @return the map of partition directory
     */
    LinkedHashMap<String, List<String>> generatorPartitionDir(AresRow aresRow);

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    String generateFileName(String transactionId);

    /** when a transaction is triggered, release resources */
    void finishAndCloseFile();

    /**
     * get current checkpoint id
     *
     * @return checkpoint id
     */
    long getCheckpointId();

    /**
     * get sink configuration
     *
     * @return sink configuration
     */
    FileSinkConfig getFileSinkConfig();

    /**
     * get file system utils
     *
     * @return file system utils
     */
    HadoopFileSystemProxy getHadoopFileSystemProxy();

    /**
     * delete files in target directory
     * @throws IOException
     */
    void truncateFiles() throws IOException;
}

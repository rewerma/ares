
package com.github.ares.connector.file.sink.writer;

import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.state.FileSinkState;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface Transaction extends Serializable {
    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    Optional<FileCommitInfo> prepareCommit();

    /** abort prepare commit operation */
    void abortPrepare();

    /**
     * abort prepare commit operation using transaction id
     *
     * @param transactionId transaction id
     */
    void abortPrepare(String transactionId);

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    List<FileSinkState> snapshotState(long checkpointId);

    /**
     * when a checkpoint triggered, file sink should begin a new transaction
     *
     * @param checkpointId checkpoint id
     */
    void beginTransaction(Long checkpointId);
}

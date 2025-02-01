package com.github.ares.connctor.jdbc.internal.xa;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.connctor.jdbc.state.XidInfo;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public interface XaGroupOps extends Serializable {

    // Commit a batch of transactions
    public GroupXaOperationResult<XidInfo> commit(
            List<XidInfo> xids, boolean allowOutOfOrderCommits, int maxCommitAttempts);

    void rollback(List<XidInfo> xids);

    GroupXaOperationResult<XidInfo> failAndRollback(Collection<XidInfo> xids);

    void recoverAndRollback(
            JobContext context,
            SinkWriter.Context sinkContext,
            XidGenerator xidGenerator,
            Xid excludeXid);
}

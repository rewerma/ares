/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ares.connctor.jdbc.internal.xa;

import com.github.ares.connctor.jdbc.config.JdbcConnectionConfig;
import com.github.ares.connctor.jdbc.internal.connection.JdbcConnectionProvider;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Collection;

/**
 * Typical workflow:
 *
 * <ol>
 *   <li>{@link #open}
 *   <li>{@link #start} transaction
 *   <li>{@link #getConnection}, write some data
 *   <li>{@link #endAndPrepare} (or {@link #failAndRollback})
 *   <li>{@link #commit} / {@link #rollback}
 *   <li>{@link #close}
 * </ol>
 *
 * {@link #recover} can be used to get abandoned prepared transactions for cleanup.
 */
public interface XaFacade extends JdbcConnectionProvider, Serializable, AutoCloseable {

    static XaFacade fromJdbcConnectionOptions(JdbcConnectionConfig jdbcConnectionConfig) {
        return new XaFacadeImplAutoLoad(jdbcConnectionConfig);
    }

    void open() throws Exception;

    boolean isOpen();

    /** Start a new transaction. */
    void start(Xid xid) throws Exception;

    /** End and then prepare the transaction. Transaction can't be resumed afterwards. */
    void endAndPrepare(Xid xid) throws Exception;

    /**
     * Commit previously prepared transaction.
     *
     * @param ignoreUnknown whether to ignore {@link XAException#XAER_NOTA XAER_NOTA} error.
     */
    void commit(Xid xid, boolean ignoreUnknown) throws TransientXaException;

    /** Rollback previously prepared transaction. */
    void rollback(Xid xid) throws TransientXaException;

    /**
     * End transaction as {@link javax.transaction.xa.XAResource#TMFAIL failed}; in case of error,
     * try to roll it back.
     */
    void failAndRollback(Xid xid) throws TransientXaException;

    /**
     * Note: this can block on some non-MVCC databases if there are ended not prepared transactions.
     */
    Collection<Xid> recover() throws TransientXaException;

    /**
     * Thrown by {@link XaFacade} when RM responds with {@link
     * javax.transaction.xa.XAResource#XA_RDONLY XA_RDONLY} indicating that the transaction doesn't
     * include any changes. When such a transaction is committed RM may return an error (usually,
     * {@link XAException#XAER_NOTA XAER_NOTA}).
     */
    class EmptyXaTransactionException extends RuntimeException {
        private final Xid xid;

        EmptyXaTransactionException(Xid xid) {
            super("end response XA_RDONLY, xid: " + xid);
            this.xid = xid;
        }

        public Xid getXid() {
            return xid;
        }
    }

    /**
     * Indicates a transient or unknown failure from the resource manager (see {@link
     * XAException#XA_RBTRANSIENT XA_RBTRANSIENT}, {@link XAException#XAER_RMFAIL XAER_RMFAIL}).
     */
    class TransientXaException extends RuntimeException {
        TransientXaException(XAException cause) {
            super(cause);
        }
    }
}

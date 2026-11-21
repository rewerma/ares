package com.github.ares.engine.spark.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.TransactionalSinkHandler;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class SparkLazyTransactionalSinkHandlerTest {

    @Test
    public void executorInitBindsLazyHandlerWithoutLoadingJdbcHandler() {
        SparkExecutorManager manager = new SparkExecutorManager();
        TransactionalSinkHandler handler = manager.createTransactionalSinkHandler();
        assertTrue(handler instanceof SparkLazyTransactionalSinkHandler);
        SparkLazyTransactionalSinkHandler lazy = (SparkLazyTransactionalSinkHandler) handler;
        assertTrue(lazy.peekDelegate() == null);
        handler.close();
        handler.commit();
        handler.rollbackQuietly();
        assertTrue(lazy.peekDelegate() == null);
    }

    @Test
    public void resolveDelegateLoadsJdbcHandlerWhenPresent() {
        SparkExecutorManager manager = new SparkExecutorManager();
        SparkLazyTransactionalSinkHandler handler = new SparkLazyTransactionalSinkHandler(manager);
        TransactionalSinkHandler delegate = handler.resolveDelegate();
        assertTrue(delegate instanceof SparkJdbcTransactionalSinkHandler);
        assertSame(delegate, handler.resolveDelegate());
    }

    @Test
    public void closeCommitRollbackDoNotCreateDelegate() {
        AtomicInteger created = new AtomicInteger();
        SparkLazyTransactionalSinkHandler handler =
                new SparkLazyTransactionalSinkHandler(
                        new SparkExecutorManager(),
                        manager -> {
                            created.incrementAndGet();
                            throw new AssertionError("delegate must not be created");
                        });
        handler.close();
        handler.commit();
        handler.rollbackQuietly();
        assertTrue(handler.peekDelegate() == null);
        assertTrue(created.get() == 0);
    }

    @Test
    public void writeLoadsDelegateOnceAndDelegates() {
        RecordingHandler recording = new RecordingHandler();
        AtomicInteger created = new AtomicInteger();
        SparkLazyTransactionalSinkHandler handler =
                new SparkLazyTransactionalSinkHandler(
                        new SparkExecutorManager(),
                        manager -> {
                            created.incrementAndGet();
                            return recording;
                        });
        handler.write(null, null, null, "t");
        handler.prepareOverwrite(null, "t");
        handler.commit();
        handler.rollbackQuietly();
        handler.close();
        assertTrue(created.get() == 1);
        assertTrue(recording.writes == 1);
        assertTrue(recording.overwrites == 1);
        assertTrue(recording.commits == 1);
        assertTrue(recording.rollbacks == 1);
        assertTrue(recording.closes == 1);
    }

    @Test
    public void missingJdbcClassIsReportedAsAresException() {
        SparkLazyTransactionalSinkHandler handler =
                new SparkLazyTransactionalSinkHandler(
                        new SparkExecutorManager(),
                        manager -> {
                            throw new ClassNotFoundException(
                                    "com.github.ares.connector.jdbc.sink.JdbcPlTransactionSession");
                        });
        try {
            handler.write(null, null, null, "t");
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(
                    e.getMessage()
                            .contains(
                                    "START TRANSACTION / JDBC transactional sink requires a JDBC connector on the classpath"));
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
        assertTrue(handler.peekDelegate() == null);
    }

    @Test
    public void eagerJdbcHandlerClassHasHardDependency() throws Exception {
        // Documents the original failure: constructing the JDBC handler requires the class.
        Class.forName("com.github.ares.connector.jdbc.sink.JdbcPlTransactionSession");
        new SparkJdbcTransactionalSinkHandler(new SparkExecutorManager());
        assertFalse(SparkJdbcTransactionalSinkHandler.class.getName().isEmpty());
    }

    private static final class RecordingHandler implements TransactionalSinkHandler {
        private int writes;
        private int overwrites;
        private int commits;
        private int rollbacks;
        private int closes;

        @Override
        public void write(
                AresSink<?, ?, ?, ?> sink,
                Object dataset,
                CatalogTable catalogTable,
                String sinkTableName) {
            writes++;
        }

        @Override
        public void prepareOverwrite(AresSink<?, ?, ?, ?> sink, String sinkTableName) {
            overwrites++;
        }

        @Override
        public void commit() {
            commits++;
        }

        @Override
        public void rollbackQuietly() {
            rollbacks++;
        }

        @Override
        public void close() {
            closes++;
        }
    }
}

package com.github.ares.engine.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;
import org.junit.Test;

public class PlTransactionManagerTest {

    @Test
    public void startIsIdempotent() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.start();
        assertTrue(manager.isActive());
        manager.commit();
        assertEquals(1, handler.commits);
        assertTrue(manager.isActive());
        manager.end();
        assertEquals(1, handler.commits);
        assertFalse(manager.isActive());
    }

    @Test
    public void commitWithoutTransactionIsNoOp() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.commit();
        assertEquals(0, handler.commits);
        assertFalse(manager.isActive());
    }

    @Test
    public void rollbackWithoutTransactionIsNoOp() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.rollback();
        assertEquals(0, handler.rollbacks);
    }

    @Test
    public void rollbackClearsActive() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.rollback();
        assertTrue(manager.isActive());
        assertEquals(1, handler.rollbacks);
        manager.rollback();
        assertEquals(1, handler.rollbacks);
        manager.end();
        assertEquals(0, handler.commits);
        assertFalse(manager.isActive());
    }

    @Test
    public void closeRollsBackOnlyWhenActive() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.close();
        assertEquals(1, handler.rollbacks);
        assertEquals(1, handler.closes);
        assertFalse(manager.isActive());
    }

    @Test
    public void closeAfterCommitDoesNotRollback() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.commit();
        manager.close();
        assertEquals(1, handler.commits);
        assertEquals(0, handler.rollbacks);
        assertEquals(1, handler.closes);
    }

    @Test
    public void writeRequiresActiveTransaction() {
        PlTransactionManager manager = new PlTransactionManager();
        manager.init(new RecordingHandler());
        try {
            manager.write(null, null, null, "t");
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(e.getMessage().contains("START TRANSACTION"));
        }
    }

    @Test
    public void commitKeepsSegmentOpenForLaterDml() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.commit();
        manager.write(null, null, null, "t");
        manager.end();
        assertEquals(2, handler.commits);
        assertEquals(1, handler.writes);
        assertFalse(manager.isActive());
    }

    @Test
    public void endCommitsOpenBatch() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.start();
        manager.write(null, null, null, "t");
        manager.end();
        assertEquals(1, handler.commits);
        assertEquals(0, handler.rollbacks);
        assertFalse(manager.isActive());
    }

    @Test
    public void endWithoutSegmentIsNoOp() {
        PlTransactionManager manager = new PlTransactionManager();
        RecordingHandler handler = new RecordingHandler();
        manager.init(handler);
        manager.end();
        assertEquals(0, handler.commits);
        assertFalse(manager.isActive());
    }

    @Test
    public void ensureNotActiveRejectsUntilEnd() {
        PlTransactionManager manager = new PlTransactionManager();
        manager.start();
        manager.commit();
        try {
            manager.ensureNotActive("TRUNCATE");
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(e.getMessage().contains("TRUNCATE"));
        }
        manager.end();
        manager.ensureNotActive("TRUNCATE");
    }

    @Test
    public void ensureNotActiveRejectsOpenTransaction() {
        PlTransactionManager manager = new PlTransactionManager();
        manager.start();
        try {
            manager.ensureNotActive("TRUNCATE");
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(e.getMessage().contains("TRUNCATE"));
        }
    }

    private static final class RecordingHandler implements TransactionalSinkHandler {
        int commits;
        int rollbacks;
        int closes;
        int writes;

        @Override
        public void write(
                AresSink<?, ?, ?, ?> sink,
                Object dataset,
                CatalogTable catalogTable,
                String sinkTableName) {
            writes++;
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

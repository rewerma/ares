package com.github.ares.engine.spark.core;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.engine.core.ExecutorManager;
import com.github.ares.engine.core.TransactionalSinkHandler;
import java.lang.reflect.InvocationTargetException;

/**
 * Defers loading {@link SparkJdbcTransactionalSinkHandler} until a transactional JDBC sink is
 * actually used. Engine init and script-end close must not require {@code JdbcPlTransactionSession}
 * on the classpath (for example a Druid-only {@code --jars} set).
 */
public class SparkLazyTransactionalSinkHandler implements TransactionalSinkHandler {

    interface HandlerFactory {
        TransactionalSinkHandler create(ExecutorManager executorManager) throws Exception;
    }

    static final String JDBC_HANDLER_CLASS =
            "com.github.ares.engine.spark.core.SparkJdbcTransactionalSinkHandler";

    private static final HandlerFactory DEFAULT_FACTORY =
            executorManager -> {
                Class<?> clazz = Class.forName(JDBC_HANDLER_CLASS);
                return (TransactionalSinkHandler)
                        clazz.getConstructor(ExecutorManager.class).newInstance(executorManager);
            };

    private final ExecutorManager executorManager;
    private final HandlerFactory factory;
    private volatile TransactionalSinkHandler delegate;

    public SparkLazyTransactionalSinkHandler(ExecutorManager executorManager) {
        this(executorManager, DEFAULT_FACTORY);
    }

    SparkLazyTransactionalSinkHandler(ExecutorManager executorManager, HandlerFactory factory) {
        this.executorManager = executorManager;
        this.factory = factory;
    }

    TransactionalSinkHandler resolveDelegate() {
        TransactionalSinkHandler current = delegate;
        if (current == null) {
            synchronized (this) {
                current = delegate;
                if (current == null) {
                    current = load();
                    delegate = current;
                }
            }
        }
        return current;
    }

    TransactionalSinkHandler peekDelegate() {
        return delegate;
    }

    private TransactionalSinkHandler load() {
        try {
            return factory.create(executorManager);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw missingJdbc(e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof ClassNotFoundException || cause instanceof NoClassDefFoundError) {
                throw missingJdbc(cause);
            }
            throw new AresException("Failed to initialize JDBC transactional sink handler", e);
        } catch (Exception e) {
            if (e instanceof AresException) {
                throw (AresException) e;
            }
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof ClassNotFoundException || cause instanceof NoClassDefFoundError) {
                throw missingJdbc(cause);
            }
            throw new AresException("Failed to initialize JDBC transactional sink handler", e);
        }
    }

    private static AresException missingJdbc(Throwable cause) {
        return new AresException(
                "START TRANSACTION / JDBC transactional sink requires a JDBC connector on the classpath",
                cause);
    }

    @Override
    public void write(
            AresSink<?, ?, ?, ?> sink,
            Object dataset,
            CatalogTable catalogTable,
            String sinkTableName) {
        resolveDelegate().write(sink, dataset, catalogTable, sinkTableName);
    }

    @Override
    public void prepareOverwrite(AresSink<?, ?, ?, ?> sink, String sinkTableName) {
        resolveDelegate().prepareOverwrite(sink, sinkTableName);
    }

    @Override
    public void commit() {
        TransactionalSinkHandler current = delegate;
        if (current != null) {
            current.commit();
        }
    }

    @Override
    public void rollbackQuietly() {
        TransactionalSinkHandler current = delegate;
        if (current != null) {
            current.rollbackQuietly();
        }
    }

    @Override
    public void close() {
        TransactionalSinkHandler current = delegate;
        if (current != null) {
            current.close();
        }
    }
}

package com.github.ares.common.engine;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static com.github.ares.com.google.common.base.Preconditions.checkNotNull;

public final class Handover<T> implements Closeable {
    private static final int DEFAULT_QUEUE_SIZE = 10000;
    private final Object lock = new Object();
    private final LinkedBlockingQueue<T> blockingQueue =
            new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
    private Throwable error;

    public boolean isEmpty() {
        return blockingQueue.isEmpty();
    }

    public Optional<T> pollNext() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        } else if (!isEmpty()) {
            return Optional.ofNullable(blockingQueue.poll());
        }
        return Optional.empty();
    }

    public void produce(final T element) throws InterruptedException, ClosedException {
        if (error != null) {
            throw new ClosedException();
        }
        blockingQueue.put(element);
    }

    public void reportError(Throwable t) {
        checkNotNull(t);

        synchronized (lock) {
            // do not override the initial exception
            if (error == null) {
                error = t;
            }
            lock.notifyAll();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (error == null) {
                error = new ClosedException();
            }
            lock.notifyAll();
        }
    }

    public static void rethrowException(Throwable t, String parentMessage) throws Exception {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof Exception) {
            throw (Exception) t;
        } else {
            throw new Exception(parentMessage, t);
        }
    }

    public static final class ClosedException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}

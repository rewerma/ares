package com.github.ares.api.sink;

import java.util.Optional;

public interface SupportResourceShare<T> {

    default MultiTableResourceManager<T> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return null;
    }

    default void setMultiTableResourceManager(
            MultiTableResourceManager<T> multiTableResourceManager, int queueIndex) {}
}

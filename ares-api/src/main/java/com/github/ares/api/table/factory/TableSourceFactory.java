package com.github.ares.api.table.factory;

import com.github.ares.api.source.AresSource;
import com.github.ares.api.source.SourceSplit;
import com.github.ares.api.source.TableSource;

import java.io.Serializable;

/**
 * This is an SPI interface, used to create {@link TableSource}. Each plugin need to have it own
 * implementation.
 */
public interface TableSourceFactory extends Factory {

    /**
     * We will never use this method now. So gave a default implement and return null.
     *
     * @param context TableFactoryContext
     */
    default <T, SplitT extends SourceSplit, StateT extends Serializable>
    TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        throw new UnsupportedOperationException(
                "The Factory has not been implemented and the deprecated Plugin will be used.");
    }

    /**
     * TODO: Implement SupportParallelism in the TableSourceFactory instead of the AresSource,
     * Then deprecated the method
     */
    Class<? extends AresSource> getSourceClass();
}

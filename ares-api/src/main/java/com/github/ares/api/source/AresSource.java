package com.github.ares.api.source;

import com.github.ares.api.common.JobContext;
import com.github.ares.api.common.PluginIdentifierInterface;
import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.serialization.DefaultSerializer;
import com.github.ares.common.serialization.Serializer;

import java.io.Serializable;
import java.util.List;

/**
 * The interface for Source. It acts like a factory class that helps construct the {@link
 * SourceSplitEnumerator} and {@link SourceReader} and corresponding serializers.
 *
 * @param <T>      The type of records produced by the source.
 * @param <SplitT> The type of splits handled by the source.
 * @param <StateT> The type of checkpoint states.
 */
public interface AresSource<T, SplitT extends SourceSplit, StateT extends Serializable>
        extends Serializable,
        PluginIdentifierInterface {

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    Boundedness getBoundedness();

    /**
     * Get the data type of the records produced by this source.
     *
     * @return Ares data type.
     * @deprecated Please use {@link #getProducedCatalogTables}
     */
    @Deprecated
    default AresDataType<T> getProducedType() {
        return (AresDataType) getProducedCatalogTables().get(0).getAresRowType();
    }

    /**
     * Get the catalog tables output by this source, It is recommended that all connectors implement
     * this method instead of {@link #getProducedType}. CatalogTable contains more information to
     * help downstream support more accurate and complete synchronization capabilities.
     */
    default List<CatalogTable> getProducedCatalogTables() {
        throw new UnsupportedOperationException(
                "getProducedCatalogTables method has not been implemented.");
    }

    /**
     * Create source reader, used to produce data.
     *
     * @param readerContext reader context.
     * @return source reader.
     * @throws Exception when create reader failed.
     */
    SourceReader<T, SplitT> createReader(SourceReader.Context readerContext) throws Exception;

    /**
     * Create split serializer, use to serialize/deserialize split generated by {@link
     * SourceSplitEnumerator}.
     *
     * @return split serializer.
     */
    default Serializer<SplitT> getSplitSerializer() {
        return new DefaultSerializer<>();
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called only once
     * when start a source.
     *
     * @param enumeratorContext enumerator context.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    SourceSplitEnumerator<SplitT, StateT> createEnumerator(
            SourceSplitEnumerator.Context<SplitT> enumeratorContext) throws Exception;

    /**
     * Create source split enumerator, used to generate splits. This method will be called when
     * restore from checkpoint.
     *
     * @param enumeratorContext enumerator context.
     * @param checkpointState   checkpoint state.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    SourceSplitEnumerator<SplitT, StateT> restoreEnumerator(
            SourceSplitEnumerator.Context<SplitT> enumeratorContext, StateT checkpointState)
            throws Exception;

    /**
     * Create enumerator state serializer, used to serialize/deserialize checkpoint state.
     *
     * @return enumerator state serializer.
     */
    default Serializer<StateT> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    default void setJobContext(JobContext jobContext) {
        // nothing
    }

    @Deprecated
    default void prepare(Config pluginConfig) {
        throw new UnsupportedOperationException("prepare method is not supported");
    }
}

package com.github.ares.connector.file.source.reader;

import com.github.ares.api.source.Collector;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.exceptions.AresRuntimeException;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;

public interface ReadStrategy extends Serializable, Closeable {
    void init(HadoopConf conf);

    void read(String path, String tableId, Collector<AresRow> output)
            throws IOException, FileConnectorException;

    AresRowType getAresRowTypeInfo(String path) throws FileConnectorException;

    default AresRowType getAresRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        return getAresRowTypeInfo(path);
    }

    default AresRowType getAresRowTypeInfoWithUserConfigRowType(
            String path, AresRowType rowType) throws FileConnectorException {
        return getAresRowTypeInfo(path);
    }

    // todo: use CatalogTable
    void setAresRowTypeInfo(AresRowType aresRowType);

    default List<String> getFileNamesByPath(String path) throws IOException {
        return getFileNamesByPath(path, null);
    }

    List<String> getFileNamesByPath(String path, FileFormat fileFormat) throws IOException;

    // todo: use ReadonlyConfig
    void setPluginConfig(Config pluginConfig);

    // todo: use CatalogTable
    AresRowType getActualAresRowTypeInfo();

    default <T> void buildColumnsWithErrorCheck(
            TablePath tablePath, Iterator<T> keys, Consumer<T> getDataType) {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (keys.hasNext()) {
            try {
                getDataType.accept(keys.next());
            } catch (AresRuntimeException e) {
                if (e.getAresErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_ARES_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    this.getClass().getSimpleName().replace("ReadStrategy", ""),
                    tablePath.getFullName(),
                    unsupported);
        }
    }
}

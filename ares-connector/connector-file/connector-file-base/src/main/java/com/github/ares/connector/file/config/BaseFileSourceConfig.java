package com.github.ares.connector.file.config;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.CatalogTableUtil;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.source.reader.ReadStrategy;
import com.github.ares.connector.file.source.reader.ReadStrategyFactory;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
public abstract class BaseFileSourceConfig implements Serializable {

    private static final long serialVersionUID = -1L;

    private CatalogTable catalogTable;
    private final FileFormat fileFormat;
    private final ReadStrategy readStrategy;
    private final ReadonlyConfig baseFileSourceConfig;
    private List<String> filePaths;

    public abstract HadoopConf getHadoopConfig();

    public abstract String getPluginName();

    public BaseFileSourceConfig(ReadonlyConfig readonlyConfig) {
        this.baseFileSourceConfig = readonlyConfig;
        this.fileFormat = readonlyConfig.get(BaseSourceConfigOptions.FILE_FORMAT_TYPE);
        this.readStrategy = ReadStrategyFactory.of(readonlyConfig, getHadoopConfig());

        this.filePaths = parseFilePaths(readonlyConfig);
        this.catalogTable = parseCatalogTable(readonlyConfig);
    }

    private List<String> parseFilePaths(ReadonlyConfig readonlyConfig) {
        String rootPath = null;
        try {
            rootPath = readonlyConfig.get(BaseSourceConfigOptions.FILE_PATH);
            return readStrategy.getFileNamesByPath(rootPath, fileFormat);
        } catch (Exception ex) {
            String errorMsg = String.format("Get file list from this path [%s] failed", rootPath);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, ex);
        }
    }

    private CatalogTable parseCatalogTable(ReadonlyConfig readonlyConfig) {
        final CatalogTable catalogTable;
        boolean configSchema = readonlyConfig.getOptional(TableSchemaOptions.SCHEMA).isPresent();
        if (configSchema) {
            catalogTable = CatalogTableUtil.buildWithConfig(getPluginName(), readonlyConfig);
        } else {
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
        }
        if (CollectionUtils.isEmpty(getFilePaths())) {
            return catalogTable;
        }
        switch (fileFormat) {
            case CSV:
            case TEXT:
            case JSON:
            case EXCEL:
            case XML:
                readStrategy.setAresRowTypeInfo(catalogTable.getAresRowType());
                return newCatalogTable(catalogTable, readStrategy.getActualAresRowTypeInfo());
            case ORC:
            case PARQUET:
                return newCatalogTable(
                        catalogTable,
                        readStrategy.getAresRowTypeInfoWithUserConfigRowType(
                                getFilePaths().get(0),
                                configSchema ? catalogTable.getAresRowType() : null));
            default:
                throw new FileConnectorException(
                        FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                        "Ares does not supported this file format: [" + fileFormat + "]");
        }
    }

    private CatalogTable newCatalogTable(
            CatalogTable catalogTable, AresRowType aresRowType) {
        TableSchema tableSchema = catalogTable.getTableSchema();

        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        String[] fieldNames = aresRowType.getFieldNames();
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();

        List<Column> finalColumns = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Column column = columnMap.get(fieldNames[i]);
            if (column != null) {
                finalColumns.add(column);
            } else {
                finalColumns.add(
                        PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, false, null, null));
            }
        }

        TableSchema finalSchema =
                TableSchema.builder()
                        .columns(finalColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                finalSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }

    public List<String> refreshFilePaths() {
        filePaths = parseFilePaths(baseFileSourceConfig);
        catalogTable = parseCatalogTable(baseFileSourceConfig);
        return filePaths;
    }
}

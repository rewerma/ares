package com.github.ares.api.table.catalog;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.api.table.catalog.schema.ReadonlyConfigParser;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.factory.FactoryUtil;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.MultipleRowType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresException;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utils contains some common methods for construct CatalogTable. */
@Slf4j
public class CatalogTableUtil implements Serializable {

    private static final AresRowType SIMPLE_SCHEMA =
            new AresRowType(
                    new String[] {"content"}, new AresDataType<?>[] {BasicType.STRING_TYPE});

    @Deprecated
    public static CatalogTable getCatalogTable(String tableName, AresRowType rowType) {
        return getCatalogTable("schema", "default", null, tableName, rowType);
    }

    public static CatalogTable getCatalogTable(
            String catalog,
            String database,
            String schema,
            String tableName,
            AresRowType rowType) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            PhysicalColumn column =
                    PhysicalColumn.of(
                            rowType.getFieldName(i), rowType.getFieldType(i), 0, true, null, null);
            schemaBuilder.column(column);
        }
        return CatalogTable.of(
                TableIdentifier.of(catalog, database, schema, tableName),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "It is converted from RowType and only has column information.");
    }

    /**
     * Get catalog table from config, if schema is specified, return a catalog table with specified
     * schema, otherwise, return a catalog table with schema from catalog.
     *
     * @deprecated DO NOT invoke it in any new TableSourceFactory/TableSinkFactory, please directly
     *     use TableSourceFactory/TableSinkFactory instance to get CatalogTable. We just use it to
     *     transition the old CatalogTable creation logic. Details please <a
     *     href="https://cwiki.apache.org/confluence/display/ARES/STIP5-Refactor+Catalog+and+CatalogTable">check
     *     </a>
     */
    @Deprecated
    public static List<CatalogTable> getCatalogTables(
            ReadonlyConfig readonlyConfig, ClassLoader classLoader) {

        // We use plugin_name as factoryId, so MySQL-CDC should be MySQL
        String factoryId = readonlyConfig.get(CommonOptions.PLUGIN_NAME).replace("-CDC", "");
        return getCatalogTables(factoryId, readonlyConfig, classLoader);
    }

    @Deprecated
    public static List<CatalogTable> getCatalogTables(
            String factoryId, ReadonlyConfig readonlyConfig, ClassLoader classLoader) {
        // Highest priority: specified schema
        Map<String, Object> schemaMap = readonlyConfig.get(TableSchemaOptions.SCHEMA);
        if (schemaMap != null) {
            if (schemaMap.isEmpty()) {
                throw new AresException("Schema config can not be empty");
            }
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(factoryId, readonlyConfig);
            return Collections.singletonList(catalogTable);
        }

        Optional<Catalog> optionalCatalog =
                FactoryUtil.createOptionalCatalog(
                        factoryId, readonlyConfig, classLoader, factoryId);
        return optionalCatalog
                .map(
                        c -> {
                            try (Catalog catalog = c) {
                                long startTime = System.currentTimeMillis();
                                catalog.open();
                                List<CatalogTable> catalogTables =
                                        catalog.getTables(readonlyConfig);
                                log.info(
                                        String.format(
                                                "Get catalog tables, cost time: %d ms",
                                                System.currentTimeMillis() - startTime));
                                if (catalogTables.isEmpty()) {
                                    throw new AresException(
                                            String.format(
                                                    "Can not find catalog table with factoryId [%s]",
                                                    factoryId));
                                }
                                return catalogTables;
                            }
                        })
                .orElseThrow(
                        () ->
                                new AresException(
                                        String.format(
                                                "Can not find catalog with factoryId [%s]",
                                                factoryId)));
    }

    public static CatalogTable buildWithConfig(Config config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        return buildWithConfig(readonlyConfig);
    }

    public static AresDataType<AresRow> convertToDataType(
            List<CatalogTable> catalogTables) {
        if (catalogTables.size() == 1) {
            return catalogTables.get(0).getTableSchema().toPhysicalRowDataType();
        } else {
            return convertToMultipleRowType(catalogTables);
        }
    }

    public static MultipleRowType convertToMultipleRowType(List<CatalogTable> catalogTables) {
        Map<String, AresRowType> rowTypeMap = new HashMap<>();
        for (CatalogTable catalogTable : catalogTables) {
            String tableId = catalogTable.getTableId().toTablePath().toString();
            rowTypeMap.put(tableId, catalogTable.getTableSchema().toPhysicalRowDataType());
        }
        return new MultipleRowType(rowTypeMap);
    }

    // We need to use buildWithConfig(String catalogName, ReadonlyConfig readonlyConfig);
    // Since this method will not inject the correct catalogName into CatalogTable
    @Deprecated
    public static List<CatalogTable> convertDataTypeToCatalogTables(
            AresDataType<?> aresDataType, String tableId) {
        List<CatalogTable> catalogTables;
        if (aresDataType instanceof MultipleRowType) {
            catalogTables = new ArrayList<>();
            for (String id : ((MultipleRowType) aresDataType).getTableIds()) {
                catalogTables.add(
                        CatalogTableUtil.getCatalogTable(
                                id, ((MultipleRowType) aresDataType).getRowType(id)));
            }
        } else {
            catalogTables =
                    Collections.singletonList(
                            CatalogTableUtil.getCatalogTable(
                                    tableId, (AresRowType) aresDataType));
        }
        return catalogTables;
    }

    public static CatalogTable buildWithConfig(ReadonlyConfig readonlyConfig) {
        return buildWithConfig("", readonlyConfig);
    }

    public static CatalogTable buildWithConfig(String catalogName, ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(TableSchemaOptions.SCHEMA) == null) {
            throw new RuntimeException(
                    "Schema config need option [schema], please correct your config first");
        }
        TableSchema tableSchema = new ReadonlyConfigParser().parse(readonlyConfig);

        ReadonlyConfig schemaConfig =
                readonlyConfig
                        .getOptional(TableSchemaOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .orElseThrow(
                                () -> new IllegalArgumentException("Schema config can't be null"));

        TablePath tablePath;
        if (StringUtils.isNotEmpty(
                schemaConfig.get(TableSchemaOptions.TableIdentifierOptions.TABLE))) {
            tablePath =
                    TablePath.of(
                            schemaConfig.get(TableSchemaOptions.TableIdentifierOptions.TABLE),
                            schemaConfig.get(
                                    TableSchemaOptions.TableIdentifierOptions.SCHEMA_FIRST));
        } else {
            Optional<String> resultTableNameOptional =
                    readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME);
            tablePath = resultTableNameOptional.map(TablePath::of).orElse(TablePath.DEFAULT);
        }

        return CatalogTable.of(
                TableIdentifier.of(catalogName, tablePath),
                tableSchema,
                new HashMap<>(),
                // todo: add partitionKeys?
                new ArrayList<>(),
                readonlyConfig.get(TableSchemaOptions.TableIdentifierOptions.COMMENT));
    }

    public static AresRowType buildSimpleTextSchema() {
        return SIMPLE_SCHEMA;
    }

    public static CatalogTable buildSimpleTextTable() {
        return getCatalogTable("default", buildSimpleTextSchema());
    }
}

package com.github.ares.connctor.example.sink;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.TableSink;
import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.api.table.factory.TableSinkFactoryContext;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.configuration.utils.OptionRule;
import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(Factory.class)
public class ExampleSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "example";
    }

    @Override
    public OptionRule optionRule() {
        return null;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        // List<Column> columns = catalogTable.getTableSchema().getColumns();
        final CatalogTable finalCatalogTable = context.getCatalogTable();

        return () -> new ExampleSink(config, finalCatalogTable.getTableSchema().toPhysicalRowDataType());
    }
}

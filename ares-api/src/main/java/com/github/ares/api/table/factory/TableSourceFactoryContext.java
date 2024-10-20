package com.github.ares.api.table.factory;

import com.github.ares.common.configuration.ReadonlyConfig;

public class TableSourceFactoryContext extends TableFactoryContext {

    public TableSourceFactoryContext(ReadonlyConfig options, ClassLoader classLoader) {
        super(options, classLoader);
    }
}

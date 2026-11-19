package com.github.ares.connector.jdbc.sink;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSinkFactory;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class Gbase8sSinkFactory extends JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.GBASE_8S;
    }
}

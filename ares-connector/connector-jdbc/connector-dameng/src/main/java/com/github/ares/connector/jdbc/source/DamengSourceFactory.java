package com.github.ares.connector.jdbc.source;

import com.github.ares.api.table.factory.Factory;
import com.github.ares.api.table.factory.TableSourceFactory;
import com.github.ares.connector.jdbc.internal.dialect.DatabaseIdentifier;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(Factory.class)
public class DamengSourceFactory extends JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.DAMENG;
    }
}

package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.internal.converter.AbstractJdbcRowConverter;

public class Gbase8sJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.GBASE_8S;
    }
}

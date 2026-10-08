package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.internal.converter.AbstractJdbcRowConverter;

public class OceanBaseJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.OCEANBASE;
    }
}

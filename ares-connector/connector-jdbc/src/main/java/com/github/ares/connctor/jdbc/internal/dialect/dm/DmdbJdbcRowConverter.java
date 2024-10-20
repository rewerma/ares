package com.github.ares.connctor.jdbc.internal.dialect.dm;

import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;

public class DmdbJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.DAMENG;
    }
}

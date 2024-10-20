package com.github.ares.connctor.jdbc.internal.dialect.vertica;

import com.github.ares.connctor.jdbc.internal.converter.AbstractJdbcRowConverter;
import com.github.ares.connctor.jdbc.internal.dialect.DatabaseIdentifier;

public class VerticaJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.VERTICA;
    }
}

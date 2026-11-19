package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.BasicTypeDefine;

public class SybaseTypeMapper implements JdbcDialectTypeMapper {
    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return SybaseTypeConverter.INSTANCE.convert(typeDefine);
    }
}

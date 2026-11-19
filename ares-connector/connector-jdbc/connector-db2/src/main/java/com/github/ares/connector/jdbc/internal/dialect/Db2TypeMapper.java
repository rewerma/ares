package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.BasicTypeDefine;

public class Db2TypeMapper implements JdbcDialectTypeMapper {
    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return Db2TypeConverter.INSTANCE.convert(typeDefine);
    }
}

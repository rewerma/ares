package com.github.ares.connctor.jdbc.internal.dialect.db2;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DB2TypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return DB2TypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length((long) precision)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}

package com.github.ares.connctor.jdbc.internal.dialect.sqlserver;

import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.connctor.jdbc.internal.dialect.JdbcDialectTypeMapper;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class SqlserverTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return SqlServerTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        if ("float".equalsIgnoreCase(nativeType) && precision == 15) {
            // char length -> max precision
            // float(1-24) char length is 7, float(25-53) char length is 15
            // float(1-24) byte length is 4, float(25-53) char length is 8
            precision = 53;
        } else if (Arrays.asList("nchar", "nvarchar").contains(nativeType)) {
            // e.g nvarchar(10) the char length is 10, but byte length is 20
            precision = precision * 2;
        }

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

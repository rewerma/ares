package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.source.TypeDefineUtils;
import com.github.ares.api.table.catalog.Column;
import com.github.ares.api.table.connector.BasicTypeDefine;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

public class DamengTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return DamengTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        if ("number".equalsIgnoreCase(nativeType) && scale == -127) {
            nativeType = "float";
        } else if (Arrays.asList("NVARCHAR2", "NCHAR", "NVARCHAR").contains(nativeType)) {
            precision = TypeDefineUtils.charToDoubleByteLength(precision);
        } else if (Arrays.asList("CHAR", "CHARACTER", "VARCHAR", "VARCHAR2").contains(nativeType)) {
            precision = TypeDefineUtils.charTo4ByteLength(precision);
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}

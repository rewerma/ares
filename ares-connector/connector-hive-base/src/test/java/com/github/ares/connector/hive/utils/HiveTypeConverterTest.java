package com.github.ares.connector.hive.utils;

import com.github.ares.api.table.catalog.AresDataTypeConvertorUtil;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.SqlType;
import org.junit.Assert;
import org.junit.Test;

public class HiveTypeConverterTest {

    private static final String PLUGIN = "Hive";

    @Test
    public void convertPrimitiveTypes() {
        Assert.assertEquals("int", HiveTypeConverter.convert(PLUGIN, "id", "int"));
        Assert.assertEquals("bigint", HiveTypeConverter.convert(PLUGIN, "id", "bigint"));
        Assert.assertEquals("string", HiveTypeConverter.convert(PLUGIN, "name", "varchar(32)"));
        Assert.assertEquals("string", HiveTypeConverter.convert(PLUGIN, "code", "char(10)"));
        Assert.assertEquals("bytes", HiveTypeConverter.convert(PLUGIN, "payload", "binary"));
        Assert.assertEquals("timestamp", HiveTypeConverter.convert(PLUGIN, "ts", "timestamp"));
        Assert.assertEquals(
                "decimal(10,2)", HiveTypeConverter.convert(PLUGIN, "amount", "decimal(10,2)"));
    }

    @Test
    public void convertComplexTypes() {
        Assert.assertEquals(
                "array<int>", HiveTypeConverter.convert(PLUGIN, "ids", "array<int>"));
        Assert.assertEquals(
                "map<string,int>",
                HiveTypeConverter.convert(PLUGIN, "kv", "map<string,int>"));
        Assert.assertEquals(
                "{id:int,name:string}",
                HiveTypeConverter.convert(PLUGIN, "row", "struct<id:int,name:string>"));
    }

    @Test
    public void convertedTypeCanBeDeserialized() {
        String aresType = HiveTypeConverter.convert(PLUGIN, "amount", "decimal(10,2)");
        Assert.assertEquals(
                SqlType.DECIMAL,
                AresDataTypeConvertorUtil.deserializeAresDataType("amount", aresType)
                        .getSqlType());
        Assert.assertEquals(
                BasicType.STRING_TYPE,
                AresDataTypeConvertorUtil.deserializeAresDataType(
                        "name", HiveTypeConverter.convert(PLUGIN, "name", "varchar(20)")));
    }
}

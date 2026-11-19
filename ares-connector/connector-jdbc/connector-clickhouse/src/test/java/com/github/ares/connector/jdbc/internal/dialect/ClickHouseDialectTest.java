package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.type.BasicType;
import org.junit.Assert;
import org.junit.Test;

public class ClickHouseDialectTest {

    @Test
    public void quotesDatabaseAndTable() {
        Assert.assertEquals(
                "`demo`.`t_user`", new ClickHouseDialect().quoteIdentifier("demo.t_user"));
        Assert.assertTrue(
                new ClickHouseDialectFactory().acceptsURL("jdbc:clickhouse://127.0.0.1:8123/demo"));
        Assert.assertTrue(new ClickHouseDialect().hashModForField("id", 8).contains("cityHash64"));
    }

    @Test
    public void unwrapsNullableInt64() {
        Assert.assertEquals(
                BasicType.LONG_TYPE,
                ClickHouseTypeConverter.INSTANCE
                        .convert(
                                BasicTypeDefine.builder()
                                        .name("id")
                                        .dataType("Nullable(Int64)")
                                        .build())
                        .getDataType());
    }
}

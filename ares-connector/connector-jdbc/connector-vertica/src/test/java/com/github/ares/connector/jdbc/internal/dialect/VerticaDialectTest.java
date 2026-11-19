package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.type.BasicType;
import org.junit.Assert;
import org.junit.Test;

public class VerticaDialectTest {

    @Test
    public void quotesIdentifiersAndAcceptsUrl() {
        Assert.assertEquals(
                "\"public\".\"store_sales\"",
                new VerticaDialect().quoteIdentifier("public.store_sales"));
        Assert.assertTrue(
                new VerticaDialectFactory().acceptsURL("jdbc:vertica://10.0.0.7:5433/vmart"));
    }

    @Test
    public void keepsMultiWordTypes() {
        Assert.assertEquals("LONG VARCHAR", VerticaTypeConverter.normalize("long varchar(100)"));
        Assert.assertEquals("DOUBLE PRECISION", VerticaTypeConverter.normalize("double precision"));
        Assert.assertEquals(
                BasicType.INT_TYPE,
                VerticaTypeConverter.INSTANCE
                        .convert(BasicTypeDefine.builder().name("id").dataType("Integer").build())
                        .getDataType());
    }
}

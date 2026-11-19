package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.type.BasicType;
import org.junit.Assert;
import org.junit.Test;

public class SybaseDialectTest {

    @Test
    public void quotesWithBracketsAndUsesTop() {
        Assert.assertEquals("[dbo].[authors]", new SybaseDialect().quoteIdentifier("dbo.authors"));
        Assert.assertTrue(
                new SybaseDialectFactory().acceptsURL("jdbc:jtds:sybase://10.0.0.1:5000/pubs2"));
        String sql = SybaseDialect.nextChunkSql("[id]", "[dbo].[authors]", 100);
        Assert.assertTrue(sql.contains("SELECT TOP 100 [id]"));
        Assert.assertFalse(sql.contains("LIMIT"));
    }

    @Test
    public void mapsInt() {
        Assert.assertEquals(
                BasicType.INT_TYPE,
                SybaseTypeConverter.INSTANCE
                        .convert(BasicTypeDefine.builder().name("id").dataType("int").build())
                        .getDataType());
    }
}

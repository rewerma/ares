package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.api.table.catalog.PhysicalColumn;
import com.github.ares.api.table.connector.BasicTypeDefine;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.SqlType;
import org.junit.Assert;
import org.junit.Test;

public class Db2DialectTest {

    @Test
    public void quotesSchemaAndTable() {
        Assert.assertEquals("\"APP\".\"T1\"", new Db2Dialect().quoteIdentifier("APP.T1"));
        Assert.assertTrue(new Db2DialectFactory().acceptsURL("jdbc:db2://10.0.0.1:50000/SAMPLE"));
    }

    @Test
    public void chunkQueryUsesFetchFirst() {
        String sql = Db2Dialect.nextChunkSql("\"ID\"", "\"APP\".\"T1\"", 1000);
        Assert.assertTrue(sql.contains("FETCH FIRST 1000 ROWS ONLY"));
        Assert.assertFalse(sql.contains("LIMIT"));
    }

    @Test
    public void mapsIntegerAndDecimal() {
        ColumnAssert.integer(
                Db2TypeConverter.INSTANCE.convert(
                        BasicTypeDefine.builder().name("ID").dataType("INTEGER").build()));
        BasicTypeDefine decimal =
                Db2TypeConverter.INSTANCE.reconvert(
                        PhysicalColumn.builder()
                                .name("AMT")
                                .dataType(new com.github.ares.api.table.type.DecimalType(10, 2))
                                .build());
        Assert.assertEquals("DECIMAL(10,2)", decimal.getColumnType());
        Assert.assertEquals(SqlType.INT, BasicType.INT_TYPE.getSqlType());
    }

    private static final class ColumnAssert {
        private static void integer(com.github.ares.api.table.catalog.Column column) {
            Assert.assertEquals(BasicType.INT_TYPE, column.getDataType());
        }
    }
}

package com.github.ares.connector.jdbc.internal.dialect;

import org.junit.Assert;
import org.junit.Test;

public class OscarDialectTest {

    @Test
    public void quotesIdentifiersAndAcceptsUrl() {
        Assert.assertEquals(
                "\"APP\".\"T_ORDER\"", new OscarDialect().quoteIdentifier("APP.T_ORDER"));
        Assert.assertEquals("Oscar", new OscarDialect().dialectName());
        Assert.assertTrue(
                new OscarDialectFactory().acceptsURL("jdbc:oscar://127.0.0.1:2003/OSRDB"));
        Assert.assertFalse(
                new OscarDialectFactory().acceptsURL("jdbc:oracle:thin:@//127.0.0.1:1521/ORCL"));
    }
}

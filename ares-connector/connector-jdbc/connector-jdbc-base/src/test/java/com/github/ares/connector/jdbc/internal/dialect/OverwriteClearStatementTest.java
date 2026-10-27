package com.github.ares.connector.jdbc.internal.dialect;

import com.github.ares.connector.jdbc.internal.converter.JdbcRowConverter;
import org.junit.Assert;
import org.junit.Test;

public class OverwriteClearStatementTest {

    @Test
    public void deleteWhenTruncateCommitsImmediately() {
        Assert.assertEquals(
                "DELETE FROM t_user",
                dialect(DatabaseIdentifier.MYSQL).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "DELETE FROM t_user",
                dialect(DatabaseIdentifier.ORACLE).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "DELETE FROM t_user",
                dialect(DatabaseIdentifier.OCEANBASE).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "DELETE FROM t_user",
                dialect(DatabaseIdentifier.DAMENG).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "DELETE FROM t_user",
                dialect(DatabaseIdentifier.DORIS).overwriteClearStatement("t_user"));
    }

    @Test
    public void truncateWhenTheDatabaseCanRollItBack() {
        Assert.assertEquals(
                "TRUNCATE TABLE t_user",
                dialect(DatabaseIdentifier.POSTGRESQL).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "TRUNCATE TABLE t_user",
                dialect(DatabaseIdentifier.KINGBASE).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "TRUNCATE TABLE t_user",
                dialect(DatabaseIdentifier.OPENGAUSS).overwriteClearStatement("t_user"));
        Assert.assertEquals(
                "TRUNCATE TABLE t_user",
                dialect(DatabaseIdentifier.SQLSERVER).overwriteClearStatement("t_user"));
    }

    private static JdbcDialect dialect(String name) {
        return new JdbcDialect() {
            @Override
            public String dialectName() {
                return name;
            }

            @Override
            public JdbcRowConverter getRowConverter() {
                return null;
            }

            @Override
            public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
                return null;
            }
        };
    }
}

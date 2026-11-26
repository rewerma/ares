package com.github.ares.engine.spark.core;

import com.github.ares.api.common.CommonOptions;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

public class SourceReadParallelismTest {

    @Test
    public void tableParallelismOverridesEnv() {
        LogicalCreateSourceTable sourceTable = new LogicalCreateSourceTable();
        sourceTable.getOptions().put(CommonOptions.PARALLELISM.key(), 8);
        Properties env = new Properties();
        env.setProperty(CommonOptions.PARALLELISM.key(), "4");
        Assert.assertEquals(8, SourceReadParallelism.resolve(sourceTable, env));
    }

    @Test
    public void envParallelismWhenTableUnset() {
        Properties env = new Properties();
        env.setProperty(CommonOptions.PARALLELISM.key(), " 6 ");
        Assert.assertEquals(6, SourceReadParallelism.resolve(null, env));
    }

    @Test
    public void defaultParallelismWhenUnset() {
        Assert.assertEquals(
                CommonOptions.PARALLELISM.defaultValue().intValue(),
                SourceReadParallelism.resolve(null, new Properties()));
    }
}

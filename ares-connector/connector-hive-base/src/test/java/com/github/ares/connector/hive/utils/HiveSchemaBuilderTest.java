package com.github.ares.connector.hive.utils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class HiveSchemaBuilderTest {

  @Test
  public void buildSchemaShouldIncludePartitionColumns() {
    Table table = new Table();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(
        Collections.singletonList(new FieldSchema("id", "int", "id column")));
    table.setSd(storageDescriptor);
    table.setPartitionKeys(
        Collections.singletonList(new FieldSchema("dt", "string", "partition column")));

    Map<String, Object> schema = HiveSchemaBuilder.buildSchema(table, "Hive");
    Map<?, ?> fields = (Map<?, ?>) schema.get("fields");

    Assert.assertEquals(2, fields.size());
    Assert.assertEquals("int", fields.get("id"));
    Assert.assertEquals("string", fields.get("dt"));
  }

  @Test
  public void buildSchemaShouldKeepColumnOrder() {
    Table table = new Table();
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(
        Arrays.asList(
            new FieldSchema("id", "bigint", null),
            new FieldSchema("name", "string", null)));
    table.setSd(storageDescriptor);
    table.setPartitionKeys(
        Arrays.asList(
            new FieldSchema("year", "int", null), new FieldSchema("month", "int", null)));

    Map<String, Object> schema = HiveSchemaBuilder.buildSchema(table, "Hive");
    Map<?, ?> fields = (Map<?, ?>) schema.get("fields");

    Assert.assertEquals(
        new ArrayList<>(Arrays.asList("id", "name", "year", "month")), new ArrayList<>(fields.keySet()));
  }
}

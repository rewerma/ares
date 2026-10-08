package com.github.ares.connector.doris.febe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.ares.api.table.catalog.CatalogTable;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.RowKind;
import com.github.ares.com.fasterxml.jackson.databind.JsonNode;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.jdbc.internal.dialect.DorisDialect;
import com.github.ares.connector.jdbc.internal.dialect.DorisTypeMapper;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

public class DorisFeBeTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void parsesQueryPlanAndBalancesTablets() {
        String body =
                "{"
                        + "\"status\":200,"
                        + "\"opaqued_query_plan\":\"plan-1\","
                        + "\"partitions\":{"
                        + "\"11\":{\"routings\":[\"be-a:9060\",\"be-b:9060\"]},"
                        + "\"12\":{\"routings\":[\"be-a:9060\"]},"
                        + "\"13\":{\"routings\":[\"be-b:9060\",\"be-a:9060\"]}"
                        + "}}";
        QueryPlanClient.QueryPlan plan = QueryPlanClient.parse(body);
        List<DorisSplit> splits = QueryPlanClient.toSplits(plan, 1, "demo", "t_user");
        assertEquals(3, splits.size());
        assertEquals("plan-1", splits.get(0).getOpaqueQueryPlan());
        assertEquals(1, splits.get(0).getTabletIds().size());
    }

    @Test
    public void parsesWrappedQueryPlan() {
        String body =
                "{"
                        + "\"msg\":\"success\",\"code\":0,"
                        + "\"data\":{"
                        + "\"opaqued_query_plan\":\"plan-2\","
                        + "\"partitions\":{\"21\":{\"routings\":[\"be-a:9060\"]}}"
                        + "}}";
        QueryPlanClient.QueryPlan plan = QueryPlanClient.parse(body);
        assertEquals("plan-2", plan.getOpaqueQueryPlan());
        assertEquals(1, plan.getTablets().size());
    }

    @Test
    public void parsesFeSchema() {
        String body =
                "{"
                        + "\"status\":200,"
                        + "\"properties\":["
                        + "{\"name\":\"id\",\"type\":\"bigint\",\"comment\":\"pk\"},"
                        + "{\"name\":\"name\",\"type\":\"varchar(32)\"},"
                        + "{\"name\":\"amount\",\"type\":\"decimal(10, 2)\"},"
                        + "{\"name\":\"huge\",\"type\":\"largeint\"}"
                        + "]}";
        CatalogTable table = DorisSchemaResolver.parseSchema(body, "demo", "t_user");
        assertEquals(4, table.getTableSchema().getColumns().size());
        assertEquals("id", table.getTableSchema().getColumns().get(0).getName());
        assertEquals(BasicType.LONG_TYPE, table.getTableSchema().getColumns().get(0).getDataType());
        assertEquals(
                "Decimal(10, 2)",
                table.getTableSchema().getColumns().get(2).getDataType().toString());
    }

    @Test
    public void mapsDorisTypes() {
        DorisTypeMapper mapper = new DorisTypeMapper();
        assertEquals(BasicType.BOOLEAN_TYPE, mapper.map("BOOLEAN", 0, 0, "c"));
        assertEquals(BasicType.STRING_TYPE, mapper.map("JSON", 0, 0, "c"));
        assertEquals(BasicType.STRING_TYPE, mapper.map("QUANTILE_STATE", 0, 0, "c"));
        assertEquals(BasicType.INT_TYPE, mapper.map("INT(11)", 11, 0, "c"));
        assertEquals("Decimal(38, 0)", mapper.map("LARGEINT", 0, 0, "c").toString());
    }

    @Test
    public void quotesIdentifiersAndBuildsScanSql() {
        DorisDialect dialect = new DorisDialect();
        assertEquals("`id`", dialect.quoteIdentifier("id"));
        assertEquals("`a``b`", dialect.quoteIdentifier("a`b"));
        Map<String, Object> options = new HashMap<>();
        options.put("mode", "febe");
        options.put("fe_nodes", "http://127.0.0.1:8030/, 127.0.0.2:8030");
        options.put("user", "root");
        options.put("password", "");
        options.put("table_name", "demo.t_user");
        options.put("scan_filter", "id > 1");
        DorisFeBeConfig config = DorisFeBeConfig.from(ReadonlyConfig.fromMap(options));
        config.setColumns(Arrays.asList("id", "name"));
        assertEquals(Arrays.asList("127.0.0.1:8030", "127.0.0.2:8030"), config.getFeNodes());
        assertEquals(
                "SELECT `id`, `name` FROM `demo`.`t_user` WHERE id > 1",
                QueryPlanClient.scanSql(config));
    }

    @Test
    public void serializesStreamLoadJsonAndAcceptsSuccessStatus() throws Exception {
        AresRow row = new AresRow(new Object[] {1, "ada", LocalDateTime.of(2024, 1, 2, 3, 4, 5)});
        AresRow deleted = new AresRow(new Object[] {2, "bob", null});
        deleted.setRowKind(RowKind.DELETE);
        byte[] body =
                StreamLoadClient.toJson(
                        new String[] {"id", "name", "c_time"}, Arrays.asList(row, deleted), true);
        JsonNode json = MAPPER.readTree(body);
        assertEquals(2, json.size());
        assertEquals(1, json.get(0).get("id").asInt());
        assertEquals("ada", json.get(0).get("name").asText());
        assertEquals("2024-01-02 03:04:05", json.get(0).get("c_time").asText());
        assertEquals(0, json.get(0).get("__DORIS_DELETE_SIGN__").asInt());
        assertEquals(1, json.get(1).get("__DORIS_DELETE_SIGN__").asInt());
        assertTrue(json.get(1).get("c_time").isNull());
        StreamLoadClient.assertSuccess(
                new DorisHttp.HttpResult(200, "{\"Status\":\"Success\",\"NumberLoadedRows\":1}"),
                "label");
        StreamLoadClient.assertSuccess(
                new DorisHttp.HttpResult(200, "{\"Status\":\"Publish Timeout\"}"), "label");
    }

    @Test
    public void readsArrowBatchByColumnName() throws Exception {
        AresRowType rowType =
                new AresRowType(
                        new String[] {"name", "id"},
                        new AresDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        try (BufferAllocator allocator = new RootAllocator()) {
            IntVector id = new IntVector("id", allocator);
            VarCharVector name = new VarCharVector("name", allocator);
            id.allocateNew(1);
            id.setSafe(0, 7);
            id.setValueCount(1);
            name.allocateNew(1);
            name.setSafe(0, "ada".getBytes(StandardCharsets.UTF_8));
            name.setValueCount(1);
            List<Field> fields = Arrays.asList(id.getField(), name.getField());
            java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
            try (VectorSchemaRoot root = new VectorSchemaRoot(fields, Arrays.asList(id, name));
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            List<AresRow> rows = ArrowBatchReader.read(output.toByteArray(), rowType, allocator);
            assertEquals(1, rows.size());
            assertEquals("ada", rows.get(0).getField(0));
            assertEquals(7, rows.get(0).getField(1));
            assertFalse(rows.isEmpty());
            id.close();
            name.close();
        }
    }

    @Test
    public void rejectsUnknownMode() {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("mode", "arrow");
        try {
            DorisFeBeConfig.checkMode(ReadonlyConfig.fromMap(options));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("jdbc"));
            return;
        }
        throw new AssertionError("expected invalid mode to fail");
    }

    @Test
    public void jdbcModeIsDefault() {
        assertFalse(DorisFeBeConfig.isFeBe(ReadonlyConfig.fromMap(Collections.emptyMap())));
    }
}

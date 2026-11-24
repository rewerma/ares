package com.github.ares.connector.druid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.api.table.catalog.TableSchema;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.LocalTimeType;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DruidTypeConvertTest {

    private static final String USER_ISO = "2023-03-09T12:23:00.000Z";

    @Test
    public void schemaPromotesTimeAndConvertHandlesIsoEpochNullAndNumeric() {
        List<String> columns = Arrays.asList("__time", "id", "name", "c_time");
        List<String> types = Arrays.asList("LONG", "LONG", "STRING", "STRING");
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        rows.add(row("__time", USER_ISO, "id", "2", "name", "alice", "c_time", null));
        rows.add(row("__time", null, "id", "3", "name", "bob", "c_time", "2023-03-09 20:23:00"));

        TableSchema schema = DruidTypeMapper.schema(columns, types, rows);
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, schema.getColumns().get(0).getDataType());
        assertEquals(BasicType.LONG_TYPE, schema.getColumns().get(1).getDataType());
        assertEquals(BasicType.STRING_TYPE, schema.getColumns().get(2).getDataType());
        assertEquals(BasicType.STRING_TYPE, schema.getColumns().get(3).getDataType());

        LocalDateTime expected = LocalDateTime.ofInstant(Instant.parse(USER_ISO), ZoneOffset.UTC);
        assertEquals(
                expected, DruidTypeMapper.convert(USER_ISO, LocalTimeType.LOCAL_DATE_TIME_TYPE));
        assertEquals(Long.valueOf(2L), DruidTypeMapper.convert("2", BasicType.LONG_TYPE));
        assertEquals("alice", DruidTypeMapper.convert("alice", BasicType.STRING_TYPE));
        assertNull(DruidTypeMapper.convert(null, LocalTimeType.LOCAL_DATE_TIME_TYPE));
        assertNull(DruidTypeMapper.convert(null, BasicType.LONG_TYPE));
        assertEquals(
                "2023-03-09 20:23:00",
                DruidTypeMapper.convert("2023-03-09 20:23:00", BasicType.STRING_TYPE));

        LocalDateTime fromEpoch =
                (LocalDateTime)
                        DruidTypeMapper.convert(
                                "1690000000000", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        assertEquals(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1690000000000L), ZoneOffset.UTC),
                fromEpoch);
    }

    @Test
    public void offsetIsoIsSameUtcInstantAndDateUsesUtcCalendarDate() {
        LocalDateTime fromOffset = DruidTimeValues.toLocalDateTime("2023-03-09T20:23:00.000+08:00");
        assertEquals(DruidTimeValues.toLocalDateTime(USER_ISO), fromOffset);
        assertEquals(LocalDate.parse("2023-03-09"), DruidTimeValues.toLocalDate("2023-03-09"));
        assertEquals(LocalDate.parse("2023-03-09"), DruidTimeValues.toLocalDate(USER_ISO));
    }

    @Test
    public void numericColumnStillFailsOnNonNumericAndIsNotCoercedToString() {
        try {
            DruidTypeMapper.convert("abc", BasicType.LONG_TYPE);
            fail("expected NumberFormatException");
        } catch (NumberFormatException ex) {
            assertTrue(ex.getMessage().contains("abc"));
        }
    }

    @Test
    public void nonTimeLongColumnWithIsoValuesIsPromoted() {
        TableSchema schema =
                DruidTypeMapper.schema(
                        Arrays.asList("event_ts", "id"),
                        Arrays.asList("LONG", "BIGINT"),
                        Collections.singletonList(row("event_ts", USER_ISO, "id", "1")));
        assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, schema.getColumns().get(0).getDataType());
        assertEquals(BasicType.LONG_TYPE, schema.getColumns().get(1).getDataType());
    }

    private static Map<String, String> row(String... pairs) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }
}

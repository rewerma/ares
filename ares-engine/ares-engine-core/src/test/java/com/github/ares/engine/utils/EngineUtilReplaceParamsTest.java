package com.github.ares.engine.utils;

import static org.junit.Assert.assertEquals;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.engine.core.PlParams;
import org.junit.Test;

public class EngineUtilReplaceParamsTest {

    @Test
    public void replacesLongerParamNamesFirst() {
        PlParams params = new PlParams();
        params.put("a", 1, PlType.of(InternalFieldType.INT));
        params.put("ab", 2, PlType.of(InternalFieldType.INT));
        String sql = "select \"${ab}\", \"${a}\"";
        assertEquals("select 2, 1", EngineUtil.replaceParams(sql, params));
    }

    @Test
    public void wrapsDateTimestampAndBytes() {
        PlParams params = new PlParams();
        params.put("d", "'2020-01-01'", PlType.of(InternalFieldType.DATE));
        params.put("ts", "'2020-01-01 00:00:00'", PlType.of(InternalFieldType.TIMESTAMP));
        params.put("b", "'FF'", PlType.of(InternalFieldType.BYTES));
        params.put("n", null, PlType.of(InternalFieldType.INT));
        assertEquals("TO_DATE('2020-01-01')", EngineUtil.replaceParams("\"${d}\"", params));
        assertEquals(
                "TO_TIMESTAMP('2020-01-01 00:00:00')",
                EngineUtil.replaceParams("\"${ts}\"", params));
        assertEquals("UNHEX('FF')", EngineUtil.replaceParams("\"${b}\"", params));
        assertEquals("null", EngineUtil.replaceParams("\"${n}\"", params));
    }
}

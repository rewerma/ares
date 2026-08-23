package com.github.ares.engine.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import org.junit.Test;

public class PlParamsTest {

    @Test
    public void putGetAndTypeRoundTrip() {
        PlParams params = new PlParams();
        params.put("cnt", 1, PlType.of(InternalFieldType.INT));
        assertTrue(params.containsKey("cnt"));
        assertEquals(Integer.valueOf(1), params.get("cnt"));
        assertEquals(InternalFieldType.INT, params.getType("cnt").getType());
    }

    @Test
    public void copyIsIndependent() {
        PlParams params = new PlParams();
        params.put("name", "a", PlType.of(InternalFieldType.VARCHAR));
        PlParams copy = params.copy();
        copy.put("name", "b", PlType.of(InternalFieldType.VARCHAR));
        copy.put("extra", 1, PlType.of(InternalFieldType.INT));
        assertEquals("a", params.get("name"));
        assertFalse(params.containsKey("extra"));
        assertEquals("b", copy.get("name"));
    }

    @Test
    public void removeAndEmptyToString() {
        PlParams params = new PlParams();
        assertEquals("{}", params.toString());
        params.put("x", 1, PlType.of(InternalFieldType.INT));
        assertEquals("{x=1}", params.toString());
        params.remove("x");
        assertFalse(params.containsKey("x"));
        assertNull(params.get("x"));
    }
}

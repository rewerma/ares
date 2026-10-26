package com.github.ares.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.common.engine.InternalFieldType;
import com.github.ares.common.engine.PlType;
import com.github.ares.common.exceptions.AresException;
import java.math.BigDecimal;
import java.time.LocalDate;
import org.junit.Test;

public class DataTypeConvertorTest {

    @Test
    public void booleanAcceptsBooleanAndNumberOnly() {
        PlType type = PlType.of(InternalFieldType.BOOLEAN);
        assertEquals(Boolean.TRUE, DataTypeConvertor.convert("flag", type, true));
        assertEquals(Boolean.TRUE, DataTypeConvertor.convert("flag", type, 1));
        assertEquals(Boolean.FALSE, DataTypeConvertor.convert("flag", type, 0));
        try {
            DataTypeConvertor.convert("flag", type, "true");
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(e.getMessage().contains("BOOLEAN"));
        }
    }

    @Test
    public void intTruncatesDecimalString() {
        assertEquals(
                Integer.valueOf(12),
                DataTypeConvertor.convert("n", PlType.of(InternalFieldType.INT), "12.9"));
    }

    @Test
    public void varcharQuotesWhenIdentifier() {
        assertEquals(
                "'it''s'",
                DataTypeConvertor.convertWithIdentifier(
                        "s", PlType.of(InternalFieldType.VARCHAR), "it's"));
        assertEquals(
                "it's",
                DataTypeConvertor.convert("s", PlType.of(InternalFieldType.VARCHAR), "it's"));
    }

    @Test
    public void dateFormatsLocalDate() {
        assertEquals(
                "'2020-01-02'",
                DataTypeConvertor.convert(
                        "d", PlType.of(InternalFieldType.DATE), LocalDate.of(2020, 1, 2)));
    }

    @Test
    public void numericAppliesScale() {
        PlType type = PlType.of(InternalFieldType.NUMERIC, 10, 2);
        assertEquals(new BigDecimal("1.24"), DataTypeConvertor.convert("n", type, "1.235"));
    }

    @Test
    public void nullPassesThrough() {
        assertEquals(null, DataTypeConvertor.convert("n", PlType.of(InternalFieldType.INT), null));
    }
}

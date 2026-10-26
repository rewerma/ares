package com.github.ares.engine.function;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.ares.common.exceptions.AresException;
import java.util.Arrays;
import org.junit.Test;

public class AssertEqualsTest {

    private final AssertEquals assertEquals = new AssertEquals();

    @Test
    public void longMatchesIntLiteral() {
        Long crc = 222957957L;
        assertSame(crc, assertEquals.evaluate(Arrays.asList(crc, 222957957)));
    }

    @Test
    public void byteArraysMatchByContent() {
        byte[] left = new byte[] {1, 2, 3};
        byte[] right = new byte[] {1, 2, 3};
        assertSame(left, assertEquals.evaluate(Arrays.asList(left, right)));
    }

    @Test
    public void differentNumbersFail() {
        try {
            assertEquals.evaluate(Arrays.asList(1L, 2));
            fail("expected AresException");
        } catch (AresException e) {
            assertTrue(e.getMessage().contains("ASSERT_EQUALS failed"));
        }
    }
}

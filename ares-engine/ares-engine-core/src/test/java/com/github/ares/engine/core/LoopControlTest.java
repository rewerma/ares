package com.github.ares.engine.core;

import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class LoopControlTest {

    @Test
    public void classifiesExitContinueAndReturnByIdentity() {
        assertTrue(LoopControl.isExit(EXIT_LOOP));
        assertTrue(LoopControl.isContinue(CONTINUE_LOOP));
        assertTrue(LoopControl.isReturn(RETURN_VALUE));
        assertTrue(LoopControl.isSignal(EXIT_LOOP));
        assertTrue(LoopControl.isSignal(CONTINUE_LOOP));
        assertTrue(LoopControl.isSignal(RETURN_VALUE));
    }

    @Test
    public void ignoresUnrelatedValues() {
        assertFalse(LoopControl.isExit(CONTINUE_LOOP));
        assertFalse(LoopControl.isContinue(EXIT_LOOP));
        assertFalse(LoopControl.isReturn(EXIT_LOOP));
        assertFalse(LoopControl.isSignal(null));
        assertFalse(LoopControl.isSignal("exitLoop"));
        assertFalse(LoopControl.isSignal(0));
    }
}

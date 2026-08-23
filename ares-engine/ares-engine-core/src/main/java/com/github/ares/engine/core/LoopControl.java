package com.github.ares.engine.core;

import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;

/**
 * Body-result sentinels for loops. EXIT/CONTINUE apply only to the innermost loop; RETURN must
 * propagate out of all loops to the enclosing function.
 */
public final class LoopControl {
    private LoopControl() {}

    public static boolean isExit(Object res) {
        return EXIT_LOOP == res;
    }

    public static boolean isContinue(Object res) {
        return CONTINUE_LOOP == res;
    }

    public static boolean isReturn(Object res) {
        return RETURN_VALUE == res;
    }

    public static boolean isSignal(Object res) {
        return isExit(res) || isContinue(res) || isReturn(res);
    }
}

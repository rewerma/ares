package com.github.ares.engine.core;

import static com.github.ares.parser.enums.OperationType.CONTINUE_LOOP;
import static com.github.ares.parser.enums.OperationType.EXIT_LOOP;
import static com.github.ares.parser.enums.OperationType.RETURN_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.github.ares.parser.plan.LogicalExpression;
import com.github.ares.parser.plan.LogicalForLoop;
import com.github.ares.parser.plan.LogicalWhileLoop;
import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class LoopExecutorTest {

    @Test
    public void whileExitStopsOnlyThisLoop() {
        AtomicInteger visits = new AtomicInteger();
        WhileLoopExecutor executor = whileExecutor(alwaysTrue());
        Object result =
                executor.execute(
                        whileLoop(),
                        new PlParams(),
                        (ops, params) -> {
                            visits.incrementAndGet();
                            return EXIT_LOOP;
                        });
        assertEquals(1, visits.get());
        assertFalse(LoopControl.isExit(result));
        assertNull(result);
    }

    @Test
    public void whileContinueDoesNotPropagateAndSkipsLastData() {
        AtomicInteger visits = new AtomicInteger();
        WhileLoopExecutor executor = whileExecutor(times(3));
        Object result =
                executor.execute(
                        whileLoop(),
                        new PlParams(),
                        (ops, params) -> {
                            visits.incrementAndGet();
                            return CONTINUE_LOOP;
                        });
        assertEquals(3, visits.get());
        assertFalse(LoopControl.isContinue(result));
        assertNull(result);
    }

    @Test
    public void whileReturnPropagatesToCaller() {
        WhileLoopExecutor executor = whileExecutor(alwaysTrue());
        Object result =
                executor.execute(whileLoop(), new PlParams(), (ops, params) -> RETURN_VALUE);
        assertSame(RETURN_VALUE, result);
    }

    @Test
    public void forExitDoesNotPropagateAndClearsIndex() {
        AtomicInteger visits = new AtomicInteger();
        ForLoopExecutor executor = forExecutor();
        PlParams params = new PlParams();
        Object result =
                executor.execute(
                        forLoop("1", "5"),
                        params,
                        (ops, plParams) -> {
                            visits.incrementAndGet();
                            return EXIT_LOOP;
                        });
        assertEquals(1, visits.get());
        assertFalse(LoopControl.isExit(result));
        assertFalse(params.containsKey("i"));
    }

    @Test
    public void forReturnPropagatesAndClearsIndex() {
        ForLoopExecutor executor = forExecutor();
        PlParams params = new PlParams();
        Object result =
                executor.execute(forLoop("1", "5"), params, (ops, plParams) -> RETURN_VALUE);
        assertSame(RETURN_VALUE, result);
        assertFalse(params.containsKey("i"));
    }

    @Test
    public void forContinueSkipsLastDataThenFinishes() {
        ForLoopExecutor executor = forExecutor();
        Object result =
                executor.execute(
                        forLoop("1", "3"),
                        new PlParams(),
                        (ops, params) -> {
                            int i = (Integer) params.get("i");
                            if (i == 2) {
                                return CONTINUE_LOOP;
                            }
                            return "v" + i;
                        });
        assertEquals("v3", result);
    }

    private static WhileLoopExecutor whileExecutor(ExpressionExecutor expressionExecutor) {
        WhileLoopExecutor executor = new WhileLoopExecutor();
        executor.init(EngineTestSupport.managerWith(expressionExecutor));
        return executor;
    }

    private static ForLoopExecutor forExecutor() {
        ForLoopExecutor executor = new ForLoopExecutor();
        executor.init(
                EngineTestSupport.managerWith(
                        new ExpressionExecutor() {
                            @Override
                            public Serializable execute(String expr, PlParams plParams) {
                                return Integer.valueOf(expr);
                            }
                        }));
        return executor;
    }

    private static ExpressionExecutor alwaysTrue() {
        return times(Integer.MAX_VALUE);
    }

    private static ExpressionExecutor times(int n) {
        return new ExpressionExecutor() {
            private int remaining = n;

            @Override
            public boolean execute4Bool(String expr, PlParams plParams) {
                if (remaining <= 0) {
                    return false;
                }
                remaining--;
                return true;
            }
        };
    }

    private static LogicalWhileLoop whileLoop() {
        LogicalWhileLoop loop = new LogicalWhileLoop();
        LogicalExpression condition = new LogicalExpression();
        condition.setExpr("true");
        loop.setCondition(condition);
        loop.setWhileBody(Collections.emptyList());
        return loop;
    }

    private static LogicalForLoop forLoop(String lower, String upper) {
        LogicalForLoop loop = new LogicalForLoop();
        loop.setIndexName("i");
        LogicalExpression lowerExpr = new LogicalExpression();
        lowerExpr.setExpr(lower);
        LogicalExpression upperExpr = new LogicalExpression();
        upperExpr.setExpr(upper);
        loop.setLowerExpr(lowerExpr);
        loop.setUpperExpr(upperExpr);
        loop.setForBody(Collections.emptyList());
        return loop;
    }
}

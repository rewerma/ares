package com.github.ares.spark.connector.utils;

import java.time.Instant;

public class InstantConverterUtils {

    private static final long MICRO_OF_SECOND = 1000_000;
    private static final int MICRO_OF_NANOS = 1000;

    /** @see Instant#toEpochMilli() */
    public static Long toEpochMicro(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        if (seconds < 0 && nanos > 0) {
            long micro = Math.multiplyExact(seconds + 1, MICRO_OF_SECOND);
            long adjustment = nanos / MICRO_OF_NANOS - MICRO_OF_SECOND;
            return Math.addExact(micro, adjustment);
        } else {
            long millis = Math.multiplyExact(seconds, MICRO_OF_SECOND);
            return Math.addExact(millis, nanos / MICRO_OF_NANOS);
        }
    }

    /** @see Instant#ofEpochMilli(long) */
    public static Instant ofEpochMicro(long epochMicro) {
        long secs = Math.floorDiv(epochMicro, MICRO_OF_SECOND);
        int mos = (int) Math.floorMod(epochMicro, MICRO_OF_SECOND);
        return Instant.ofEpochSecond(secs, Math.multiplyExact(mos, MICRO_OF_NANOS));
    }
}

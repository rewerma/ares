package com.github.ares.spark.connector.statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class JobStatisticInformation {
    private static final Logger LOG = LoggerFactory.getLogger(JobStatisticInformation.class);

    private static final String BORDER = "***********************************************";
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int COUNT_FIELD_WIDTH = 20;

    private JobStatisticInformation() {}

    public static void log(long startTimeMillis, long readCount, long writeCount, long failedCount) {
        long endTimeMillis = System.currentTimeMillis();
        long totalTimeSeconds = Math.max(0, (endTimeMillis - startTimeMillis) / 1000);

        StringBuilder builder = new StringBuilder();
        builder.append('\n').append("        ").append(BORDER).append('\n');
        builder.append("           Job Statistic Information\n");
        builder.append(BORDER).append('\n');
        builder.append("Start Time                : ")
                .append(formatDateTime(startTimeMillis))
                .append('\n');
        builder.append("End Time                  : ")
                .append(formatDateTime(endTimeMillis))
                .append('\n');
        builder.append("Total Time(s)             : ")
                .append(formatCount(totalTimeSeconds))
                .append('\n');
        builder.append("Total Read Count          : ")
                .append(formatCount(readCount))
                .append('\n');
        builder.append("Total Write Count         : ")
                .append(formatCount(writeCount))
                .append('\n');
        builder.append("Total Failed Count        : ")
                .append(formatCount(failedCount))
                .append('\n');
        builder.append(BORDER);
        LOG.info(builder.toString());
    }

    private static String formatDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault())
                .format(DATE_TIME_FORMATTER);
    }

    private static String formatCount(long count) {
        return String.format("%" + COUNT_FIELD_WIDTH + "d", count);
    }
}
